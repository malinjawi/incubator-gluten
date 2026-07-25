/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Tests for the Gluten-local grouping-set aggregation operator.
//
// Most correctness tests compare the custom operator with Expand followed by
// intermediate aggregation, then apply the same final aggregation to both.
//
// OperatorTestBase registers Presto aggregate functions. The main tests cover
// fixed-width, row-typed, and external-memory accumulator states:
//
//   sum(BIGINT)   -> BIGINT
//   avg(DOUBLE)   -> ROW(DOUBLE, BIGINT)
//   array_agg(T)  -> ARRAY(T)
//
// Spark-specific decimal and average buffers are covered separately below.

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <numeric>
#include <optional>

#include <folly/Random.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/HashAggregation.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include "operators/plannodes/GroupingSetAggregationNode.h"
#include "operators/plannodes/GroupingSetLattice.h"
#include "operators/plannodes/MultiGroupingSetAggregation.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec {

namespace {

using Set = GroupingSetSpec;

// Low-memory differential cases also exercise a known failure in the
// Expand-plus-intermediate reference plan. Fused-only budget tests below isolate
// the custom operator's flush behavior from that reference failure.

class MultiGroupingSetAggregationTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    // Registration is process-wide, so pair each registration with TearDown.
    registerMultiGroupingSetAggregation();
  }

  void TearDown() override {
    Operator::unregisterAllOperators();
    OperatorTestBase::TearDown();
  }

  // -------------------------------------------------------------------------
  // Grouping-set shapes
  // -------------------------------------------------------------------------

  /// ROLLUP(k1..kn): a chain in the lattice.
  static std::vector<Set> rollupSets(int32_t numKeys) {
    std::vector<Set> sets;
    for (auto i = 0; i <= numKeys; ++i) {
      Set set;
      set.groupingId = i;
      for (auto j = 0; j < numKeys; ++j) {
        if (j < numKeys - i) {
          set.activeKeysMask |= GroupingSetMask{1} << j;
        }
      }
      sets.push_back(std::move(set));
    }
    return sets;
  }

  /// CUBE(k1..kn): the full power set, with 2^n grouping sets.
  static std::vector<Set> cubeSets(int32_t numKeys) {
    std::vector<Set> sets;
    const int32_t total = 1 << numKeys;
    // Descending so that set 0 is the all-keys-active set, which is the grain
    // the child partial aggregation delivers (and hence the bypass candidate).
    for (int32_t m = total - 1; m >= 0; --m) {
      Set set;
      set.groupingId = m;
      set.activeKeysMask = static_cast<GroupingSetMask>(m);
      sets.push_back(std::move(set));
    }
    return sets;
  }

  /// Grouping sets with no containment form an antichain and a flat fan-out.
  static std::vector<Set> antichainSets() {
    std::vector<Set> sets;
    // {k1, k2}
    Set a;
    a.activeKeysMask = 0b011;
    a.groupingId = 11;
    sets.push_back(std::move(a));
    // {k3}
    Set b;
    b.activeKeysMask = 0b100;
    b.groupingId = 22;
    sets.push_back(std::move(b));
    return sets;
  }

  // -------------------------------------------------------------------------
  // Input generation
  // -------------------------------------------------------------------------

  /// Raw input rows: three grouping keys of decreasing cardinality (so the
  /// hierarchy actually reduces, which is the shape the operator is designed
  /// for), plus the aggregate inputs.
  std::vector<RowVectorPtr> makeInput(
      int32_t numBatches,
      vector_size_t batchSize,
      int32_t k1Cardinality,
      int32_t k2Cardinality,
      int32_t k3Cardinality,
      uint32_t seed = 1234) {
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);
    folly::Random::DefaultGenerator rng(seed);
    for (auto b = 0; b < numBatches; ++b) {
      std::vector<int64_t> k1(batchSize), k2(batchSize), k3(batchSize), v(batchSize);
      std::vector<double> w(batchSize);
      for (auto i = 0; i < batchSize; ++i) {
        k1[i] = folly::Random::rand32(k1Cardinality, rng);
        k2[i] = folly::Random::rand32(k2Cardinality, rng);
        k3[i] = folly::Random::rand32(k3Cardinality, rng);
        v[i] = folly::Random::rand32(1000, rng);
        w[i] = static_cast<double>(folly::Random::rand32(1000, rng)) / 7.0;
      }
      batches.push_back(makeRowVector(
          {"k1", "k2", "k3", "v", "w"},
          {makeFlatVector<int64_t>(k1),
           makeFlatVector<int64_t>(k2),
           makeFlatVector<int64_t>(k3),
           makeFlatVector<int64_t>(v),
           makeFlatVector<double>(w)}));
    }
    return batches;
  }

  static RowTypePtr rawInputType() {
    return ROW({"k1", "k2", "k3", "v", "w"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT(), DOUBLE()});
  }

  // -------------------------------------------------------------------------
  // Plan construction
  // -------------------------------------------------------------------------
  //
  // Both plans share the same child subplan:
  //
  //     Values(raw)
  //       -> partialAggregation({k1..kn}, {sum(v) as s, avg(w) as m,
  //                                        array_agg(v) as l})
  //
  // Its output is ROW(k1..kn, s, m, l): keys followed by one intermediate
  // column per aggregate. A real partial aggregation supplies valid state
  // vectors, including array_agg's external memory.

  struct AggSpec {
    /// Partial-aggregation expressions over the raw input columns.
    std::vector<std::string> partial;
    /// Raw argument types per aggregate, same order as `partial`.
    std::vector<std::vector<TypePtr>> rawTypes;
    /// Final-aggregation expressions over the intermediate columns.
    std::vector<std::string> final;
    /// Output columns of the closing projection, keys and gid excluded.
    std::vector<std::string> outputs;
  };

  static AggSpec defaultAggSpec() {
    return AggSpec{
        {"sum(v) as s", "avg(w) as m", "array_agg(v) as l"},
        {{BIGINT()}, {DOUBLE()}, {BIGINT()}},
        {"sum(s) as s", "avg(m) as m", "array_agg(l) as l"},
        {"s", "m", "array_sort(l) as l"}};
  }

  static std::vector<std::string> keyNames(int32_t numKeys) {
    std::vector<std::string> keys;
    for (auto i = 0; i < numKeys; ++i) {
      keys.push_back(fmt::format("k{}", i + 1));
    }
    return keys;
  }

  /// The finest-grain flushable partial aggregation that feeds both plans.
  core::PlanNodePtr
  makeChild(PlanBuilder& builder, const std::vector<RowVectorPtr>& input, int32_t numKeys, const AggSpec& spec) {
    return builder.values(input).partialAggregation(keyNames(numKeys), spec.partial).planNode();
  }

  /// Rebuilds the child's aggregates as kIntermediate ones: the call is
  /// re-expressed over the child's *intermediate* output column, while
  /// rawInputTypes keeps the raw argument types.
  ///
  /// Transcribed from PlanBuilder::createIntermediateOrFinalAggregation, which
  /// is private. resolveIntermediateType is the single source of truth for the
  /// intermediate type, which is what guarantees this test agrees with
  /// GroupingSetAggregationNode::makeOutputType and the operator's
  /// makeNaturalType.
  static std::vector<core::AggregationNode::Aggregate> makeIntermediateAggregates(
      const core::AggregationNode* childAgg) {
    const auto numGroupingKeys = childAgg->groupingKeys().size();
    std::vector<core::AggregationNode::Aggregate> aggregates;
    aggregates.reserve(childAgg->aggregates().size());

    for (size_t i = 0; i < childAgg->aggregates().size(); ++i) {
      const auto& partial = childAgg->aggregates()[i];
      core::AggregationNode::Aggregate agg;
      for (const auto& rawInput : partial.call->inputs()) {
        agg.rawInputTypes.push_back(rawInput->type());
      }
      const auto intermediateType = resolveIntermediateType(partial.call->name(), agg.rawInputTypes);

      const auto& childOutput = childAgg->outputType();
      const auto channel = numGroupingKeys + i;
      std::vector<core::TypedExprPtr> inputs = {
          std::make_shared<core::FieldAccessTypedExpr>(childOutput->childAt(channel), childOutput->nameOf(channel))};

      agg.call = std::make_shared<core::CallTypedExpr>(intermediateType, std::move(inputs), partial.call->name());
      aggregates.push_back(std::move(agg));
    }
    return aggregates;
  }

  static std::vector<core::FieldAccessTypedExprPtr> makeGroupingKeys(const core::PlanNodePtr& child, int32_t numKeys) {
    std::vector<core::FieldAccessTypedExprPtr> keys;
    const auto& type = child->outputType();
    for (auto i = 0; i < numKeys; ++i) {
      const auto name = fmt::format("k{}", i + 1);
      keys.push_back(std::make_shared<core::FieldAccessTypedExpr>(type->findChild(name), name));
    }
    return keys;
  }

  /// ROW(k1..kn, acc1..accm, gid BIGINT).
  static RowTypePtr
  makeExpectedOutputType(const core::PlanNodePtr& child, int32_t numKeys, const std::vector<std::string>& aggNames) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    const auto& childType = child->outputType();
    for (auto i = 0; i < numKeys; ++i) {
      names.push_back(childType->nameOf(i));
      types.push_back(childType->childAt(i));
    }
    for (size_t i = 0; i < aggNames.size(); ++i) {
      names.push_back(aggNames[i]);
      types.push_back(childType->childAt(numKeys + i));
    }
    names.push_back("gid");
    types.push_back(BIGINT());
    return ROW(std::move(names), std::move(types));
  }

  /// (a) The fused plan: child -> GroupingSetAggregationNode -> final agg.
  core::PlanNodePtr finishFusedPlan(
      PlanBuilder& builder,
      const core::AggregationNode* childAgg,
      int32_t numKeys,
      const std::vector<Set>& sets,
      std::optional<int32_t> childGroupedSet,
      const AggSpec& spec = defaultAggSpec()) {
    VELOX_CHECK_NOT_NULL(childAgg);

    auto aggregates = makeIntermediateAggregates(childAgg);
    auto aggNames = childAgg->aggregateNames();
    auto groupingKeys = makeGroupingKeys(builder.planNode(), numKeys);

    return builder
        .addNode([&](std::string id, core::PlanNodePtr source) {
          return std::make_shared<GroupingSetAggregationNode>(
              std::move(id), groupingKeys, sets, aggNames, aggregates, childGroupedSet, std::move(source));
        })
        .finalAggregation(appendGid(keyNames(numKeys)), spec.final, spec.rawTypes)
        .project(finalProjection(numKeys, spec))
        .planNode();
  }

  core::PlanNodePtr makeFusedPlan(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      std::optional<int32_t> childGroupedSet,
      const AggSpec& spec = defaultAggSpec()) {
    PlanBuilder builder(pool());
    auto child = makeChild(builder, input, numKeys, spec);
    const auto* childAgg = dynamic_cast<const core::AggregationNode*>(child.get());
    return finishFusedPlan(builder, childAgg, numKeys, sets, childGroupedSet, spec);
  }

  /// Builds the fused half over a materialized stream of intermediate states.
  /// `childAgg` supplies the base function names and raw argument types that
  /// are not encoded in a ValuesNode's schema.
  core::PlanNodePtr makeFusedPlanFromIntermediate(
      const std::vector<RowVectorPtr>& intermediate,
      const core::AggregationNode* childAgg,
      int32_t numKeys,
      const std::vector<Set>& sets,
      std::optional<int32_t> childGroupedSet,
      const AggSpec& spec) {
    PlanBuilder builder(pool());
    builder.values(intermediate);
    return finishFusedPlan(builder, childAgg, numKeys, sets, childGroupedSet, spec);
  }

  /// (b) Reference: child -> ExpandNode -> kIntermediate AggregationNode ->
  ///     final aggregation.
  ///
  /// PlanBuilder cannot express the kIntermediate half: the public
  /// intermediateAggregation(keys, aggs) overload forwards without
  /// rawInputTypes and trips a VELOX_CHECK_EQ, the no-arg overload requires the
  /// preceding node to be a raw-input partial aggregation (ours is an Expand),
  /// and the overload that would work is private. So the node is constructed
  /// directly via addNode().
  core::PlanNodePtr finishReferencePlan(
      PlanBuilder& builder,
      const core::AggregationNode* childAgg,
      int32_t numKeys,
      const std::vector<Set>& sets,
      const AggSpec& spec = defaultAggSpec()) {
    VELOX_CHECK_NOT_NULL(childAgg);

    const auto aggNames = childAgg->aggregateNames();
    const auto& childType = builder.planNode()->outputType();

    // One projection set per grouping set. Active keys pass through, inactive
    // keys become a typed NULL, accumulator columns always pass through, and
    // the trailing literal is the gid -- the same literals the fused node got,
    // which is what makes this a fair comparison.
    std::vector<std::vector<std::string>> projections;
    for (size_t s = 0; s < sets.size(); ++s) {
      const auto& set = sets[s];
      std::vector<std::string> projection;
      for (auto j = 0; j < numKeys; ++j) {
        const auto name = fmt::format("k{}", j + 1);
        // Only the FIRST projection set carries aliases (PlanBuilder.cpp:
        // 1267-1276); the rest are positional.
        if (set.containsKey(j)) {
          projection.push_back(s == 0 ? fmt::format("{0} as {0}", name) : name);
        } else {
          const auto typedNull = fmt::format("null::{}", childType->childAt(j)->toString());
          projection.push_back(s == 0 ? fmt::format("{} as {}", typedNull, name) : typedNull);
        }
      }
      for (const auto& aggName : aggNames) {
        projection.push_back(aggName);
      }
      // Bare integer literal: Velox parses it to a BIGINT constant. "N::bigint"
      // would parse to a CAST, which ExpandNode rejects outright.
      projection.push_back(s == 0 ? fmt::format("{} as gid", set.groupingId) : fmt::format("{}", set.groupingId));
      projections.push_back(std::move(projection));
    }

    builder.expand(projections);

    return builder
        .addNode([&](std::string id, core::PlanNodePtr source) {
          std::vector<core::FieldAccessTypedExprPtr> groupingKeys = makeGroupingKeys(source, numKeys);
          const auto& sourceType = source->outputType();
          groupingKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(sourceType->findChild("gid"), "gid"));

          return std::make_shared<core::AggregationNode>(
              std::move(id),
              core::AggregationNode::Step::kIntermediate,
              groupingKeys,
              /*preGroupedKeys=*/
              std::vector<core::FieldAccessTypedExprPtr>{},
              aggNames,
              makeIntermediateAggregates(childAgg),
              /*ignoreNullKeys=*/false,
              /*noGroupsSpanBatches=*/false,
              std::move(source));
        })
        .finalAggregation(appendGid(keyNames(numKeys)), spec.final, spec.rawTypes)
        .project(finalProjection(numKeys, spec))
        .planNode();
  }

  core::PlanNodePtr makeReferencePlan(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      const AggSpec& spec = defaultAggSpec()) {
    PlanBuilder builder(pool());
    auto child = makeChild(builder, input, numKeys, spec);
    const auto* childAgg = dynamic_cast<const core::AggregationNode*>(child.get());
    return finishReferencePlan(builder, childAgg, numKeys, sets, spec);
  }

  core::PlanNodePtr makeReferencePlanFromIntermediate(
      const std::vector<RowVectorPtr>& intermediate,
      const core::AggregationNode* childAgg,
      int32_t numKeys,
      const std::vector<Set>& sets,
      const AggSpec& spec) {
    PlanBuilder builder(pool());
    builder.values(intermediate);
    return finishReferencePlan(builder, childAgg, numKeys, sets, spec);
  }

  static std::vector<std::string> appendGid(std::vector<std::string> keys) {
    keys.push_back("gid");
    return keys;
  }

  /// array_agg's result ORDER is not stable across flush timing -- the fused
  /// operator makes no promise about it, and neither does Velox's own
  /// partial/final split. Sorting the array before comparison tests the
  /// multiset of collected values, which is the property that actually has to
  /// hold.
  static std::vector<std::string> finalProjection(int32_t numKeys, const AggSpec& spec) {
    auto columns = appendGid(keyNames(numKeys));
    for (const auto& out : spec.outputs) {
      columns.push_back(out);
    }
    return columns;
  }

  // -------------------------------------------------------------------------
  // Differential driver
  // -------------------------------------------------------------------------

  /// Runs both plans under the same config and asserts multiset equality.
  /// assertEqualResults is multiset-based and has the epsilon handling for the
  /// floating-point avg column, which applies here because the final
  /// aggregation yields exactly one row per group.
  void assertFusedMatchesReference(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::unordered_map<std::string, std::string>& configs = {},
      const AggSpec& spec = defaultAggSpec()) {
    assertFusedMatchesReferenceForSets(
        input,
        numKeys,
        rollupSets(numKeys),
        /*childGroupedSet=*/0,
        configs,
        spec);
  }

  void assertFusedMatchesReferenceForSets(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      std::optional<int32_t> childGroupedSet,
      const std::unordered_map<std::string, std::string>& configs = {},
      const AggSpec& spec = defaultAggSpec()) {
    // Diagnostic hook, off unless PLAN_ONLY is exported. Runs ONE side of the
    // differential in isolation, which is how the five blocked cases above were
    // attributed to the reference plan rather than to this operator. It does
    // not affect a normal run: with PLAN_ONLY unset both plans run and the
    // multiset comparison below is the assertion.
    const char* only = getenv("PLAN_ONLY");
    if (only && std::string(only) == "ref") {
      auto r = AssertQueryBuilder(makeReferencePlan(input, numKeys, sets, spec)).configs(configs).copyResults(pool());
      fprintf(stderr, "[isolate] reference produced %d rows\n", (int)r->size());
      return;
    }
    if (only && std::string(only) == "fused") {
      auto r = AssertQueryBuilder(makeFusedPlan(input, numKeys, sets, childGroupedSet, spec))
                   .configs(configs)
                   .copyResults(pool());
      fprintf(stderr, "[isolate] fused produced %d rows\n", (int)r->size());
      return;
    }
    auto fused = AssertQueryBuilder(makeFusedPlan(input, numKeys, sets, childGroupedSet, spec))
                     .configs(configs)
                     .copyResults(pool());
    auto reference =
        AssertQueryBuilder(makeReferencePlan(input, numKeys, sets, spec)).configs(configs).copyResults(pool());
    ASSERT_TRUE(assertEqualResults({reference}, {fused}));
  }

  void assertFusedMatchesReferenceWithAggregates(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<std::string>& partial,
      const std::vector<std::vector<TypePtr>>& rawTypes,
      const std::vector<std::string>& final,
      const std::vector<std::string>& outputs,
      const std::unordered_map<std::string, std::string>& configs = {}) {
    assertFusedMatchesReference(input, numKeys, configs, AggSpec{partial, rawTypes, final, outputs});
  }

  // Compare a large-budget run with low-budget runs to isolate operator flush
  // correctness from the failing low-memory reference plan.
  void assertFusedSelfConsistentAcrossBudgets(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      const AggSpec& spec,
      const std::vector<int64_t>& budgets) {
    auto runAt = [&](int64_t budget) {
      const std::unordered_map<std::string, std::string> configs = {
          {core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", budget)},
          {core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", budget)},
      };
      return AssertQueryBuilder(makeFusedPlan(
                                    input,
                                    numKeys,
                                    sets,
                                    /*childGroupedSet=*/0,
                                    spec))
          .configs(configs)
          .copyResults(pool());
    };

    const int64_t kLargeBudget = 1LL << 30;
    auto expected = runAt(kLargeBudget);
    for (auto budget : budgets) {
      auto actual = runAt(budget);
      SCOPED_TRACE(fmt::format("budget={}", budget));
      ASSERT_EQ(actual->size(), expected->size()) << "fused plan changed row count under flush at budget " << budget;
      ASSERT_TRUE(assertEqualResults({expected}, {actual}))
          << "fused plan disagreed with itself under flush at budget " << budget;
    }
  }

  struct UpstreamPartialStats {
    int64_t outputRows{0};
    int64_t flushRows{0};
    int64_t flushes{0};
    int64_t abandonedRows{0};
  };

  static UpstreamPartialStats upstreamPartialStats(
      const std::shared_ptr<Task>& task,
      const core::PlanNodeId& planNodeId) {
    UpstreamPartialStats result;
    for (const auto& pipeline : task->taskStats().pipelineStats) {
      for (const auto& stats : pipeline.operatorStats) {
        if (stats.planNodeId != planNodeId) {
          continue;
        }
        result.outputRows += static_cast<int64_t>(stats.outputPositions);
        const auto addRuntimeStat = [&](std::string_view name, int64_t& value) {
          if (const auto it = stats.runtimeStats.find(std::string(name)); it != stats.runtimeStats.end()) {
            value += it->second.sum;
          }
        };
        addRuntimeStat(HashAggregation::kFlushRowCount, result.flushRows);
        addRuntimeStat(HashAggregation::kFlushTimes, result.flushes);
        addRuntimeStat(HashAggregation::kAbandonedPartialAggregationRows, result.abandonedRows);
      }
    }
    return result;
  }

  static int64_t groupingSetRuntimeStat(const std::shared_ptr<Task>& task, std::string_view name) {
    int64_t result{0};
    for (const auto& pipeline : task->taskStats().pipelineStats) {
      for (const auto& stats : pipeline.operatorStats) {
        if (stats.operatorType != "GroupingSetAggregation") {
          continue;
        }
        if (const auto it = stats.runtimeStats.find(std::string(name)); it != stats.runtimeStats.end()) {
          result += it->second.sum;
        }
      }
    }
    return result;
  }

  static int32_t groupingSetRuntimeStatOccurrences(const std::shared_ptr<Task>& task, std::string_view name) {
    int32_t result{0};
    for (const auto& pipeline : task->taskStats().pipelineStats) {
      for (const auto& stats : pipeline.operatorStats) {
        if (stats.operatorType == "GroupingSetAggregation" && stats.runtimeStats.contains(std::string(name))) {
          ++result;
        }
      }
    }
    return result;
  }

  static void assertGroupingSetLifetimeAccounting(const std::shared_ptr<Task>& task, int32_t numSets) {
    ASSERT_NE(task, nullptr);
    for (int32_t set = 0; set < numSets; ++set) {
      const auto prefix = fmt::format("gsagg.set{}", set);
      const auto inputRows = prefix + ".inputRows";
      const auto outputRows = prefix + ".outputRows";
      const auto totalInputRows = prefix + ".totalInputRows";
      const auto totalOutputRows = prefix + ".totalOutputRows";

      ASSERT_GT(groupingSetRuntimeStatOccurrences(task, inputRows), 0) << inputRows;
      ASSERT_GT(groupingSetRuntimeStatOccurrences(task, outputRows), 0) << outputRows;
      ASSERT_GT(groupingSetRuntimeStatOccurrences(task, totalInputRows), 0) << totalInputRows;
      ASSERT_GT(groupingSetRuntimeStatOccurrences(task, totalOutputRows), 0) << totalOutputRows;
      EXPECT_EQ(groupingSetRuntimeStat(task, inputRows), groupingSetRuntimeStat(task, totalInputRows))
          << "set " << set << " legacy input deltas do not cover the full lifetime";
      EXPECT_EQ(groupingSetRuntimeStat(task, outputRows), groupingSetRuntimeStat(task, totalOutputRows))
          << "set " << set << " legacy output deltas do not cover the full lifetime";
    }
  }
};

// Lattice construction.
// buildDerivationPlan is a pure function of grouping masks and the bypass hint.

TEST_F(MultiGroupingSetAggregationTest, latticeChainIsACascade) {
  // ROLLUP(k1,k2,k3): masks 111, 110, 100, 000 form a chain.
  const std::vector<GroupingSetMask> masks = {0b111, 0b110, 0b100, 0b000};
  auto plan = buildDerivationPlan(masks);

  EXPECT_EQ(plan.parent[0], kRawInputParent);
  EXPECT_EQ(plan.parent[1], 0);
  EXPECT_EQ(plan.parent[2], 1);
  EXPECT_EQ(plan.parent[3], 2);
  // Only the lattice root reads the input.
  EXPECT_EQ(std::count(plan.parent.begin(), plan.parent.end(), kRawInputParent), 1);
  // Parents strictly before children.
  std::vector<int32_t> rank(masks.size());
  for (size_t i = 0; i < plan.order.size(); ++i) {
    rank[plan.order[i]] = static_cast<int32_t>(i);
  }
  for (size_t i = 0; i < masks.size(); ++i) {
    if (plan.parent[i] != kRawInputParent) {
      EXPECT_LT(rank[plan.parent[i]], rank[i]);
    }
  }
}

TEST_F(MultiGroupingSetAggregationTest, latticeAntichainIsAFlatFanOut) {
  // No containment means every set is a root.
  const std::vector<GroupingSetMask> masks = {0b1100, 0b0011};
  auto plan = buildDerivationPlan(masks);
  EXPECT_EQ(plan.parent[0], kRawInputParent);
  EXPECT_EQ(plan.parent[1], kRawInputParent);
  EXPECT_TRUE(plan.children[0].empty());
  EXPECT_TRUE(plan.children[1].empty());
}

TEST_F(MultiGroupingSetAggregationTest, latticeCubePicksSmallestParent) {
  // CUBE(a,b,c), masks descending: index 0 == 111 ... index 7 == 000.
  const std::vector<GroupingSetMask> masks = {0b111, 0b110, 0b101, 0b100, 0b011, 0b010, 0b001, 0b000};
  auto plan = buildDerivationPlan(masks);

  // Only the top of the lattice reads input; the other seven sets are derived.
  EXPECT_EQ(std::count(plan.parent.begin(), plan.parent.end(), kRawInputParent), 1);
  EXPECT_EQ(plan.parent[0], kRawInputParent);

  // Every chosen parent must be a strict superset.
  for (size_t i = 0; i < masks.size(); ++i) {
    const auto p = plan.parent[i];
    if (p == kRawInputParent) {
      continue;
    }
    EXPECT_EQ(masks[i] & ~masks[p], 0u) << "set " << i;
    EXPECT_NE(masks[i], masks[p]) << "set " << i;
    // CUBE provides a parent with exactly one additional active key.
    EXPECT_EQ(__builtin_popcountll(masks[p]), __builtin_popcountll(masks[i]) + 1)
        << "set " << i << " should derive from a minimal superset";
  }
}

TEST_F(MultiGroupingSetAggregationTest, latticeUsesEstimatedParentRows) {
  // For the empty set, the popcount fallback prefers {k1}. Statistics show
  // that {k2,k3} is much smaller, so it is the cheaper derivation source.
  const std::vector<GroupingSetMask> masks = {0b111, 0b001, 0b110, 0b000};
  auto fallback = buildDerivationPlan(masks);
  EXPECT_EQ(fallback.parent[3], 1);

  const std::vector<double> estimatedRows = {2'000'000, 1'000'000, 400, 1};
  auto costed = buildDerivationPlan(masks, std::nullopt, estimatedRows);
  EXPECT_EQ(costed.parent[3], 2);
  EXPECT_EQ(std::count(costed.parent.begin(), costed.parent.end(), kRawInputParent), 1);
}

TEST_F(MultiGroupingSetAggregationTest, latticeParentPreferenceTiesAreStable) {
  // The two two-key candidates are equally desirable parents of the empty
  // set. Preserve the existing original-input-order tie break both with the
  // popcount fallback and with equal row estimates.
  const std::vector<GroupingSetMask> masks = {0b1111, 0b0011, 0b1100, 0b0000};

  const auto fallback = buildDerivationPlan(masks);
  EXPECT_EQ(fallback.parent, (std::vector<int32_t>{kRawInputParent, 0, 0, 1}));
  EXPECT_EQ(fallback.order, (std::vector<int32_t>{0, 1, 2, 3}));
  EXPECT_EQ(fallback.children[0], (std::vector<int32_t>{1, 2}));
  EXPECT_EQ(fallback.children[1], (std::vector<int32_t>{3}));

  const auto costed = buildDerivationPlan(masks, std::nullopt, {1'000, 10, 10, 1});
  EXPECT_EQ(costed.parent, (std::vector<int32_t>{kRawInputParent, 0, 0, 1}));

  // Equal estimates fall back to the smaller grouping set before input order.
  const std::vector<GroupingSetMask> differentWidths = {0b1111, 0b0110, 0b0001, 0b0000};
  const auto equalCost = buildDerivationPlan(differentWidths, std::nullopt, {1'000, 10, 10, 1});
  EXPECT_EQ(equalCost.parent, (std::vector<int32_t>{kRawInputParent, 0, 0, 2}));
}

TEST_F(MultiGroupingSetAggregationTest, latticeRejectsInvalidParentEstimates) {
  const std::vector<GroupingSetMask> masks = {0b11, 0b01};
  VELOX_ASSERT_THROW(
      buildDerivationPlan(masks, std::nullopt, {1}), "row estimates must be empty or have one entry per set");
  VELOX_ASSERT_THROW(
      buildDerivationPlan(masks, std::nullopt, {1, -1}), "row estimates must be finite and non-negative");
  VELOX_ASSERT_THROW(
      buildDerivationPlan(masks, std::nullopt, {1, std::numeric_limits<double>::quiet_NaN()}),
      "row estimates must be finite and non-negative");
  VELOX_ASSERT_THROW(
      buildDerivationPlan(masks, std::nullopt, {1, std::numeric_limits<double>::infinity()}),
      "row estimates must be finite and non-negative");
}

TEST_F(MultiGroupingSetAggregationTest, latticeBypassMarking) {
  const std::vector<GroupingSetMask> masks = {0b111, 0b110, 0b100, 0b000};
  auto plan = buildDerivationPlan(masks, /*childGroupedSet=*/0);
  EXPECT_TRUE(plan.bypass[0]);
  EXPECT_FALSE(plan.bypass[1]);
  // A bypassed root remains in the forest and feeds its children.
  EXPECT_EQ(plan.children[0].size(), 1u);
}

TEST_F(MultiGroupingSetAggregationTest, latticeRejectsBypassOfANonRoot) {
  // Only roots can bypass aggregation; a derived bypass would forward its
  // parent's states without merging.
  const std::vector<GroupingSetMask> masks = {0b111, 0b110, 0b100, 0b000};
  VELOX_ASSERT_THROW(
      buildDerivationPlan(masks, /*childGroupedSet=*/1), "The bypassed grouping set must be a lattice root");
}

TEST_F(MultiGroupingSetAggregationTest, latticeRejectsDuplicateMasks) {
  // The public lattice API validates masks independently of the plan node.
  const std::vector<GroupingSetMask> masks = {0b111, 0b110, 0b110};
  VELOX_ASSERT_THROW(buildDerivationPlan(masks), "Duplicate grouping-set mask");
}

TEST_F(MultiGroupingSetAggregationTest, latticeNonAdjacentParent) {
  // The nearest available superset can differ by more than one key.
  const std::vector<GroupingSetMask> masks = {0b111, 0b001};
  auto plan = buildDerivationPlan(masks);
  EXPECT_EQ(plan.parent[0], kRawInputParent);
  EXPECT_EQ(plan.parent[1], 0);
  EXPECT_EQ(std::count(plan.parent.begin(), plan.parent.end(), kRawInputParent), 1);
}

TEST_F(MultiGroupingSetAggregationTest, latticeWideMasks) {
  // GroupingSetMask is uint64_t and the node caps keys at 64. Exercise the top
  // bit: a chain over bits 63, 62, 61 must still be a cascade, with no shift
  // UB and no sign-extension surprise in popcount.
  const GroupingSetMask top = GroupingSetMask{1} << 63;
  const GroupingSetMask second = GroupingSetMask{1} << 62;
  const GroupingSetMask third = GroupingSetMask{1} << 61;
  const std::vector<GroupingSetMask> masks = {top | second | third, top | second, top, 0};
  auto plan = buildDerivationPlan(masks);
  EXPECT_EQ(plan.parent[0], kRawInputParent);
  EXPECT_EQ(plan.parent[1], 0);
  EXPECT_EQ(plan.parent[2], 1);
  EXPECT_EQ(plan.parent[3], 2);
}

TEST_F(MultiGroupingSetAggregationTest, latticeSupportsMaximumGroupingSetCount) {
  // A 64-level chain over 63 keys exercises every candidate-bitmap bit,
  // including rank 63, and the empty-set query with allCandidates == UINT64_MAX.
  std::vector<GroupingSetMask> masks;
  masks.reserve(kMaxGroupingSets);
  const auto allKeys = std::numeric_limits<GroupingSetMask>::max() >> 1;
  for (int32_t removedKeys = 0; removedKeys < kMaxGroupingSets; ++removedKeys) {
    masks.push_back(allKeys >> removedKeys);
  }

  const auto plan = buildDerivationPlan(masks);
  EXPECT_EQ(plan.parent[0], kRawInputParent);
  for (int32_t set = 1; set < kMaxGroupingSets; ++set) {
    EXPECT_EQ(plan.parent[set], set - 1);
  }
}

TEST_F(MultiGroupingSetAggregationTest, latticeRejectsMoreThanMaximumGroupingSetCount) {
  std::vector<GroupingSetMask> masks(kMaxGroupingSets + 1);
  std::iota(masks.begin(), masks.end(), GroupingSetMask{0});
  VELOX_ASSERT_THROW(
      buildDerivationPlan(masks),
      fmt::format("A grouping-set derivation plan supports at most {} sets", kMaxGroupingSets));
}

TEST_F(MultiGroupingSetAggregationTest, groupingSetSpecSupportsHighestBit) {
  const GroupingSetSpec set{GroupingSetMask{1} << 63, 7};
  EXPECT_EQ(set.numActiveKeys(), 1);
  EXPECT_TRUE(set.containsKey(63));
  EXPECT_FALSE(set.containsKey(62));
  EXPECT_FALSE(set.containsKey(64));
}

TEST_F(MultiGroupingSetAggregationTest, latticeRejectsEmptyMasks) {
  VELOX_ASSERT_THROW(buildDerivationPlan({}), "A grouping-set aggregation needs at least one set");
}

// Plan-node validation.

TEST_F(MultiGroupingSetAggregationTest, duplicateMaskRejected) {
  // Two sets at the same grain with different gids would each be a lattice root
  // and each scan the raw input for identical work. A planner bug; reject at
  // plan build time rather than paper over it.
  auto input = makeInput(1, 10, 2, 2, 2);
  PlanBuilder builder(pool());
  auto child = makeChild(builder, input, 3, defaultAggSpec());
  const auto* childAgg = dynamic_cast<const core::AggregationNode*>(child.get());
  ASSERT_NE(childAgg, nullptr);

  std::vector<Set> sets = {Set{0b011, 1}, Set{0b011, 2}};

  VELOX_ASSERT_THROW(
      std::make_shared<GroupingSetAggregationNode>(
          "gsagg-0",
          makeGroupingKeys(child, 3),
          sets,
          childAgg->aggregateNames(),
          makeIntermediateAggregates(childAgg),
          std::nullopt,
          child),
      "Duplicate grouping-set mask");

  std::vector<Set> tooManySets(kMaxGroupingSets + 1, Set{0b011, 1});
  VELOX_ASSERT_THROW(
      std::make_shared<GroupingSetAggregationNode>(
          "gsagg-too-many",
          makeGroupingKeys(child, 3),
          tooManySets,
          childAgg->aggregateNames(),
          makeIntermediateAggregates(childAgg),
          std::nullopt,
          child),
      fmt::format("supports at most {} grouping sets", kMaxGroupingSets));
}

TEST_F(MultiGroupingSetAggregationTest, zeroAggregatesRejected) {
  // Velox treats a GroupingSet with no aggregate functions as distinct
  // aggregation, whose extraction path differs from the merge-only contract
  // implemented by this operator.
  auto input = makeInput(1, 10, 2, 2, 2);
  PlanBuilder builder(pool());
  auto child = makeChild(builder, input, 3, defaultAggSpec());

  VELOX_ASSERT_THROW(
      std::make_shared<GroupingSetAggregationNode>(
          "gsagg-no-measures",
          makeGroupingKeys(child, 3),
          rollupSets(3),
          std::vector<std::string>{},
          std::vector<core::AggregationNode::Aggregate>{},
          std::nullopt,
          child),
      "needs at least one aggregate");
}

TEST_F(MultiGroupingSetAggregationTest, maskOutsideGroupingKeyRangeRejected) {
  auto input = makeInput(1, 10, 2, 2, 2);
  PlanBuilder builder(pool());
  auto child = makeChild(builder, input, 3, defaultAggSpec());
  const auto* childAgg = dynamic_cast<const core::AggregationNode*>(child.get());
  ASSERT_NE(childAgg, nullptr);

  // Bit 63 is representable by the mask, but this node has only keys 0..2.
  const std::vector<Set> sets = {Set{GroupingSetMask{1} << 63, 1}};
  VELOX_ASSERT_THROW(
      std::make_shared<GroupingSetAggregationNode>(
          "gsagg-0",
          makeGroupingKeys(child, 3),
          sets,
          childAgg->aggregateNames(),
          makeIntermediateAggregates(childAgg),
          std::nullopt,
          child),
      "contains keys outside the 3 grouping keys");
}

TEST_F(MultiGroupingSetAggregationTest, outputTypeShape) {
  // Guards the output-type contract independently of execution, so that a
  // regression fails here with a readable message rather than as a type
  // mismatch deep inside a GroupingSet.
  auto input = makeInput(1, 100, 3, 5, 7);
  PlanBuilder builder(pool());
  auto child = makeChild(builder, input, 3, defaultAggSpec());
  const auto* childAgg = dynamic_cast<const core::AggregationNode*>(child.get());
  ASSERT_NE(childAgg, nullptr);

  auto node = std::make_shared<GroupingSetAggregationNode>(
      "gsagg-0",
      makeGroupingKeys(child, 3),
      rollupSets(3),
      childAgg->aggregateNames(),
      makeIntermediateAggregates(childAgg),
      /*childGroupedSet=*/0,
      child);

  auto expected = makeExpectedOutputType(child, 3, childAgg->aggregateNames());
  EXPECT_EQ(*node->outputType(), *expected);
  EXPECT_EQ(node->gidChannel(), expected->size() - 1);
}

TEST_F(MultiGroupingSetAggregationTest, accumulatorTypeMismatchRejected) {
  auto input = makeInput(1, 100, 3, 5, 7);
  PlanBuilder builder(pool());
  auto child = makeChild(
      builder,
      input,
      /*numKeys=*/1,
      AggSpec{{"sum(v) as s"}, {{BIGINT()}}, {"sum(s) as s"}, {"s"}});
  const auto* childAgg = dynamic_cast<const core::AggregationNode*>(child.get());
  ASSERT_NE(childAgg, nullptr);

  auto aggregates = makeIntermediateAggregates(childAgg);
  auto source = builder.project({"k1", "cast(s as double) as s"}).planNode();

  VELOX_ASSERT_THROW(
      std::make_shared<GroupingSetAggregationNode>(
          "gsagg-0",
          makeGroupingKeys(source, 1),
          rollupSets(1),
          childAgg->aggregateNames(),
          aggregates,
          /*childGroupedSet=*/0,
          source),
      "expects intermediate type BIGINT");
}

TEST_F(MultiGroupingSetAggregationTest, planNodeSerdeRoundTrip) {
  // SetUp registers the custom node. Register the two source-node types in the
  // serialized subtree, then verify fields and derived output type explicitly.
  Type::registerSerDe();
  core::ITypedExpr::registerSerDe();
  {
    auto& reg = DeserializationWithContextRegistryForSharedPtr();
    reg.Register("ValuesNode", core::ValuesNode::create);
    reg.Register("AggregationNode", core::AggregationNode::create);
  }
  auto input = makeInput(1, 100, 3, 5, 7);
  PlanBuilder builder(pool());
  auto child = makeChild(builder, input, 3, defaultAggSpec());
  const auto* childAgg = dynamic_cast<const core::AggregationNode*>(child.get());
  ASSERT_NE(childAgg, nullptr);

  // Cover childGroupedSet present and absent, and a genuine CUBE lattice so that
  // Masks with interior holes -- not just rollup prefixes -- survive
  // the round-trip.
  using Shape = std::pair<std::vector<Set>, std::optional<int32_t>>;
  const std::vector<Shape> shapes = {
      {rollupSets(3), std::optional<int32_t>{0}},
      {rollupSets(3), std::nullopt},
      {cubeSets(3), std::optional<int32_t>{0}}};

  for (size_t s = 0; s < shapes.size(); ++s) {
    SCOPED_TRACE(fmt::format("shape={}", s));
    const auto& sets = shapes[s].first;
    const auto& childGroupedSet = shapes[s].second;

    core::PlanNodePtr node = std::make_shared<GroupingSetAggregationNode>(
        "gsagg-0",
        makeGroupingKeys(child, 3),
        sets,
        childAgg->aggregateNames(),
        makeIntermediateAggregates(childAgg),
        childGroupedSet,
        child);

    const auto serialized = node->serialize();
    ASSERT_EQ(serialized["groupingSets"].size(), sets.size());
    for (size_t i = 0; i < sets.size(); ++i) {
      const auto& serializedSet = serialized["groupingSets"][i];
      EXPECT_EQ(serializedSet["keyIsActive"].size(), 3);
      EXPECT_EQ(serializedSet.count("activeKeysMask"), 0);
    }
    if (s == 0) {
      auto malformed = serialized;
      malformed["groupingSets"][0]["keyIsActive"] = folly::dynamic::array(true, false);
      VELOX_ASSERT_THROW(
          ISerializable::deserialize<core::PlanNode>(malformed, pool()),
          "Every serialized grouping-set mask must match the grouping-key count");

      auto oversizedChild = serialized;
      oversizedChild["childGroupedSet"] = int64_t{1} << 32;
      VELOX_ASSERT_THROW(
          ISerializable::deserialize<core::PlanNode>(oversizedChild, pool()),
          "Serialized childGroupedSet must fit in int32");

      auto oversizedSets = serialized;
      oversizedSets["groupingSets"] = folly::dynamic::array;
      for (int32_t i = 0; i <= kMaxGroupingSets; ++i) {
        oversizedSets["groupingSets"].push_back(serialized["groupingSets"][0]);
      }
      VELOX_ASSERT_THROW(
          ISerializable::deserialize<core::PlanNode>(oversizedSets, pool()),
          fmt::format("supports at most {} grouping sets", kMaxGroupingSets));
    }
    const auto copy = ISerializable::deserialize<core::PlanNode>(serialized, pool());
    ASSERT_EQ(node->toString(true, true), copy->toString(true, true));

    // Also verify fields that identify grouping sets and the derived output.
    auto gsCopy = std::dynamic_pointer_cast<const GroupingSetAggregationNode>(copy);
    ASSERT_NE(gsCopy, nullptr);
    ASSERT_EQ(gsCopy->groupingSets().size(), sets.size());
    for (size_t i = 0; i < sets.size(); ++i) {
      EXPECT_EQ(gsCopy->groupingSets()[i].groupingId, sets[i].groupingId);
      EXPECT_EQ(gsCopy->groupingSets()[i].activeKeysMask, sets[i].activeKeysMask);
    }
    EXPECT_EQ(gsCopy->childGroupedSet(), childGroupedSet);
    EXPECT_EQ(*gsCopy->outputType(), *node->outputType());
  }

  // The legacy boolean-array wire format must also preserve bit 63. A signed
  // numeric JSON field could not do this safely.
  std::vector<std::string> wideNames = keyNames(64);
  wideNames.push_back("v");
  std::vector<VectorPtr> wideChildren;
  wideChildren.reserve(65);
  for (int32_t key = 0; key < 64; ++key) {
    wideChildren.push_back(makeFlatVector<int64_t>({key}));
  }
  wideChildren.push_back(makeFlatVector<int64_t>({1}));
  auto wideInput = makeRowVector(std::move(wideNames), std::move(wideChildren));
  PlanBuilder wideBuilder(pool());
  auto wideChild = wideBuilder.values({wideInput}).partialAggregation(keyNames(64), {"sum(v) as s"}).planNode();
  const auto* wideChildAgg = dynamic_cast<const core::AggregationNode*>(wideChild.get());
  ASSERT_NE(wideChildAgg, nullptr);

  core::PlanNodePtr wideNode = std::make_shared<GroupingSetAggregationNode>(
      "gsagg-wide",
      makeGroupingKeys(wideChild, 64),
      std::vector<Set>{{GroupingSetMask{1} << 63, 7}},
      wideChildAgg->aggregateNames(),
      makeIntermediateAggregates(wideChildAgg),
      std::nullopt,
      wideChild);
  const auto wideSerialized = wideNode->serialize();
  ASSERT_EQ(wideSerialized["groupingSets"][0]["keyIsActive"].size(), 64);
  EXPECT_TRUE(wideSerialized["groupingSets"][0]["keyIsActive"][63].asBool());
  const auto wideCopy = ISerializable::deserialize<core::PlanNode>(wideSerialized, pool());
  const auto wideGroupingSetCopy = std::dynamic_pointer_cast<const GroupingSetAggregationNode>(wideCopy);
  ASSERT_NE(wideGroupingSetCopy, nullptr);
  EXPECT_EQ(wideGroupingSetCopy->groupingSets()[0].activeKeysMask, GroupingSetMask{1} << 63);
}

// Differential correctness.

TEST_F(MultiGroupingSetAggregationTest, differentialCorrectness) {
  auto input = makeInput(
      /*numBatches=*/10,
      /*batchSize=*/1'000,
      /*k1Cardinality=*/5,
      /*k2Cardinality=*/23,
      /*k3Cardinality=*/97);
  assertFusedMatchesReference(input, /*numKeys=*/3);
}

TEST_F(MultiGroupingSetAggregationTest, differentialCorrectnessManySeeds) {
  // Cheap fuzzing over the key-cardinality shape: a steeply reducing hierarchy
  // (the case fusion is designed for) and a barely reducing one (the case the
  // abandon valve is designed for) exercise very different code paths.
  const std::vector<std::array<int32_t, 3>> shapes = {{2, 4, 8}, {3, 100, 5000}, {1, 1, 2}, {50, 50, 50}};
  for (uint32_t seed = 0; seed < shapes.size(); ++seed) {
    SCOPED_TRACE(fmt::format("seed={}", seed));
    auto input = makeInput(4, 500, shapes[seed][0], shapes[seed][1], shapes[seed][2], seed + 1);
    assertFusedMatchesReference(input, 3);
  }
}

TEST_F(MultiGroupingSetAggregationTest, differentialNoBypass) {
  // Withholding the bypass hint must not change results.
  auto input = makeInput(10, 1'000, 5, 23, 97);
  assertFusedMatchesReferenceForSets(input, 3, rollupSets(3), /*childGroupedSet=*/std::nullopt);
}

TEST_F(MultiGroupingSetAggregationTest, bypassAcceptsFlushedAndAbandonedUpstreamStates) {
  // Materialize one actual flushable child once, then give the exact same
  // duplicate-state stream to the bypass, no-bypass, and ordinary
  // Expand-plus-intermediate plans. This isolates the contract at the custom
  // operator boundary from the pinned reference operator's low-memory drain
  // bug.
  constexpr int32_t kNumKeys = 3;
  constexpr int32_t kCardinalityPerKey = 4;
  constexpr int32_t kMaxFinestGroups = kCardinalityPerKey * kCardinalityPerKey * kCardinalityPerKey;
  constexpr int32_t kNumBatches = 6;
  constexpr vector_size_t kBatchSize = 256;

  std::vector<RowVectorPtr> rawInput;
  rawInput.reserve(kNumBatches);
  for (int32_t batch = 0; batch < kNumBatches; ++batch) {
    std::vector<int64_t> k1(kBatchSize);
    std::vector<int64_t> k2(kBatchSize);
    std::vector<int64_t> k3(kBatchSize);
    std::vector<std::optional<int64_t>> v(kBatchSize);
    std::vector<std::optional<double>> w(kBatchSize);
    for (vector_size_t row = 0; row < kBatchSize; ++row) {
      k1[row] = row % kCardinalityPerKey;
      k2[row] = (row / kCardinalityPerKey) % kCardinalityPerKey;
      k3[row] = (row / (kCardinalityPerKey * kCardinalityPerKey)) % kCardinalityPerKey;

      const auto ordinal = static_cast<int64_t>(batch) * kBatchSize + row;
      // Keep one finest group entirely null and scatter nulls through all
      // others. avg's intermediate state is ROW(DOUBLE, BIGINT), so this
      // covers both null state handling and a multi-field accumulator.
      const bool allNullGroup = k1[row] == 0 && k2[row] == 0 && k3[row] == 0;
      v[row] = allNullGroup || ordinal % 5 == 0 ? std::nullopt : std::optional<int64_t>{ordinal % 101};
      w[row] = allNullGroup || ordinal % 7 == 0 ? std::nullopt
                                                : std::optional<double>{static_cast<double>(ordinal % 211) / 13.0};
    }
    rawInput.push_back(makeRowVector(
        {"k1", "k2", "k3", "v", "w"},
        {makeFlatVector<int64_t>(k1),
         makeFlatVector<int64_t>(k2),
         makeFlatVector<int64_t>(k3),
         makeNullableFlatVector<int64_t>(v),
         makeNullableFlatVector<double>(w)}));
  }

  const AggSpec spec{
      {"sum(v) as s", "avg(w) as m"}, {{BIGINT()}, {DOUBLE()}}, {"sum(s) as s", "avg(m) as m"}, {"s", "m"}};

  PlanBuilder childBuilder(pool());
  const auto child = makeChild(childBuilder, rawInput, kNumKeys, spec);
  const auto* childAgg = dynamic_cast<const core::AggregationNode*>(child.get());
  ASSERT_NE(childAgg, nullptr);
  const auto childPlanNodeId = child->id();

  // The first non-empty batch fills and flushes the table. The zero
  // thresholds then make HashAggregation abandon, so later raw batches are
  // converted one-for-one with GroupingSet::toIntermediate().
  const std::unordered_map<std::string, std::string> forceAbandon = {
      {core::QueryConfig::kAbandonPartialAggregationMinRows, "0"},
      {core::QueryConfig::kAbandonPartialAggregationMinPct, "0"},
      {core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", 1LL << 30)},
      {core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", 1LL << 30)},
  };
  std::shared_ptr<Task> upstreamTask;
  auto intermediate = AssertQueryBuilder(child).configs(forceAbandon).copyResults(pool(), upstreamTask);
  ASSERT_NE(upstreamTask, nullptr);
  ASSERT_NE(intermediate, nullptr);

  const auto upstream = upstreamPartialStats(upstreamTask, childPlanNodeId);
  ASSERT_GT(upstream.flushes, 0);
  ASSERT_GT(upstream.flushRows, 0);
  ASSERT_GT(upstream.abandonedRows, 0);
  EXPECT_EQ(upstream.outputRows, intermediate->size());
  EXPECT_EQ(upstream.flushRows + upstream.abandonedRows, upstream.outputRows);
  // There are at most 64 finest keys. More than 64 states proves that the
  // materialized stream contains duplicate keys across the flush/abandon
  // boundary rather than merely asserting that the valve fired.
  ASSERT_GT(intermediate->size(), kMaxFinestGroups);
  ASSERT_EQ(intermediate->type()->childAt(kNumKeys + 1)->kind(), TypeKind::ROW);

  const std::vector<RowVectorPtr> intermediateInput{intermediate};
  const auto sets = rollupSets(kNumKeys);

  std::shared_ptr<Task> bypassTask;
  auto bypass = AssertQueryBuilder(makeFusedPlanFromIntermediate(
                                       intermediateInput,
                                       childAgg,
                                       kNumKeys,
                                       sets,
                                       /*childGroupedSet=*/0,
                                       spec))
                    .copyResults(pool(), bypassTask);

  std::shared_ptr<Task> noBypassTask;
  auto noBypass = AssertQueryBuilder(makeFusedPlanFromIntermediate(
                                         intermediateInput,
                                         childAgg,
                                         kNumKeys,
                                         sets,
                                         /*childGroupedSet=*/std::nullopt,
                                         spec))
                      .copyResults(pool(), noBypassTask);

  auto reference =
      AssertQueryBuilder(makeReferencePlanFromIntermediate(intermediateInput, childAgg, kNumKeys, sets, spec))
          .copyResults(pool());

  ASSERT_TRUE(assertEqualResults({reference}, {bypass}));
  ASSERT_TRUE(assertEqualResults({reference}, {noBypass}));
  ASSERT_TRUE(assertEqualResults({noBypass}, {bypass}));

  ASSERT_NE(bypassTask, nullptr);
  ASSERT_NE(noBypassTask, nullptr);
  EXPECT_EQ(groupingSetRuntimeStat(bypassTask, "gsagg.bypassRows"), intermediate->size());
  EXPECT_EQ(groupingSetRuntimeStat(noBypassTask, "gsagg.bypassRows"), 0);
}

TEST_F(MultiGroupingSetAggregationTest, differentialCube) {
  // CUBE(k1,k2,k3): eight sets with multiple candidate parents.
  auto input = makeInput(8, 1'000, 4, 12, 40);
  assertFusedMatchesReferenceForSets(input, 3, cubeSets(3), /*childGroupedSet=*/0);
}

TEST_F(MultiGroupingSetAggregationTest, differentialAntichainGroupingSets) {
  // GROUPING SETS ((k1,k2),(k3)) forms a flat fan-out.
  auto input = makeInput(8, 1'000, 4, 12, 40);
  assertFusedMatchesReferenceForSets(input, 3, antichainSets(), /*childGroupedSet=*/std::nullopt);
}

TEST_F(MultiGroupingSetAggregationTest, differentialSingleKey) {
  // A single key is the two-set chain {k1} -> {}.
  auto input = makeInput(5, 1'000, 6, 1, 1);
  assertFusedMatchesReferenceForSets(input, 1, rollupSets(1), /*childGroupedSet=*/0);
}

TEST_F(MultiGroupingSetAggregationTest, finalRuntimeStatsRecorded) {
  auto input = makeInput(2, 100, 4, 12, 40);
  std::shared_ptr<Task> task;
  AssertQueryBuilder(makeFusedPlan(input, 3, rollupSets(3), /*childGroupedSet=*/0)).copyResults(pool(), task);

  bool foundOperator = false;
  for (const auto& pipeline : task->taskStats().pipelineStats) {
    for (const auto& stats : pipeline.operatorStats) {
      if (stats.operatorType != "GroupingSetAggregation") {
        continue;
      }
      foundOperator = true;
      EXPECT_EQ(stats.runtimeStats.count("gsagg.bypassRows"), 1);
      EXPECT_EQ(stats.runtimeStats.count("gsagg.flushes"), 1);
      EXPECT_EQ(stats.runtimeStats.count("gsagg.nestedDrains"), 1);
      EXPECT_EQ(stats.runtimeStats.count("gsagg.operatorTargetFlushes"), 1);
      EXPECT_EQ(stats.runtimeStats.count("gsagg.flushTargetBytes"), 1);
      EXPECT_EQ(stats.runtimeStats.count("gsagg.peakSampledFlushableBytes"), 1);
      EXPECT_EQ(stats.runtimeStats.count("gsagg.maxTargetOvershootBytes"), 1);
      EXPECT_EQ(stats.runtimeStats.count("gsagg.growthReservationCalls"), 1);
      EXPECT_EQ(stats.runtimeStats.count("gsagg.growthReservationBytes"), 1);
      EXPECT_EQ(stats.runtimeStats.count("abandonedPartialAggregationRows"), 1);
    }
  }
  EXPECT_TRUE(foundOperator);
}

TEST_F(MultiGroupingSetAggregationTest, globalVariableWidthStateFlushesAndResets) {
  // A global array_agg has no hash table, but its external accumulator memory
  // must still participate in pressure. Compare repeated global cycles with a
  // one-cycle run, then require evidence that pressure produced extra drains.
  auto input = makeInput(20, 1'000, 5, 40, 400);
  const AggSpec spec{{"array_agg(v) as l"}, {{BIGINT()}}, {"array_agg(l) as l"}, {"array_sort(l) as l"}};
  const std::vector<Set> globalOnly = {Set{0, 0}};

  auto expected = AssertQueryBuilder(makeFusedPlan(
                                         input,
                                         3,
                                         globalOnly,
                                         /*childGroupedSet=*/std::nullopt,
                                         spec))
                      .config(core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .copyResults(pool());

  std::shared_ptr<Task> task;
  auto actual = AssertQueryBuilder(makeFusedPlan(
                                       input,
                                       3,
                                       globalOnly,
                                       /*childGroupedSet=*/std::nullopt,
                                       spec))
                    .config(core::QueryConfig::kMaxPartialAggregationMemory, "4096")
                    .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, "4096")
                    .copyResults(pool(), task);

  EXPECT_TRUE(assertEqualResults({expected}, {actual}));

  bool foundOperator = false;
  for (const auto& pipeline : task->taskStats().pipelineStats) {
    for (const auto& stats : pipeline.operatorStats) {
      if (stats.operatorType != "GroupingSetAggregation") {
        continue;
      }
      foundOperator = true;
      const auto flushes = stats.runtimeStats.find("gsagg.flushes");
      ASSERT_NE(flushes, stats.runtimeStats.end());
      EXPECT_GT(flushes->second.sum, 1) << "global state never pressure-drained before the final sweep";
    }
  }
  EXPECT_TRUE(foundOperator);
}

TEST_F(MultiGroupingSetAggregationTest, noBypassMultiLevelPressureFlushes) {
  // The production default leaves finest-set bypass disabled. Exercise
  // repeated pressure with every level, including the finest root, owning a
  // GroupingSet.
  auto input = makeInput(20, 1'000, 5, 80, 1'000);
  const auto sets = rollupSets(3);

  auto expected = AssertQueryBuilder(makeFusedPlan(
                                         input,
                                         3,
                                         sets,
                                         /*childGroupedSet=*/std::nullopt))
                      .config(core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .copyResults(pool());

  std::shared_ptr<Task> task;
  auto actual = AssertQueryBuilder(makeFusedPlan(
                                       input,
                                       3,
                                       sets,
                                       /*childGroupedSet=*/std::nullopt))
                    .config(core::QueryConfig::kMaxPartialAggregationMemory, "4096")
                    .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, "4096")
                    .copyResults(pool(), task);

  EXPECT_TRUE(assertEqualResults({expected}, {actual}));

  int64_t flushes{0};
  int64_t pressureFlushedRows{0};
  bool foundOperator = false;
  for (const auto& pipeline : task->taskStats().pipelineStats) {
    for (const auto& stats : pipeline.operatorStats) {
      if (stats.operatorType != "GroupingSetAggregation") {
        continue;
      }
      foundOperator = true;
      const auto it = stats.runtimeStats.find("gsagg.flushes");
      ASSERT_NE(it, stats.runtimeStats.end());
      flushes += it->second.sum;
      const auto flushedRows = stats.runtimeStats.find("flushRowCount");
      ASSERT_NE(flushedRows, stats.runtimeStats.end());
      pressureFlushedRows += flushedRows->second.sum;
    }
  }
  EXPECT_TRUE(foundOperator);
  EXPECT_GT(flushes, static_cast<int64_t>(sets.size()))
      << "no-bypass path reached only its final sweep and never pressure-drained";
  EXPECT_GT(pressureFlushedRows, 0) << "pressure rows were not exported through the standard metric";
}

TEST_F(MultiGroupingSetAggregationTest, adaptiveAbandonWaitsForMinimumEvidenceRows) {
  // The finest level barely reduces, while the coarser levels collapse to at
  // most eight, two, and one groups. A tiny budget therefore creates repeated
  // pressure evidence at set 0 without needing a large test data set.
  constexpr int32_t kNumBatches = 12;
  constexpr vector_size_t kBatchSize = 256;
  auto input = makeInput(kNumBatches, kBatchSize, 2, 4, 100'000);
  const auto sets = rollupSets(3);
  const AggSpec spec{{"sum(v) as s"}, {{BIGINT()}}, {"sum(s) as s"}, {"s"}};

  auto expected = AssertQueryBuilder(makeFusedPlan(
                                         input,
                                         3,
                                         sets,
                                         /*childGroupedSet=*/std::nullopt,
                                         spec))
                      .config(core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .copyResults(pool());

  std::shared_ptr<Task> task;
  auto actual = AssertQueryBuilder(makeFusedPlan(
                                       input,
                                       3,
                                       sets,
                                       /*childGroupedSet=*/std::nullopt,
                                       spec))
                    .config(core::QueryConfig::kMaxPartialAggregationMemory, "4096")
                    .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, "4096")
                    .config(core::QueryConfig::kAbandonPartialAggregationMinRows, "1000000")
                    .config(core::QueryConfig::kAbandonPartialAggregationMinPct, "0")
                    .copyResults(pool(), task);

  ASSERT_TRUE(assertEqualResults({expected}, {actual}));
  ASSERT_NE(task, nullptr);
  EXPECT_GT(groupingSetRuntimeStat(task, "gsagg.flushes"), static_cast<int64_t>(sets.size()))
      << "the test did not collect more than the final-sweep drains";
  EXPECT_GT(groupingSetRuntimeStat(task, "gsagg.set0.flushTimes"), 2)
      << "set 0 did not complete multiple pressure drains before its final drain";
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.dynamicallyAbandonedSets"), 0);
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.dynamicAbandonRows"), 0);
  for (int32_t set = 0; set < sets.size(); ++set) {
    EXPECT_EQ(groupingSetRuntimeStat(task, fmt::format("gsagg.set{}.dynamicallyAbandoned", set)), 0);
    EXPECT_EQ(groupingSetRuntimeStat(task, fmt::format("gsagg.set{}.dynamicAbandonRows", set)), 0);
  }
  assertGroupingSetLifetimeAccounting(task, sets.size());
}

TEST_F(MultiGroupingSetAggregationTest, adaptiveAbandonDistinguishesPlannedBypass) {
  // With zero policy thresholds, three completed pressure drains are enough to
  // abandon the non-reducing finest table. Extra batches after that point pin
  // the identity phase and its row accounting.
  constexpr int32_t kNumBatches = 16;
  constexpr vector_size_t kBatchSize = 256;
  auto input = makeInput(kNumBatches, kBatchSize, 2, 4, 100'000);
  const auto sets = rollupSets(3);
  const AggSpec spec{{"sum(v) as s"}, {{BIGINT()}}, {"sum(s) as s"}, {"s"}};

  auto expected = AssertQueryBuilder(makeFusedPlan(
                                         input,
                                         3,
                                         sets,
                                         /*childGroupedSet=*/std::nullopt,
                                         spec))
                      .config(core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .copyResults(pool());

  const std::unordered_map<std::string, std::string> forceAdaptiveAbandon = {
      {core::QueryConfig::kMaxPartialAggregationMemory, "4096"},
      {core::QueryConfig::kMaxExtendedPartialAggregationMemory, "4096"},
      {core::QueryConfig::kAbandonPartialAggregationMinRows, "0"},
      {core::QueryConfig::kAbandonPartialAggregationMinPct, "0"},
  };

  std::shared_ptr<Task> dynamicTask;
  auto dynamic = AssertQueryBuilder(makeFusedPlan(
                                        input,
                                        3,
                                        sets,
                                        /*childGroupedSet=*/std::nullopt,
                                        spec))
                     .configs(forceAdaptiveAbandon)
                     .copyResults(pool(), dynamicTask);
  ASSERT_TRUE(assertEqualResults({expected}, {dynamic}));
  ASSERT_NE(dynamicTask, nullptr);
  EXPECT_EQ(groupingSetRuntimeStat(dynamicTask, "gsagg.set0.plannedBypass"), 0);
  EXPECT_EQ(groupingSetRuntimeStat(dynamicTask, "gsagg.set0.plannedBypassRows"), 0);
  EXPECT_EQ(groupingSetRuntimeStat(dynamicTask, "gsagg.set0.dynamicallyAbandoned"), 1);
  EXPECT_GT(groupingSetRuntimeStat(dynamicTask, "gsagg.set0.dynamicAbandonRows"), 0)
      << "no rows arrived after the adaptive valve opened";
  EXPECT_EQ(groupingSetRuntimeStat(dynamicTask, "gsagg.plannedBypassRows"), 0);
  EXPECT_GT(groupingSetRuntimeStat(dynamicTask, "gsagg.dynamicAbandonRows"), 0);
  EXPECT_EQ(
      groupingSetRuntimeStat(dynamicTask, "abandonedPartialAggregationRows"),
      groupingSetRuntimeStat(dynamicTask, "gsagg.dynamicAbandonRows"));
  assertGroupingSetLifetimeAccounting(dynamicTask, sets.size());

  std::shared_ptr<Task> plannedTask;
  auto planned = AssertQueryBuilder(makeFusedPlan(
                                        input,
                                        3,
                                        sets,
                                        /*childGroupedSet=*/0,
                                        spec))
                     .configs(forceAdaptiveAbandon)
                     .copyResults(pool(), plannedTask);
  ASSERT_TRUE(assertEqualResults({expected}, {planned}));
  ASSERT_NE(plannedTask, nullptr);
  EXPECT_EQ(groupingSetRuntimeStat(plannedTask, "gsagg.set0.plannedBypass"), 1);
  EXPECT_GT(groupingSetRuntimeStat(plannedTask, "gsagg.set0.plannedBypassRows"), 0);
  EXPECT_EQ(groupingSetRuntimeStat(plannedTask, "gsagg.set0.dynamicallyAbandoned"), 0);
  EXPECT_EQ(groupingSetRuntimeStat(plannedTask, "gsagg.set0.dynamicAbandonRows"), 0);
  EXPECT_GT(groupingSetRuntimeStat(plannedTask, "gsagg.plannedBypassRows"), 0);
  assertGroupingSetLifetimeAccounting(plannedTask, sets.size());
}

TEST_F(MultiGroupingSetAggregationTest, abandonedInternalLevelStillEnforcesDescendantHardCap) {
  // Feed materialized intermediate sum states so the tiny grouping-set budget
  // does not also constrain an upstream partial aggregate. Each batch contains
  // 16,384 rows over 4,096 full keys:
  //
  //   set 0 {k1,k2,k3}: 25% output/input
  //   set 1 {k1,k2}:    100% output/input (k3 is constant)
  //   set 2 {k1}:        25% output/input
  //
  // With a 100% abandon threshold, only internal set 1 opens its adaptive
  // pass-through valve. Later set-0 drains must then discover and hard-cap set
  // 2 through that abandoned level.
  constexpr int32_t kNumBatches = 6;
  constexpr vector_size_t kBatchSize = 16'384;
  constexpr int32_t kFullKeys = 4'096;
  std::vector<RowVectorPtr> intermediate;
  intermediate.reserve(kNumBatches);
  for (auto batch = 0; batch < kNumBatches; ++batch) {
    intermediate.push_back(makeRowVector(
        {"k1", "k2", "k3", "s"},
        {makeFlatVector<int64_t>(kBatchSize, [](auto row) { return (row % kFullKeys) / 4; }),
         makeFlatVector<int64_t>(kBatchSize, [](auto row) { return (row % kFullKeys) % 4; }),
         makeFlatVector<int64_t>(kBatchSize, [](auto) { return 0; }),
         makeFlatVector<int64_t>(kBatchSize, [](auto) { return 1; })}));
  }

  const auto sets = rollupSets(3);
  const AggSpec spec{{"sum(v) as s"}, {{BIGINT()}}, {"sum(s) as s"}, {"s"}};

  // Supply the base function metadata and raw argument type that are not
  // encoded in a ValuesNode containing already-materialized states.
  PlanBuilder metadataBuilder(pool());
  auto metadataChild = makeChild(metadataBuilder, makeInput(1, 1, 1, 1, 1), 3, spec);
  const auto* childAgg = dynamic_cast<const core::AggregationNode*>(metadataChild.get());
  ASSERT_NE(childAgg, nullptr);

  auto expected = AssertQueryBuilder(makeFusedPlanFromIntermediate(
                                         intermediate,
                                         childAgg,
                                         3,
                                         sets,
                                         /*childGroupedSet=*/std::nullopt,
                                         spec))
                      .config(core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .copyResults(pool());

  std::shared_ptr<Task> task;
  auto actual = AssertQueryBuilder(makeFusedPlanFromIntermediate(
                                       intermediate,
                                       childAgg,
                                       3,
                                       sets,
                                       /*childGroupedSet=*/std::nullopt,
                                       spec))
                    .config(core::QueryConfig::kMaxPartialAggregationMemory, "4096")
                    .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, "4096")
                    .config(core::QueryConfig::kAbandonPartialAggregationMinRows, "0")
                    .config(core::QueryConfig::kAbandonPartialAggregationMinPct, "100")
                    .config(core::QueryConfig::kPreferredOutputBatchRows, "8192")
                    .copyResults(pool(), task);

  ASSERT_TRUE(assertEqualResults({expected}, {actual}));
  ASSERT_NE(task, nullptr);
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.dynamicallyAbandonedSets"), 1);
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.set0.dynamicallyAbandoned"), 0);
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.set1.dynamicallyAbandoned"), 1);
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.set1.abandonPct"), 100);
  EXPECT_GE(groupingSetRuntimeStat(task, "gsagg.set1.flushTimes"), 3);
  EXPECT_GT(groupingSetRuntimeStat(task, "gsagg.set1.dynamicAbandonRows"), 0);
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.set2.dynamicallyAbandoned"), 0);
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.set3.dynamicallyAbandoned"), 0);
  const auto transitiveDrains = groupingSetRuntimeStat(task, "gsagg.transitiveHardCapDrains");
  EXPECT_GT(transitiveDrains, 0) << "a live descendant behind the abandoned internal level escaped its hard cap";
  EXPECT_GE(groupingSetRuntimeStat(task, "gsagg.nestedDrains"), transitiveDrains);
  assertGroupingSetLifetimeAccounting(task, sets.size());
}

// Forced flush.
//
// Set both normal and extended limits so the operator cannot grow out of the
// constrained budget.
//
// These low-memory differential tests are disabled because the pinned Velox
// Expand-plus-intermediate reference plan fails before it can be compared with
// the fused plan. The pressure/abandon cases fail inside HashAggregation; the
// variable-width case reports an accumulator/key type mismatch. PLAN_ONLY=fused
// isolates the custom operator while those reference-side blockers are fixed.

TEST_F(MultiGroupingSetAggregationTest, DISABLED_forcedFlush) {
  auto input = makeInput(20, 1'000, 7, 60, 700);

  const std::unordered_map<std::string, std::string> tinyBudget = {
      {core::QueryConfig::kMaxPartialAggregationMemory, "4096"},
      {core::QueryConfig::kMaxExtendedPartialAggregationMemory, "8192"},
  };

  assertFusedMatchesReference(input, /*numKeys=*/3, tinyBudget);
}

TEST_F(MultiGroupingSetAggregationTest, DISABLED_noMoreInputDuringDrain) {
  // A tiny budget and small output batches widen the window where noMoreInput()
  // arrives during a pressure drain.
  auto input = makeInput(30, 2'000, 9, 90, 3'000);
  input.push_back(makeInput(1, 3, 9, 90, 3'000, /*seed=*/99)[0]);

  assertFusedMatchesReference(
      input,
      3,
      {
          {core::QueryConfig::kMaxPartialAggregationMemory, "2048"},
          {core::QueryConfig::kMaxExtendedPartialAggregationMemory, "2048"},
          {core::QueryConfig::kPreferredOutputBatchRows, "16"},
      });
}

// Forced abandon.
//
// Zero thresholds trigger abandon after the first non-empty batch. The global
// set remains aggregated.

TEST_F(MultiGroupingSetAggregationTest, DISABLED_forcedAbandon) {
  auto input = makeInput(10, 1'000, 5, 40, 900);
  assertFusedMatchesReference(
      input,
      /*numKeys=*/3,
      {
          {core::QueryConfig::kAbandonPartialAggregationMinRows, "0"},
          {core::QueryConfig::kAbandonPartialAggregationMinPct, "0"},
      });
}

TEST_F(MultiGroupingSetAggregationTest, DISABLED_forcedAbandonWithFlush) {
  // Abandon and flush interacting: an abandoned node forwards states straight
  // into its children, which are themselves under memory pressure. This is the
  // combination most likely to double-feed or drop a state.
  auto input = makeInput(10, 1'000, 5, 40, 900);
  assertFusedMatchesReference(
      input,
      3,
      {
          {core::QueryConfig::kAbandonPartialAggregationMinRows, "0"},
          {core::QueryConfig::kAbandonPartialAggregationMinPct, "0"},
          {core::QueryConfig::kMaxPartialAggregationMemory, "4096"},
          {core::QueryConfig::kMaxExtendedPartialAggregationMemory, "4096"},
      });
}

// ===========================================================================
// Boundary shapes
// ===========================================================================

TEST_F(MultiGroupingSetAggregationTest, boundaryTwoKeys) {
  auto input = makeInput(5, 1'000, 4, 50, 1);
  assertFusedMatchesReference(input, /*numKeys=*/2);
}

TEST_F(MultiGroupingSetAggregationTest, boundaryAllRowsIdentical) {
  // Every set collapses to a single group, so every set is maximally reducing
  // and the bypass lane emits one row per input batch.
  const vector_size_t size = 2'000;
  auto batch = makeRowVector(
      {"k1", "k2", "k3", "v", "w"},
      {makeFlatVector<int64_t>(size, [](auto) { return 7; }),
       makeFlatVector<int64_t>(size, [](auto) { return 7; }),
       makeFlatVector<int64_t>(size, [](auto) { return 7; }),
       makeFlatVector<int64_t>(size, [](auto) { return 1; }),
       makeFlatVector<double>(size, [](auto) { return 1.5; })});
  assertFusedMatchesReference({batch, batch, batch}, /*numKeys=*/3);
}

TEST_F(MultiGroupingSetAggregationTest, boundaryAllRowsDistinctAtEveryLevel) {
  // The worst case for fusion: G_i == G_{i+1} at every level, so no set
  // reduces, memory grows monotonically and (with default config) the abandon
  // valve is the only thing that saves it. Correctness must not depend on which
  // way that goes.
  const vector_size_t size = 2'000;
  auto batch = makeRowVector(
      {"k1", "k2", "k3", "v", "w"},
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeFlatVector<double>(size, [](auto row) { return static_cast<double>(row); })});
  assertFusedMatchesReference({batch}, /*numKeys=*/3);
}

TEST_F(MultiGroupingSetAggregationTest, boundarySingleBatch) {
  // One batch, so noMoreInput() arrives immediately after a single addInput()
  // and the entire final sweep runs from a cold start with no pressure drain
  // ever having happened.
  auto input = makeInput(/*numBatches=*/1, /*batchSize=*/1'000, 4, 20, 200);
  assertFusedMatchesReference(input, /*numKeys=*/3);
}

TEST_F(MultiGroupingSetAggregationTest, boundaryEmptyBatch) {
  // Values::Values() DROPS zero-row vectors from its value list before the plan
  // ever runs, so a ValuesNode holding only an empty batch produces no batches
  // at all and addInput() is never called. What this covers is a plan whose
  // input list is non-empty but whose delivered row count is zero.
  auto empty = makeRowVector(rawInputType(), 0);
  assertFusedMatchesReference({empty}, /*numKeys=*/3);
}

TEST_F(MultiGroupingSetAggregationTest, boundaryZeroInputRows) {
  // Spark grouping sets emit no rows for empty input. Guard against a global
  // GroupingSet synthesizing an identity row during the final sweep.
  auto empty = makeRowVector(rawInputType(), 0);
  assertFusedMatchesReference({empty}, /*numKeys=*/3);

  // Pin the absolute cardinality in addition to differential equality.
  auto fused = AssertQueryBuilder(makeFusedPlan({empty}, 3, rollupSets(3), 0)).copyResults(pool());
  ASSERT_EQ(fused->size(), 0) << "empty input must produce zero rows, per Spark's grouping-sets "
                                 "semantics; got a synthesised grand-total row";
}

// ===========================================================================
// Aggregate coverage
// ===========================================================================
//
// The main differential tests already carry all three shapes together. These
// isolate each one so a failure names the accumulator rather than the plan.

TEST_F(MultiGroupingSetAggregationTest, aggregateFixedWidthOnly) {
  auto input = makeInput(8, 1'000, 5, 40, 400);
  assertFusedMatchesReferenceWithAggregates(input, 3, {"sum(v) as s"}, {{BIGINT()}}, {"sum(s) as s"}, {"s"});
}

TEST_F(MultiGroupingSetAggregationTest, aggregateStructIntermediate) {
  // avg(DOUBLE): intermediate is ROW(DOUBLE, BIGINT). A struct intermediate is
  // where a wrong column index shows up as a type error rather than a silent
  // wrong number.
  auto input = makeInput(8, 1'000, 5, 40, 400);
  assertFusedMatchesReferenceWithAggregates(input, 3, {"avg(w) as m"}, {{DOUBLE()}}, {"avg(m) as m"}, {"m"});
}

TEST_F(MultiGroupingSetAggregationTest, aggregateVariableWidthExternalMemory) {
  // array_agg(BIGINT) covers variable-width external-memory state.
  auto input = makeInput(8, 1'000, 5, 40, 400);
  assertFusedMatchesReferenceWithAggregates(
      input, 3, {"array_agg(v) as l"}, {{BIGINT()}}, {"array_agg(l) as l"}, {"array_sort(l) as l"});
}

TEST_F(MultiGroupingSetAggregationTest, DISABLED_aggregateVariableWidthUnderFlush) {
  // Variable-width external-memory state under repeated flushes.
  auto input = makeInput(8, 1'000, 5, 40, 400);
  assertFusedMatchesReferenceWithAggregates(
      input,
      3,
      {"array_agg(v) as l"},
      {{BIGINT()}},
      {"array_agg(l) as l"},
      {"array_sort(l) as l"},
      {
          {core::QueryConfig::kMaxPartialAggregationMemory, "4096"},
          {core::QueryConfig::kMaxExtendedPartialAggregationMemory, "4096"},
      });
}

// Spark partial buffers.
//
// Spark represents multi-field aggregate buffers as one ROW-typed Velox
// intermediate column:
//
//   spark sum(DECIMAL(p,s)) -> intermediate ROW(DECIMAL(min(38,p+10),s), boolean)
//                              == (sum, isEmpty)
//   spark avg(DOUBLE)       -> intermediate ROW(DOUBLE, BIGINT)
//                              == (sum, count)
//   spark avg(DECIMAL(p,s)) -> intermediate ROW(DECIMAL(...), BIGINT)
//
// Register them under a prefix so they coexist with the Presto functions above.
class MultiGroupingSetSparkBufferTest : public MultiGroupingSetAggregationTest {
 protected:
  void SetUp() override {
    MultiGroupingSetAggregationTest::SetUp();
    functions::aggregate::sparksql::registerAggregateFunctions(
        "spark_", /*withCompanionFunctions=*/true, /*overwrite=*/true);
  }

  // Add a physical decimal column because partialAggregation requires a field
  // input. Values remain within the declared precision.
  std::vector<RowVectorPtr> makeDecimalInput(
      int32_t numBatches,
      vector_size_t batchSize,
      int32_t k1Cardinality,
      int32_t k2Cardinality,
      int32_t k3Cardinality,
      uint32_t seed = 1234,
      const TypePtr& decimalType = DECIMAL(7, 2)) {
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);
    folly::Random::DefaultGenerator rng(seed);
    for (auto b = 0; b < numBatches; ++b) {
      std::vector<int64_t> k1(batchSize), k2(batchSize), k3(batchSize), v(batchSize), d(batchSize);
      std::vector<double> w(batchSize);
      for (auto i = 0; i < batchSize; ++i) {
        k1[i] = folly::Random::rand32(k1Cardinality, rng);
        k2[i] = folly::Random::rand32(k2Cardinality, rng);
        k3[i] = folly::Random::rand32(k3Cardinality, rng);
        v[i] = folly::Random::rand32(1000, rng);
        w[i] = static_cast<double>(folly::Random::rand32(1000, rng)) / 7.0;
        // Unscaled decimal(7,2): 0..999999 -> 0.00..9999.99.
        d[i] = folly::Random::rand32(1'000'000, rng);
      }
      batches.push_back(makeRowVector(
          {"k1", "k2", "k3", "v", "w", "d"},
          {makeFlatVector<int64_t>(k1),
           makeFlatVector<int64_t>(k2),
           makeFlatVector<int64_t>(k3),
           makeFlatVector<int64_t>(v),
           makeFlatVector<double>(w),
           makeFlatVector<int64_t>(d, decimalType)}));
    }
    return batches;
  }

  /// Eight-key q67-shaped input: four integer keys followed by four string
  /// keys. The string columns mix inline, external-buffer, and null values.
  /// DECIMAL(18,4) matches the widened product that feeds the query's Spark
  /// decimal sum and produces a two-field ROW accumulator.
  std::vector<RowVectorPtr> makeQ67ShapeInput(int32_t numBatches, vector_size_t batchSize) {
    const auto decimalType = DECIMAL(18, 4);
    const std::array<std::optional<std::string>, 4> s1Values = {"R", "A", "RETURN", std::nullopt};
    const std::array<std::optional<std::string>, 3> s2Values = {"O", "F", std::nullopt};
    const std::array<std::optional<std::string>, 4> s3Values = {"AIR", "SHIP", "TRUCK", std::nullopt};
    const std::array<std::optional<std::string>, 4> s4Values = {
        "DELIVER IN PERSON", "TAKE BACK RETURN", "EXPRESS PRIORITY COURIER", std::nullopt};

    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);
    for (auto batch = 0; batch < numBatches; ++batch) {
      std::vector<int32_t> k1(batchSize), k2(batchSize), k3(batchSize), k4(batchSize);
      std::vector<std::optional<std::string>> s1(batchSize), s2(batchSize), s3(batchSize), s4(batchSize);
      std::vector<std::optional<int64_t>> d(batchSize);

      for (auto row = 0; row < batchSize; ++row) {
        const int64_t id = static_cast<int64_t>(batch) * batchSize + row;
        // A mixed-radix prefix creates progressively larger rollup levels and
        // more than one output batch at the finest grain.
        k1[row] = id % 3;
        k2[row] = (id / 3) % 5;
        k3[row] = (id / 15) % 7;
        k4[row] = (id / 105) % 11;
        s1[row] = s1Values[(id / 2) % s1Values.size()];
        s2[row] = s2Values[(id / 5) % s2Values.size()];
        s3[row] = s3Values[(id / 11) % s3Values.size()];
        s4[row] = s4Values[(id / 17) % s4Values.size()];

        // Keep values within DECIMAL(18,4), with nulls interspersed across
        // batches so the Spark sum's isEmpty field is exercised.
        if (id % 13 == 0) {
          d[row] = std::nullopt;
        } else {
          d[row] = (id * 10'007) % 100'000'000;
        }
      }

      batches.push_back(makeRowVector(
          {"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "d"},
          {makeFlatVector<int32_t>(k1),
           makeFlatVector<int32_t>(k2),
           makeFlatVector<int32_t>(k3),
           makeFlatVector<int32_t>(k4),
           makeNullableFlatVector<std::string>(s1),
           makeNullableFlatVector<std::string>(s2),
           makeNullableFlatVector<std::string>(s3),
           makeNullableFlatVector<std::string>(s4),
           makeNullableFlatVector<int64_t>(d, decimalType)}));
    }
    return batches;
  }
};

TEST_F(MultiGroupingSetSparkBufferTest, decimalSum) {
  // Spark decimal sum uses the two-field (sum, isEmpty) state.
  auto input = makeDecimalInput(10, 1'000, 5, 23, 97);
  assertFusedMatchesReferenceWithAggregates(
      input, 3, {"spark_sum(d) as s"}, {{DECIMAL(7, 2)}}, {"spark_sum(s) as s"}, {"s"});
}

TEST_F(MultiGroupingSetSparkBufferTest, avgDouble) {
  // Spark avg(DOUBLE) uses the (sum, count) state.
  auto input = makeInput(10, 1'000, 5, 23, 97);
  assertFusedMatchesReferenceWithAggregates(
      input, 3, {"spark_avg(w) as m"}, {{DOUBLE()}}, {"spark_avg(m) as m"}, {"m"});
}

TEST_F(MultiGroupingSetSparkBufferTest, avgDecimal) {
  // Use DECIMAL(12,2) because Velox rejects lower-precision decimal averages.
  auto input = makeDecimalInput(10, 1'000, 5, 23, 97, /*seed=*/1234, /*decimalType=*/DECIMAL(12, 2));
  assertFusedMatchesReferenceWithAggregates(
      input, 3, {"spark_avg(d) as m"}, {{DECIMAL(12, 2)}}, {"spark_avg(m) as m"}, {"m"});
}

TEST_F(MultiGroupingSetSparkBufferTest, mixedMultiAndSingleColumn) {
  // Mix row-typed and scalar states to catch per-aggregate channel errors.
  auto input = makeDecimalInput(10, 1'000, 5, 23, 97);
  assertFusedMatchesReferenceWithAggregates(
      input,
      3,
      {"spark_sum(d) as s", "spark_avg(w) as m", "spark_sum(v) as t"},
      {{DECIMAL(7, 2)}, {DOUBLE()}, {BIGINT()}},
      {"spark_sum(s) as s", "spark_avg(m) as m", "spark_sum(t) as t"},
      {"s", "m", "t"});
}

TEST_F(MultiGroupingSetSparkBufferTest, mixedMultiColumnCube) {
  // Exercise row-typed states across a CUBE derivation lattice.
  auto input = makeDecimalInput(8, 1'000, 4, 12, 40);
  const auto spec = AggSpec{
      {"spark_sum(d) as s", "spark_avg(w) as m"},
      {{DECIMAL(7, 2)}, {DOUBLE()}},
      {"spark_sum(s) as s", "spark_avg(m) as m"},
      {"s", "m"}};
  assertFusedMatchesReferenceForSets(input, 3, cubeSets(3), /*childGroupedSet=*/0, {}, spec);
}

TEST_F(MultiGroupingSetSparkBufferTest, q67ShapeEightMixedKeysDecimalRollup) {
  auto input = makeQ67ShapeInput(/*numBatches=*/6, /*batchSize=*/257);
  const auto sets = rollupSets(/*numKeys=*/8);
  ASSERT_EQ(sets.size(), 9);
  const auto spec = AggSpec{{"spark_sum(d) as s"}, {{DECIMAL(18, 4)}}, {"spark_sum(s) as s"}, {"s"}};

  for (const auto childGroupedSet : {std::optional<int32_t>{std::nullopt}, std::optional<int32_t>{0}}) {
    SCOPED_TRACE(childGroupedSet.has_value() ? "finest-set bypass enabled" : "finest-set bypass disabled");
    assertFusedMatchesReferenceForSets(input, 8, sets, childGroupedSet, {}, spec);
  }
}

TEST_F(MultiGroupingSetSparkBufferTest, q67ShapeEightMixedKeysDecimalRollupUnderPressure) {
  // Close the q67-specific pressure cross-product: nine cascade levels,
  // nullable/string keys, Spark's ROW(decimal, isEmpty) state, and the
  // production-default no-bypass path. Compare against the same fused plan at a
  // large budget because the pinned Expand reference is not pressure-safe.
  auto input = makeQ67ShapeInput(/*numBatches=*/6, /*batchSize=*/257);
  const auto sets = rollupSets(/*numKeys=*/8);
  ASSERT_EQ(sets.size(), 9);
  const auto spec = AggSpec{{"spark_sum(d) as s"}, {{DECIMAL(18, 4)}}, {"spark_sum(s) as s"}, {"s"}};

  auto expected = AssertQueryBuilder(makeFusedPlan(input, 8, sets, /*childGroupedSet=*/std::nullopt, spec))
                      .config(core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                      .copyResults(pool());

  std::shared_ptr<Task> task;
  auto actual = AssertQueryBuilder(makeFusedPlan(input, 8, sets, /*childGroupedSet=*/std::nullopt, spec))
                    .config(core::QueryConfig::kMaxPartialAggregationMemory, "4096")
                    .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, "4096")
                    .copyResults(pool(), task);

  ASSERT_TRUE(assertEqualResults({expected}, {actual}));
  ASSERT_NE(task, nullptr);
  EXPECT_GT(groupingSetRuntimeStat(task, "gsagg.flushes"), static_cast<int64_t>(sets.size()))
      << "the nine-level q67 shape reached only final drains and never pressure-drained";
  EXPECT_GT(groupingSetRuntimeStat(task, "flushRowCount"), 0)
      << "q67-shaped pressure rows were not exported through the standard metric";
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.numLatticeRoots"), 1);
  EXPECT_EQ(groupingSetRuntimeStat(task, "gsagg.dynamicallyAbandonedSets"), 0);
  assertGroupingSetLifetimeAccounting(task, sets.size());
}

TEST_F(MultiGroupingSetSparkBufferTest, DISABLED_multiColumnBudgetSweep) {
  // Compare row-typed states across budgets while high cardinality forces
  // repeated flushes. The persistent test helper applies the required
  // child-HashAggregation buffered-input patch and test-pressure opts this
  // regression in; the default gtest run keeps it disabled.
  auto input = makeDecimalInput(
      /*numBatches=*/40,
      /*batchSize=*/2'000,
      /*k1Cardinality=*/8,
      /*k2Cardinality=*/120,
      /*k3Cardinality=*/5'000);
  const auto spec = AggSpec{
      {"spark_sum(d) as s", "spark_avg(w) as m", "spark_sum(v) as t"},
      {{DECIMAL(7, 2)}, {DOUBLE()}, {BIGINT()}},
      {"spark_sum(s) as s", "spark_avg(m) as m", "spark_sum(t) as t"},
      {"s", "m", "t"}};
  assertFusedSelfConsistentAcrossBudgets(
      input, 3, rollupSets(3), spec, {4096, 65536, 262144, 1LL << 20, 4LL << 20, 16LL << 20});
}

TEST_F(MultiGroupingSetSparkBufferTest, decimalSumAbsoluteCorrectness) {
  // Compare the rollup grand total with an independent global aggregation.
  auto input = makeDecimalInput(6, 1'000, 4, 20, 200);

  auto fused = AssertQueryBuilder(makeFusedPlan(
                                      input,
                                      3,
                                      rollupSets(3),
                                      /*childGroupedSet=*/0,
                                      AggSpec{{"spark_sum(d) as s"}, {{DECIMAL(7, 2)}}, {"spark_sum(s) as s"}, {"s"}}))
                   .copyResults(pool());

  // Independent grand total: sum over all rows, no grouping.
  auto grandTotal =
      AssertQueryBuilder(PlanBuilder(pool()).values(input).singleAggregation({}, {"spark_sum(d) as s"}).planNode())
          .copyResults(pool());
  ASSERT_EQ(grandTotal->size(), 1);

  // The fused output is ROW(k1,k2,k3,gid,s). The grand-total set is gid == 3
  // (all keys masked out); there must be exactly one such row and its s must
  // equal the independent grand total.
  auto* gidVec = fused->childAt(3)->asFlatVector<int64_t>();
  ASSERT_NE(gidVec, nullptr);
  const auto& fusedS = fused->childAt(4);
  const auto& expectedS = grandTotal->childAt(0);
  int32_t grandRows = 0;
  for (auto i = 0; i < fused->size(); ++i) {
    if (gidVec->valueAt(i) == 3) {
      ++grandRows;
      ASSERT_TRUE(fusedS->equalValueAt(expectedS.get(), i, 0)) << "grand-total decimal sum mismatch at fused row " << i;
    }
  }
  EXPECT_EQ(grandRows, 1) << "rollup must emit exactly one grand-total row";
}

} // namespace

// Opt-in rollup-stage microbenchmark. Run with
// --gtest_also_run_disabled_tests --gtest_filter=*q67ProfileBenchmark.
// This is not an end-to-end TPC-DS measurement.
TEST_F(MultiGroupingSetAggregationTest, DISABLED_q67ProfileBenchmark) {
  const int32_t numBatches = 40;
  const vector_size_t batchSize = 25'000; // 1M rows
  auto input = makeInput(numBatches, batchSize, 100, 3'000, 900'000);

  const auto rows = numBatches * batchSize;
  auto groups = AssertQueryBuilder(
                    PlanBuilder(pool()).values(input).singleAggregation({"k1", "k2", "k3"}, {"count(1)"}).planNode())
                    .copyResults(pool())
                    ->size();
  fprintf(
      stderr, "\n[q67-profile] %d rows, %d finest groups, %.2f rows/group\n", rows, (int)groups, (double)rows / groups);

  auto timeIt = [&](const core::PlanNodePtr& plan) {
    auto t0 = std::chrono::steady_clock::now();
    auto r = AssertQueryBuilder(plan).copyResults(pool());
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    return std::make_pair(ms, (int)r->size());
  };

  const auto sets = rollupSets(3);
  auto fusedPlan = [&]() { return makeFusedPlan(input, 3, sets, 0); };
  auto refPlan = [&]() { return makeReferencePlan(input, 3, sets); };

  std::vector<int64_t> fusedMs, refMs;
  int fusedRows = 0, refRows = 0;
  const char* only = getenv("ONLY");
  const bool runFused = (only == nullptr || std::string(only) == "fused");
  const bool runRef = (only == nullptr || std::string(only) == "ref");
  if (runFused) {
    timeIt(fusedPlan());
  }
  if (runRef) {
    timeIt(refPlan());
  }
  for (int i = 0; i < 3; ++i) {
    if (runFused) {
      auto f = timeIt(fusedPlan());
      fusedMs.push_back(f.first);
      fusedRows = f.second;
    }
    if (runRef) {
      auto r = timeIt(refPlan());
      refMs.push_back(r.first);
      refRows = r.second;
    }
  }
  if (!runFused || !runRef) {
    if (runFused) {
      fprintf(
          stderr,
          "[q67-profile] FUSED best=%lldms rows=%d\n",
          (long long)*std::min_element(fusedMs.begin(), fusedMs.end()),
          fusedRows);
    }
    if (runRef) {
      fprintf(
          stderr,
          "[q67-profile] REFERENCE best=%lldms rows=%d\n",
          (long long)*std::min_element(refMs.begin(), refMs.end()),
          refRows);
    }
    fflush(stderr);
    return;
  }
  std::sort(fusedMs.begin(), fusedMs.end());
  std::sort(refMs.begin(), refMs.end());
  fprintf(
      stderr,
      "[q67-profile] reference(Expand+kIntermediate) best=%lldms median=%lldms rows=%d\n",
      (long long)refMs.front(),
      (long long)refMs[1],
      refRows);
  fprintf(
      stderr,
      "[q67-profile] fused(GroupingSetAggregation)   best=%lldms median=%lldms rows=%d\n",
      (long long)fusedMs.front(),
      (long long)fusedMs[1],
      fusedRows);
  fprintf(
      stderr,
      "[q67-profile] SPEEDUP best=%.2fx median=%.2fx  (rows match=%d)\n\n",
      (double)refMs.front() / fusedMs.front(),
      (double)refMs[1] / fusedMs[1],
      (int)(refRows == fusedRows));
  fflush(stderr);
  EXPECT_EQ(refRows, fusedRows);
}

// Compare the same operator at a large budget and a range of constrained
// budgets. FLUSH_TRACE prints per-operator row counts when diagnosing failures;
// FLUSH_BUDGETS overrides the default sweep.

// The persistent test helper applies the buffered-input fix in
// ep/build-velox/src/modify_hash_aggregation_input_buffer.patch before
// building.
// test-pressure opts this regression in; the default gtest run keeps it
// disabled because an unpatched external Velox checkout can overwrite an
// undrained child-HashAggregation batch.
TEST_F(MultiGroupingSetAggregationTest, DISABLED_flushCorrectnessSmallBudget) {
  // 3-key rollup. k3's cardinality is deliberately high so the finest node
  // holds far more groups than a small budget can house, which is what makes
  // the drain cycle recur rather than fire once.
  auto input = makeInput(
      /*numBatches=*/40,
      /*batchSize=*/2'000,
      /*k1Cardinality=*/8,
      /*k2Cardinality=*/120,
      /*k3Cardinality=*/5'000);

  const auto sets = rollupSets(3);

  auto runAt = [&](int64_t budget) {
    std::unordered_map<std::string, std::string> configs = {
        {core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", budget)},
        {core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", budget)},
    };
    std::shared_ptr<Task> task;
    auto result = AssertQueryBuilder(makeFusedPlan(input, 3, sets, /*childGroupedSet=*/0))
                      .configs(configs)
                      .copyResults(pool(), task);
    // Optional per-operator row accounting.
    if (getenv("FLUSH_TRACE") != nullptr) {
      for (const auto& pipeline : task->taskStats().pipelineStats) {
        for (const auto& op : pipeline.operatorStats) {
          fprintf(
              stderr,
              "  [budget=%lld] %-28s in=%lld out=%lld\n",
              (long long)budget,
              op.operatorType.c_str(),
              (long long)op.inputPositions,
              (long long)op.outputPositions);
          for (const auto& [name, counter] : op.runtimeStats) {
            fprintf(
                stderr,
                "        %-40s count=%lld sum=%lld\n",
                name.c_str(),
                (long long)counter.count,
                (long long)counter.sum);
          }
        }
      }
      fflush(stderr);
    }
    return result;
  };

  // Baseline: the same plan at a budget no drain can reach.
  const int64_t kLargeBudget = 1LL << 30;
  auto expected = runAt(kLargeBudget);
  fprintf(stderr, "[flush-sweep] expected (budget=1GiB) rows=%d\n", (int)expected->size());
  fflush(stderr);

  std::vector<int64_t> budgets = {4096, 65536, 262144, 1LL << 20, 4LL << 20, 16LL << 20};
  // Iteration hook: FLUSH_BUDGETS="4096,65536,1048576".
  if (const char* env = getenv("FLUSH_BUDGETS")) {
    budgets.clear();
    std::string s(env);
    size_t pos = 0;
    while (pos < s.size()) {
      auto comma = s.find(',', pos);
      if (comma == std::string::npos) {
        comma = s.size();
      }
      budgets.push_back(std::stoll(s.substr(pos, comma - pos)));
      pos = comma + 1;
    }
  }

  std::vector<std::pair<int64_t, bool>> results;
  for (auto budget : budgets) {
    auto actual = runAt(budget);
    const bool equal = (actual->size() == expected->size()) && assertEqualResults({expected}, {actual});
    fprintf(
        stderr,
        "[flush-sweep] budget=%lldB rows=%d expected=%d %s\n",
        (long long)budget,
        (int)actual->size(),
        (int)expected->size(),
        equal ? "OK" : "MISMATCH");
    fflush(stderr);
    results.emplace_back(budget, equal);
  }

  for (const auto& [budget, equal] : results) {
    EXPECT_TRUE(equal) << "fused plan disagreed with itself at budget " << budget << " bytes";
  }
}

// Arbitration safety.
//
// Run the operator without a blocking final aggregation, pause after each
// output batch, and reclaim its leaf pool. Finalizing the collected partial
// batches must match a large-budget run.

namespace {
// Locate the operator's leaf memory pool. Its name is
// "op.<planNodeId>.<pipelineId>.<driverId>.GroupingSetAggregation"
// (Task::addOperatorPool), so the operator type is the stable suffix.
memory::MemoryPool* findGsaggPool(memory::MemoryPool* pool) {
  static const std::string kSuffix = ".GroupingSetAggregation";
  memory::MemoryPool* found = nullptr;
  pool->visitChildren([&](memory::MemoryPool* child) {
    if (child->isLeaf()) {
      const auto& name = child->name();
      if (name.size() >= kSuffix.size() && name.compare(name.size() - kSuffix.size(), kSuffix.size(), kSuffix) == 0) {
        found = child;
        return false;
      }
    } else if ((found = findGsaggPool(child)) != nullptr) {
      return false;
    }
    return true;
  });
  return found;
}

struct ArbitrationRun {
  std::vector<RowVectorPtr> partialBatches;
  int64_t reclaimCalls{0};
  int64_t freedBytes{0};
  int32_t reclaimPoints{0};
};
} // namespace

class MultiGroupingSetArbitrationTest : public MultiGroupingSetAggregationTest {
 protected:
  // Stop at the custom operator so partial batches reach the cursor directly.
  core::PlanNodePtr makeOperatorOnlyPlan(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      const AggSpec& spec) {
    PlanBuilder builder(pool());
    auto child = makeChild(builder, input, numKeys, spec);
    const auto* childAgg = dynamic_cast<const core::AggregationNode*>(child.get());
    VELOX_CHECK_NOT_NULL(childAgg);
    auto aggregates = makeIntermediateAggregates(childAgg);
    auto aggNames = childAgg->aggregateNames();
    auto groupingKeys = makeGroupingKeys(child, numKeys);
    return builder
        .addNode([&](std::string id, core::PlanNodePtr source) {
          return std::make_shared<GroupingSetAggregationNode>(
              std::move(id),
              groupingKeys,
              sets,
              aggNames,
              aggregates,
              /*childGroupedSet=*/std::optional<int32_t>{0},
              std::move(source));
        })
        .planNode();
  }

  // Re-aggregates collected partial batches into the final answer, so an
  // arbitration run's output can be compared to a plain run's.
  RowVectorPtr finalize(const std::vector<RowVectorPtr>& partialBatches, int32_t numKeys, const AggSpec& spec) {
    return AssertQueryBuilder(PlanBuilder(pool())
                                  .values(partialBatches)
                                  .finalAggregation(appendGid(keyNames(numKeys)), spec.final, spec.rawTypes)
                                  .project(finalProjection(numKeys, spec))
                                  .planNode())
        .copyResults(pool());
  }

  ArbitrationRun runWithReclaimEachBatch(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      const AggSpec& spec,
      int64_t budget) {
    // A root reclaimer lets Task attach a reclaimer to the operator pool.
    auto queryCtx = core::QueryCtx::create(driverExecutor_.get());
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(),
        /*capacity=*/1LL << 30,
        exec::MemoryReclaimer::create()));

    CursorParameters params;
    params.planNode = makeOperatorOnlyPlan(input, numKeys, sets, spec);
    params.queryCtx = queryCtx;
    params.maxDrivers = 1;
    params.queryConfigs = {
        {core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", budget)},
        {core::QueryConfig::kMaxExtendedPartialAggregationMemory, fmt::format("{}", budget)},
    };
    auto cursor = TaskCursor::create(params);

    // The cursor copies each batch into the task's own pool, which dies with
    // the cursor at the end of this function. Deep-copy into the test's pool so
    // the collected batches outlive the arbitration task.
    auto deepCopy = [&](const RowVectorPtr& src) {
      auto dst = std::static_pointer_cast<RowVector>(BaseVector::create(src->type(), src->size(), pool()));
      dst->copy(src.get(), 0, 0, src->size());
      return dst;
    };

    ArbitrationRun run;
    memory::MemoryPool* opPool = nullptr;
    while (cursor->moveNext()) {
      run.partialBatches.push_back(deepCopy(cursor->current()));
      auto task = cursor->task();
      if (opPool == nullptr) {
        opPool = findGsaggPool(task->pool());
      }
      if (opPool == nullptr || opPool->reclaimer() == nullptr) {
        continue;
      }
      // Reclaim requires a paused task and an arbitration context.
      task->requestPause().wait();
      const int64_t before = static_cast<int64_t>(opPool->reservedBytes());
      {
        memory::ScopedMemoryArbitrationContext arbCtx{opPool};
        memory::MemoryReclaimer::Stats stats;
        opPool->reclaim(/*targetBytes=*/0, /*maxWaitMs=*/0, stats);
      }
      const int64_t after = static_cast<int64_t>(opPool->reservedBytes());
      Task::resume(task);
      ++run.reclaimPoints;
      run.freedBytes += std::max<int64_t>(before - after, 0);
    }

    const auto taskStats = cursor->task()->taskStats();
    for (const auto& pipeline : taskStats.pipelineStats) {
      for (const auto& op : pipeline.operatorStats) {
        if (op.operatorType != "GroupingSetAggregation") {
          continue;
        }
        if (auto it = op.runtimeStats.find("gsagg.reclaim.count"); it != op.runtimeStats.end()) {
          run.reclaimCalls = std::max<int64_t>(run.reclaimCalls, it->second.sum);
        }
      }
    }
    return run;
  }

  void assertArbitrationSafe(const AggSpec& spec, const char* label) {
    SCOPED_TRACE(label);
    auto input = makeInput(10, 1'000, 5, 40, 400);
    const auto sets = rollupSets(3);

    // Reference: the full plan at a large budget, without reclaim.
    auto expected = AssertQueryBuilder(makeFusedPlan(input, 3, sets, /*childGroupedSet=*/0, spec))
                        .config(core::QueryConfig::kMaxPartialAggregationMemory, fmt::format("{}", 1LL << 30))
                        .copyResults(pool());

    // A 16 KiB budget forces repeated flushes and reclaimer callbacks.
    auto run = runWithReclaimEachBatch(input, 3, sets, spec, /*budget=*/16 << 10);

    EXPECT_GT(run.reclaimPoints, 0) << "operator pool never reclaimed";
    EXPECT_GT(run.reclaimCalls, 0) << "operator reclaim() body never ran (no reclaimer wired?)";
    ASSERT_FALSE(run.partialBatches.empty());
    auto finalized = finalize(run.partialBatches, 3, spec);
    EXPECT_TRUE(assertEqualResults({expected}, {finalized})) << "results changed under memory-pressure reclaim";

    fprintf(
        stderr,
        "[arbitration] %s: reclaimPoints=%d reclaimCalls=%lld freedBytes=%lld\n",
        label,
        run.reclaimPoints,
        (long long)run.reclaimCalls,
        (long long)run.freedBytes);
    fflush(stderr);
  }
};

TEST_F(MultiGroupingSetArbitrationTest, reclaimFixedWidth) {
  // Fixed-width accumulator.
  assertArbitrationSafe(AggSpec{{"sum(v) as s"}, {{BIGINT()}}, {"sum(s) as s"}, {"s"}}, "sum(bigint)");
}

TEST_F(MultiGroupingSetArbitrationTest, reclaimStructIntermediate) {
  // Row-typed accumulator.
  assertArbitrationSafe(AggSpec{{"avg(w) as m"}, {{DOUBLE()}}, {"avg(m) as m"}, {"m"}}, "avg(double)");
}

TEST_F(MultiGroupingSetArbitrationTest, reclaimVariableWidthExternalMemory) {
  // Variable-width external-memory accumulator.
  assertArbitrationSafe(
      AggSpec{{"array_agg(v) as l"}, {{BIGINT()}}, {"array_agg(l) as l"}, {"array_sort(l) as l"}}, "array_agg(bigint)");
}

} // namespace facebook::velox::exec
