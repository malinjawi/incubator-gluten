/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Tests for the fused grouping-set aggregation operator (HYBRID_DESIGN.md §7.4).
//
// The load-bearing shape of every test here is DIFFERENTIAL: the same child
// subplan feeds (a) the fused operator and (b) the plan Gluten produces today,
// Expand + a kIntermediate AggregationNode, and the two results are compared as
// multisets after an identical final aggregation. That single comparison
// validates the derivation forest, the bypass lane, gid placement, the null
// masks and state merging in one shot.
//
// ===========================================================================
// Aggregate-function flavour -- a deliberate choice, not an accident
// ===========================================================================
// OperatorTestBase::SetUpTestCase registers PRESTO functions, not Spark ones.
// Rather than add a second registration and diverge from every other file in
// velox/exec/tests, these tests use the Presto functions, which cover the three
// accumulator shapes that matter exactly:
//
//   sum(BIGINT)   -> intermediate BIGINT              : fixed-width
//   avg(DOUBLE)   -> intermediate ROW(DOUBLE, BIGINT) : STRUCT intermediate
//   array_agg(T)  -> intermediate ARRAY(T)            : variable-width, AND the
//                    one that reports accumulatorUsesExternalMemory() == true
//
// The Spark equivalents have the same state-merge algebra, so the
// operator-level properties under test carry over. What does NOT carry over is
// Spark's decimal sum overflow behaviour; that belongs in a Gluten e2e test.

#include <chrono>
#include <folly/Random.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include "operators/rollup/GroupingSetAggregationNode.h"
#include "operators/rollup/GroupingSetLattice.h"
#include "operators/rollup/MultiGroupingSetAggregation.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec {

namespace {

using Set = GroupingSetAggregationNode::Set;

// =============================================================================
// KNOWN-BAD BASELINE -- five cases are BLOCKED by a defect OUTSIDE this operator
// =============================================================================
// forcedFlush, noMoreInputDuringDrain, forcedAbandon, forcedAbandonWithFlush and
// aggregateVariableWidthUnderFlush all fail, and in every one of them it is the
// REFERENCE plan -- Expand + kIntermediate AggregationNode, which contains no
// fused operator at all -- that dies. Attributed by running each side of the
// differential separately via the PLAN_ONLY hook in
// assertFusedMatchesReferenceForSets (re-measured 2026-07-21 against this
// merged operator):
//
//   test                              reference        fused
//   forcedFlush                       SIGSEGV          4411 rows, clean
//   noMoreInputDuringDrain            SIGSEGV          4812 rows, clean
//   forcedAbandon                     SIGSEGV          9951 rows, clean
//   forcedAbandonWithFlush            SIGSEGV          9951 rows, clean
//   aggregateVariableWidthUnderFlush  VELOX_CHECK      3151 rows, clean
//
// The last one throws rather than crashing, and the message names the culprit
// precisely -- VectorHasher.h:188 "Type mismatch: BIGINT vs. ARRAY<BIGINT>",
// i.e. the reference plan's own hasher is decoding an accumulator column as a
// key. The four SIGSEGVs were previously traced to
//   HashAggregation::addInput -> GroupingSet::addInputForActiveRows ->
//   SimpleNumericAggregate::updateGroups (null group pointer).
//
// So the differential harness cannot be used to test the flush and abandon
// paths until that is understood; it is a Velox-side issue in the Expand +
// kIntermediate shape, or an unsupported configuration. The fused side of all
// five runs clean and produces plausible row counts. DO NOT weaken these tests
// to make them pass -- the assertion they carry is still the right one, it is
// the reference half that has to be fixed.
// =============================================================================

class MultiGroupingSetAggregationTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    // Same idiom as the only in-tree custom-operator test, CustomJoinTest.cpp:
    // registerOperator() appends to a process-wide static vector, so without the
    // TearDown below every test in the binary would add another copy.
    registerMultiGroupingSetAggregation();
  }

  void TearDown() override {
    Operator::unregisterAllOperators();
    OperatorTestBase::TearDown();
  }

  // -------------------------------------------------------------------------
  // Grouping-set shapes
  // -------------------------------------------------------------------------

  /// ROLLUP(k1..kn): a CHAIN in the lattice. The derivation plan degenerates to
  /// a pure cascade, which is the prefix-rollup prototype's behaviour recovered
  /// as a special case rather than hardcoded.
  static std::vector<Set> rollupSets(int32_t numKeys) {
    std::vector<Set> sets;
    for (auto i = 0; i <= numKeys; ++i) {
      Set set;
      set.gid = i;
      set.keyIsActive.resize(numKeys);
      for (auto j = 0; j < numKeys; ++j) {
        set.keyIsActive[j] = (j < numKeys - i);
      }
      sets.push_back(std::move(set));
    }
    return sets;
  }

  /// CUBE(k1..kn): the full power set, 2^n sets. A genuine LATTICE -- most sets
  /// have several superset parents and the plan must pick one. Not expressible
  /// at all in the prefix-rollup representation.
  static std::vector<Set> cubeSets(int32_t numKeys) {
    std::vector<Set> sets;
    const int32_t total = 1 << numKeys;
    // Descending so that set 0 is the all-keys-active set, which is the grain
    // the child partial aggregation delivers (and hence the bypass candidate).
    for (int32_t m = total - 1; m >= 0; --m) {
      Set set;
      set.gid = m;
      set.keyIsActive.resize(numKeys);
      for (auto j = 0; j < numKeys; ++j) {
        set.keyIsActive[j] = ((m >> j) & 1) != 0;
      }
      sets.push_back(std::move(set));
    }
    return sets;
  }

  /// Arbitrary GROUPING SETS with NO containment between them: an ANTICHAIN.
  /// Every set is a lattice root, so the derivation plan is exactly the flat
  /// fan-out -- the degeneracy claim in HYBRID_DESIGN.md §2.1, tested.
  static std::vector<Set> antichainSets() {
    std::vector<Set> sets;
    // {k1, k2}
    Set a;
    a.gid = 11;
    a.keyIsActive = {true, true, false};
    sets.push_back(std::move(a));
    // {k3}
    Set b;
    b.gid = 22;
    b.keyIsActive = {false, false, true};
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
      std::vector<int64_t> k1(batchSize), k2(batchSize), k3(batchSize),
          v(batchSize);
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
    return ROW(
        {"k1", "k2", "k3", "v", "w"},
        {BIGINT(), BIGINT(), BIGINT(), BIGINT(), DOUBLE()});
  }

  // -------------------------------------------------------------------------
  // Plan construction
  // -------------------------------------------------------------------------
  //
  // Both plans share the SAME child subplan:
  //
  //     Values(raw)
  //       -> partialAggregation({k1..kn}, {sum(v) as s, avg(w) as m,
  //                                        array_agg(v) as l})
  //
  // whose output type is ROW(k1..kn, s, m, l) -- keys first, then one
  // intermediate-typed column per aggregate. That is exactly the precondition
  // MultiGroupingSetAggregation::initialize() VELOX_CHECKs. Using a real
  // partial aggregation as the source (rather than hand-building state vectors)
  // is what makes the accumulator columns genuinely well-formed intermediate
  // states, including array_agg's external memory.

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
  core::PlanNodePtr makeChild(
      PlanBuilder& builder,
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const AggSpec& spec) {
    return builder.values(input)
        .partialAggregation(keyNames(numKeys), spec.partial)
        .planNode();
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
  static std::vector<core::AggregationNode::Aggregate>
  makeIntermediateAggregates(const core::AggregationNode* childAgg) {
    const auto numGroupingKeys = childAgg->groupingKeys().size();
    std::vector<core::AggregationNode::Aggregate> aggregates;
    aggregates.reserve(childAgg->aggregates().size());

    for (size_t i = 0; i < childAgg->aggregates().size(); ++i) {
      const auto& partial = childAgg->aggregates()[i];
      core::AggregationNode::Aggregate agg;
      for (const auto& rawInput : partial.call->inputs()) {
        agg.rawInputTypes.push_back(rawInput->type());
      }
      const auto intermediateType =
          resolveIntermediateType(partial.call->name(), agg.rawInputTypes);

      const auto& childOutput = childAgg->outputType();
      const auto channel = numGroupingKeys + i;
      std::vector<core::TypedExprPtr> inputs = {
          std::make_shared<core::FieldAccessTypedExpr>(
              childOutput->childAt(channel), childOutput->nameOf(channel))};

      agg.call = std::make_shared<core::CallTypedExpr>(
          intermediateType, std::move(inputs), partial.call->name());
      aggregates.push_back(std::move(agg));
    }
    return aggregates;
  }

  static std::vector<core::FieldAccessTypedExprPtr> makeGroupingKeys(
      const core::PlanNodePtr& child,
      int32_t numKeys) {
    std::vector<core::FieldAccessTypedExprPtr> keys;
    const auto& type = child->outputType();
    for (auto i = 0; i < numKeys; ++i) {
      const auto name = fmt::format("k{}", i + 1);
      keys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          type->findChild(name), name));
    }
    return keys;
  }

  /// ROW(k1..kn, acc1..accm, gid BIGINT).
  static RowTypePtr makeExpectedOutputType(
      const core::PlanNodePtr& child,
      int32_t numKeys,
      const std::vector<std::string>& aggNames) {
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
  core::PlanNodePtr makeFusedPlan(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      std::optional<int32_t> childGroupedSet,
      const AggSpec& spec = defaultAggSpec()) {
    PlanBuilder builder(pool());
    auto child = makeChild(builder, input, numKeys, spec);
    const auto* childAgg =
        dynamic_cast<const core::AggregationNode*>(child.get());
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
              childGroupedSet,
              std::move(source));
        })
        .finalAggregation(
            appendGid(keyNames(numKeys)), spec.final, spec.rawTypes)
        .project(finalProjection(numKeys, spec))
        .planNode();
  }

  /// (b) The reference plan: child -> ExpandNode -> AggregationNode
  ///     (kIntermediate) -> final aggregation. This is what Gluten produces
  ///     today.
  ///
  /// PlanBuilder CANNOT express the kIntermediate half: the public
  /// intermediateAggregation(keys, aggs) overload forwards without
  /// rawInputTypes and trips a VELOX_CHECK_EQ, the no-arg overload requires the
  /// preceding node to be a raw-input partial aggregation (ours is an Expand),
  /// and the overload that would work is private. So the node is constructed
  /// directly via addNode().
  core::PlanNodePtr makeReferencePlan(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      const AggSpec& spec = defaultAggSpec()) {
    PlanBuilder builder(pool());
    auto child = makeChild(builder, input, numKeys, spec);
    const auto* childAgg =
        dynamic_cast<const core::AggregationNode*>(child.get());
    VELOX_CHECK_NOT_NULL(childAgg);

    const auto aggNames = childAgg->aggregateNames();

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
        if (set.keyIsActive[j]) {
          projection.push_back(s == 0 ? fmt::format("{0} as {0}", name) : name);
        } else {
          projection.push_back(
              s == 0 ? fmt::format("null::bigint as {}", name)
                     : std::string("null::bigint"));
        }
      }
      for (const auto& aggName : aggNames) {
        projection.push_back(aggName);
      }
      // Bare integer literal: Velox parses it to a BIGINT constant. "N::bigint"
      // would parse to a CAST, which ExpandNode rejects outright.
      projection.push_back(
          s == 0 ? fmt::format("{} as gid", set.gid)
                 : fmt::format("{}", set.gid));
      projections.push_back(std::move(projection));
    }

    builder.expand(projections);

    return builder
        .addNode([&](std::string id, core::PlanNodePtr source) {
          std::vector<core::FieldAccessTypedExprPtr> groupingKeys =
              makeGroupingKeys(source, numKeys);
          const auto& sourceType = source->outputType();
          groupingKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
              sourceType->findChild("gid"), "gid"));

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
        .finalAggregation(
            appendGid(keyNames(numKeys)), spec.final, spec.rawTypes)
        .project(finalProjection(numKeys, spec))
        .planNode();
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
  static std::vector<std::string> finalProjection(
      int32_t numKeys,
      const AggSpec& spec) {
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
      auto r = AssertQueryBuilder(makeReferencePlan(input, numKeys, sets, spec))
                   .configs(configs).copyResults(pool());
      fprintf(stderr, "[isolate] reference produced %d rows\n", (int)r->size());
      return;
    }
    if (only && std::string(only) == "fused") {
      auto r = AssertQueryBuilder(
                   makeFusedPlan(input, numKeys, sets, childGroupedSet, spec))
                   .configs(configs).copyResults(pool());
      fprintf(stderr, "[isolate] fused produced %d rows\n", (int)r->size());
      return;
    }
    auto fused =
        AssertQueryBuilder(
            makeFusedPlan(input, numKeys, sets, childGroupedSet, spec))
            .configs(configs)
            .copyResults(pool());
    auto reference =
        AssertQueryBuilder(makeReferencePlan(input, numKeys, sets, spec))
            .configs(configs)
            .copyResults(pool());
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
    assertFusedMatchesReference(
        input, numKeys, configs, AggSpec{partial, rawTypes, final, outputs});
  }

  // ------------------------------------------------------------------------
  // Fused-vs-fused budget sweep
  // ------------------------------------------------------------------------
  // The differential harness (fused vs Expand+kIntermediate) cannot be run at a
  // lowered budget: the REFERENCE plan segfaults there for every accumulator
  // shape (the known-bad baseline at the top of this file). To exercise the
  // FLUSH path for a given AggSpec without that poison, run the SAME fused plan
  // at a budget no drain can reach and again at each swept budget, and require
  // multiset equality. Aggregation is exact, so flush timing may not change the
  // answer. This is the flushCorrectnessSmallBudget idiom, generalised over the
  // AggSpec so the multi-field-buffer shapes (decimal sum, avg) get flush
  // coverage too.
  void assertFusedSelfConsistentAcrossBudgets(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      const AggSpec& spec,
      const std::vector<int64_t>& budgets) {
    auto runAt = [&](int64_t budget) {
      const std::unordered_map<std::string, std::string> configs = {
          {core::QueryConfig::kMaxPartialAggregationMemory,
           fmt::format("{}", budget)},
          {core::QueryConfig::kMaxExtendedPartialAggregationMemory,
           fmt::format("{}", budget)},
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
      ASSERT_EQ(actual->size(), expected->size())
          << "fused plan changed row count under flush at budget " << budget;
      ASSERT_TRUE(assertEqualResults({expected}, {actual}))
          << "fused plan disagreed with itself under flush at budget " << budget;
    }
  }
};

// ===========================================================================
// The lattice, in isolation
// ===========================================================================
//
// buildDerivationPlan is the only genuinely new ALGORITHM in the merge, and it
// is a pure function of (masks, hint) with no Velox runtime dependencies. Test
// it directly: the three degenerate shapes it has to produce are the whole
// argument for having one mechanism instead of two.

TEST_F(MultiGroupingSetAggregationTest, latticeChainIsACascade) {
  // ROLLUP(k1,k2,k3): masks 111, 110, 100, 000. Every set has exactly one
  // strict superset available, so the plan MUST be a chain -- i.e. exactly the
  // prefix-rollup prototype's cascade, recovered rather than hardcoded.
  const std::vector<GroupingSetMask> masks = {0b111, 0b110, 0b100, 0b000};
  auto plan = buildDerivationPlan(masks);

  EXPECT_EQ(plan.parent[0], kRawInputParent);
  EXPECT_EQ(plan.parent[1], 0);
  EXPECT_EQ(plan.parent[2], 1);
  EXPECT_EQ(plan.parent[3], 2);
  // Raw input is scanned ONCE, not four times.
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
  // GROUPING SETS ((a,b),(c,d)): no containment, so every set is a root and the
  // plan is EXACTLY the flat fan-out. This is the degeneracy claim that makes
  // "always build the derivation plan" free rather than a tradeoff -- there is
  // no workload where you would want to disable it.
  const std::vector<GroupingSetMask> masks = {0b1100, 0b0011};
  auto plan = buildDerivationPlan(masks);
  EXPECT_EQ(plan.parent[0], kRawInputParent);
  EXPECT_EQ(plan.parent[1], kRawInputParent);
  EXPECT_TRUE(plan.children[0].empty());
  EXPECT_TRUE(plan.children[1].empty());
}

TEST_F(MultiGroupingSetAggregationTest, latticeCubePicksSmallestParent) {
  // CUBE(a,b,c), masks descending: index 0 == 111 ... index 7 == 000.
  const std::vector<GroupingSetMask> masks = {
      0b111, 0b110, 0b101, 0b100, 0b011, 0b010, 0b001, 0b000};
  auto plan = buildDerivationPlan(masks);

  // Only the top of the lattice reads raw input; the other seven sets are
  // derived. Vanilla Expand scans the input eight times.
  EXPECT_EQ(std::count(plan.parent.begin(), plan.parent.end(), kRawInputParent), 1);
  EXPECT_EQ(plan.parent[0], kRawInputParent);

  // Every chosen parent is a strict superset -- the precondition the derivation
  // argument (§6.2) needs for f: G(P) -> G(S) to be a total function.
  for (size_t i = 0; i < masks.size(); ++i) {
    const auto p = plan.parent[i];
    if (p == kRawInputParent) {
      continue;
    }
    EXPECT_EQ(masks[i] & ~masks[p], 0u) << "set " << i;
    EXPECT_NE(masks[i], masks[p]) << "set " << i;
    // SMALLEST parent: no other already-placed strict superset has fewer
    // active keys.
    EXPECT_EQ(__builtin_popcountll(masks[p]), __builtin_popcountll(masks[i]) + 1)
        << "set " << i << " should derive from a minimal superset";
  }
}

TEST_F(MultiGroupingSetAggregationTest, latticeBypassMarking) {
  const std::vector<GroupingSetMask> masks = {0b111, 0b110, 0b100, 0b000};
  auto plan = buildDerivationPlan(masks, /*childGroupedSet=*/0);
  EXPECT_TRUE(plan.bypass[0]);
  EXPECT_FALSE(plan.bypass[1]);
  // A bypassed set is still a NODE in the forest: its children are fed from the
  // stream that passes through it, so they see R rows rather than G(set) rows.
  EXPECT_EQ(plan.children[0].size(), 1u);
}

TEST_F(MultiGroupingSetAggregationTest, latticeRejectsBypassOfANonRoot) {
  // A bypassed set gets no hash table: its rows are forwarded verbatim from the
  // operator's raw input. Asking for that on a DERIVED set would forward its
  // parent's already-aggregated rows unmerged -- valid partial states, but a
  // silent row blow-up downstream. buildDerivationPlan must refuse rather than
  // honour it.
  const std::vector<GroupingSetMask> masks = {0b111, 0b110, 0b100, 0b000};
  VELOX_ASSERT_THROW(
      buildDerivationPlan(masks, /*childGroupedSet=*/1),
      "The bypassed grouping set must be a lattice root");
}

TEST_F(MultiGroupingSetAggregationTest, latticeRejectsDuplicateMasks) {
  // Enforced at this function's own boundary, not only in the node ctor:
  // GroupingSetLattice.h is an installed public header and callers need not go
  // through GroupingSetAggregationNode. Two sets at the same grain would each
  // become a root and rescan the raw input for identical work.
  const std::vector<GroupingSetMask> masks = {0b111, 0b110, 0b110};
  VELOX_ASSERT_THROW(buildDerivationPlan(masks), "Duplicate grouping-set mask");
}

TEST_F(MultiGroupingSetAggregationTest, latticeNonAdjacentParent) {
  // GROUPING SETS ((a,b,c),(a)): the minimal available superset is TWO bits
  // away. latticeCubePicksSmallestParent's popcount+1 assertion structurally
  // cannot reach this branch, because a CUBE always offers a popcount+1
  // superset.
  const std::vector<GroupingSetMask> masks = {0b111, 0b001};
  auto plan = buildDerivationPlan(masks);
  EXPECT_EQ(plan.parent[0], kRawInputParent);
  EXPECT_EQ(plan.parent[1], 0);
  EXPECT_EQ(
      std::count(plan.parent.begin(), plan.parent.end(), kRawInputParent), 1);
}

TEST_F(MultiGroupingSetAggregationTest, latticeWideMasks) {
  // GroupingSetMask is uint64_t and the node caps keys at 64. Exercise the top
  // bit: a chain over bits 63, 62, 61 must still be a cascade, with no shift
  // UB and no sign-extension surprise in popcount.
  const GroupingSetMask top = GroupingSetMask{1} << 63;
  const GroupingSetMask second = GroupingSetMask{1} << 62;
  const GroupingSetMask third = GroupingSetMask{1} << 61;
  const std::vector<GroupingSetMask> masks = {
      top | second | third, top | second, top, 0};
  auto plan = buildDerivationPlan(masks);
  EXPECT_EQ(plan.parent[0], kRawInputParent);
  EXPECT_EQ(plan.parent[1], 0);
  EXPECT_EQ(plan.parent[2], 1);
  EXPECT_EQ(plan.parent[3], 2);
}

TEST_F(MultiGroupingSetAggregationTest, latticeRejectsEmptyMasks) {
  VELOX_ASSERT_THROW(
      buildDerivationPlan({}),
      "A grouping-set aggregation needs at least one set");
}

// ===========================================================================
// Node validation
// ===========================================================================

TEST_F(MultiGroupingSetAggregationTest, duplicateMaskRejected) {
  // Two sets at the same grain with different gids would each be a lattice root
  // and each scan the raw input for identical work. A planner bug; reject at
  // plan build time rather than paper over it.
  auto input = makeInput(1, 10, 2, 2, 2);
  PlanBuilder builder(pool());
  auto child = makeChild(builder, input, 3, defaultAggSpec());
  const auto* childAgg =
      dynamic_cast<const core::AggregationNode*>(child.get());
  ASSERT_NE(childAgg, nullptr);

  std::vector<Set> sets = {
      Set{{true, true, false}, 1}, Set{{true, true, false}, 2}};

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
}

TEST_F(MultiGroupingSetAggregationTest, outputTypeShape) {
  // Guards the output-type contract independently of execution, so that a
  // regression fails here with a readable message rather than as a type
  // mismatch deep inside a GroupingSet.
  auto input = makeInput(1, 100, 3, 5, 7);
  PlanBuilder builder(pool());
  auto child = makeChild(builder, input, 3, defaultAggSpec());
  const auto* childAgg =
      dynamic_cast<const core::AggregationNode*>(child.get());
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

TEST_F(MultiGroupingSetAggregationTest, planNodeSerdeRoundTrip) {
  // B4: the node round-trips through folly::dynamic. serialize()/create() are
  // registered from registerMultiGroupingSetAggregation() (called in SetUp),
  // under the name PlanNode::serialize() emits -- "GroupingSetAggregationNode".
  // This is the PlanNodeSerdeTest contract: a deserialized copy must be
  // toString-identical to the original. The round-tripped subtree is
  //   GroupingSetAggregationNode -> partial AggregationNode -> ValuesNode,
  // so this also exercises that create() reconstructs the node through the
  // public constructor -- every ctor VELOX_CHECK runs on deserialize, and
  // outputType_ is re-derived (the aggregate intermediate types come back
  // through resolveIntermediateType). Our node's create() is registered in
  // SetUp() via registerMultiGroupingSetAggregation().
  //
  // We register ONLY the node types the round-tripped subtree actually contains
  // (ValuesNode, AggregationNode) rather than calling core::PlanNode::registerSerDe(),
  // which bulk-inserts ~40 core-node creators at once. On this hand-linked test
  // binary that bulk insert trips a folly F14 hardened rehash assertion
  // (F14Table.h:2380) -- a build artifact of mixing folly F14 assertion flags
  // between libvelox.a and this translation unit, NOT a defect in the node's
  // serde: the crash is inside PlanNode::registerSerDe() before any create()
  // runs. Registering just the two source-node creators keeps the registry map
  // small and exercises the same deserialize() dispatch path end to end. A
  // proper CMake build with uniform flags can use the full registerSerDe().
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
  const auto* childAgg =
      dynamic_cast<const core::AggregationNode*>(child.get());
  ASSERT_NE(childAgg, nullptr);

  // Cover childGroupedSet present and absent, and a genuine CUBE lattice so that
  // keyIsActive masks with interior holes -- not just rollup prefixes -- survive
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
    const auto copy =
        ISerializable::deserialize<core::PlanNode>(serialized, pool());
    ASSERT_EQ(node->toString(true, true), copy->toString(true, true));

    // toString equality is necessary but not sufficient -- assert the fields
    // that identify a grouping set (gid and mask) and the derived output type
    // came back intact on the real type.
    auto gsCopy =
        std::dynamic_pointer_cast<const GroupingSetAggregationNode>(copy);
    ASSERT_NE(gsCopy, nullptr);
    ASSERT_EQ(gsCopy->groupingSets().size(), sets.size());
    for (size_t i = 0; i < sets.size(); ++i) {
      EXPECT_EQ(gsCopy->groupingSets()[i].gid, sets[i].gid);
      EXPECT_EQ(
          gsCopy->groupingSets()[i].keyIsActive, sets[i].keyIsActive);
    }
    EXPECT_EQ(gsCopy->childGroupedSet(), childGroupedSet);
    EXPECT_EQ(*gsCopy->outputType(), *node->outputType());
  }
}

// ===========================================================================
// Differential correctness
// ===========================================================================

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
  const std::vector<std::array<int32_t, 3>> shapes = {
      {2, 4, 8}, {3, 100, 5000}, {1, 1, 2}, {50, 50, 50}};
  for (uint32_t seed = 0; seed < shapes.size(); ++seed) {
    SCOPED_TRACE(fmt::format("seed={}", seed));
    auto input = makeInput(
        4, 500, shapes[seed][0], shapes[seed][1], shapes[seed][2], seed + 1);
    assertFusedMatchesReference(input, 3);
  }
}

TEST_F(MultiGroupingSetAggregationTest, differentialNoBypass) {
  // The same rollup with the bypass hint withheld, so the finest set gets a
  // real hash table. Both configurations must produce the same answer: the
  // bypass is a performance decision (HYBRID_DESIGN.md §3.1, P3 is NOT a
  // correctness condition), not a semantic one.
  auto input = makeInput(10, 1'000, 5, 23, 97);
  assertFusedMatchesReferenceForSets(
      input, 3, rollupSets(3), /*childGroupedSet=*/std::nullopt);
}

TEST_F(MultiGroupingSetAggregationTest, differentialCube) {
  // CUBE(k1,k2,k3): a real lattice, eight sets, most with several candidate
  // parents. Not expressible at all in the prefix-rollup representation this
  // merge replaced -- the planner would have had to fall back to the unfused
  // plan.
  auto input = makeInput(8, 1'000, 4, 12, 40);
  assertFusedMatchesReferenceForSets(
      input, 3, cubeSets(3), /*childGroupedSet=*/0);
}

TEST_F(MultiGroupingSetAggregationTest, differentialAntichainGroupingSets) {
  // GROUPING SETS ((k1,k2),(k3)): no containment, so the derivation plan is
  // all-roots and the operator is literally the flat fan-out. The other reason
  // the mask representation is load-bearing: Spark emits a single Expand for
  // this shape too.
  auto input = makeInput(8, 1'000, 4, 12, 40);
  assertFusedMatchesReferenceForSets(
      input, 3, antichainSets(), /*childGroupedSet=*/std::nullopt);
}

TEST_F(MultiGroupingSetAggregationTest, differentialSingleKey) {
  // n == 1 was a hard REJECT in the prefix-rollup node (it required >= 2 keys,
  // so the planner had to fall back). The mask representation has no arity
  // restriction and this is now just another chain: {k1} -> {}.
  auto input = makeInput(5, 1'000, 6, 1, 1);
  assertFusedMatchesReferenceForSets(
      input, 1, rollupSets(1), /*childGroupedSet=*/0);
}

// ===========================================================================
// Forced flush
// ===========================================================================
//
// Config key names verified against velox/core/QueryConfig.h. BOTH must be
// squeezed: maybeGrowBudget mirrors
// HashAggregation::maybeIncreasePartialAggregationMemoryUsage, which doubles
// the per-node limit up to the EXTENDED ceiling. Setting only the first would
// let every node grow its way out of flushing after one or two flushes and the
// test would silently stop testing anything.
//
// BLOCKED: see the baseline note at the top of this file -- the REFERENCE plan
// segfaults at every non-default budget.

TEST_F(MultiGroupingSetAggregationTest, forcedFlush) {
  auto input = makeInput(20, 1'000, 7, 60, 700);

  const std::unordered_map<std::string, std::string> tinyBudget = {
      {core::QueryConfig::kMaxPartialAggregationMemory, "4096"},
      {core::QueryConfig::kMaxExtendedPartialAggregationMemory, "8192"},
  };

  assertFusedMatchesReference(input, /*numKeys=*/3, tinyBudget);
}

TEST_F(MultiGroupingSetAggregationTest, noMoreInputDuringDrain) {
  // noMoreInput() arriving while a pressure drain is in flight. In the
  // prototype this was a data-loss hazard guarded by pendingFinalCascade_,
  // because there was ONE shared drain cursor. Here every node owns its
  // iterator, so noMoreInput() just sets the sweep cursor and the in-flight
  // drain finishes off drainStack_ first -- the hazard is not expressible.
  //
  // Provoked structurally rather than by timing injection: a tiny memory budget
  // guarantees a drain is in flight, and small output batches keep each drain
  // spread over many getOutput() calls, widening the window.
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

// ===========================================================================
// Forced abandon
// ===========================================================================
//
// The predicate (HashAggregation.cpp:183-187):
//   numInputRows > abandonPartialAggregationMinRows &&
//   100 * numOutput / numInputRows >= abandonPartialAggregationMinPct
// so minRows=0 / minPct=0 makes it true as soon as any row has arrived. Note
// the STRICT inequality on rows: a node abandons after its first non-empty
// batch, not before it.
//
// Expected degeneration: every non-global node becomes a pure projection that
// forwards states, and all the real merging lands in the grand total. The
// grand-total set is explicitly EXEMPT from the valve. Because the reference
// plan is unaffected by these knobs, the differential comparison asserts that
// abandoning is SEMANTICALLY TRANSPARENT, which is the whole claim.

TEST_F(MultiGroupingSetAggregationTest, forcedAbandon) {
  auto input = makeInput(10, 1'000, 5, 40, 900);
  assertFusedMatchesReference(
      input,
      /*numKeys=*/3,
      {
          {core::QueryConfig::kAbandonPartialAggregationMinRows, "0"},
          {core::QueryConfig::kAbandonPartialAggregationMinPct, "0"},
      });
}

TEST_F(MultiGroupingSetAggregationTest, forcedAbandonWithFlush) {
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
  // and the bypass lane emits one row per input batch. Also the case where the
  // abandon valve must NOT fire.
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
       makeFlatVector<double>(
           size, [](auto row) { return static_cast<double>(row); })});
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
  //
  // OPEN: a genuine zero-row addInput() needs a unit-level harness (construct
  // the operator via a DriverCtx and call addInput() with a size-0 RowVector),
  // asserting getOutput() returns nullptr rather than an empty vector. Not
  // expressible through AssertQueryBuilder.
  auto empty = makeRowVector(rawInputType(), 0);
  assertFusedMatchesReference({empty}, /*numKeys=*/3);
}

TEST_F(MultiGroupingSetAggregationTest, boundaryZeroInputRows) {
  // Spark emits ZERO rows for a grouping-sets/ROLLUP query over empty input --
  // it never synthesises a grand-total row, because spark_grouping_id is itself
  // a grouping key so the aggregation is never keyless. Verified against
  // gluten-ut/spark40/.../postgreSQL/groupingsets.sql.out:190-212.
  //
  // The fused plan's grand-total node is a global GroupingSet, and
  // GroupingSet::getGlobalAggregationOutput WOULD emit its single row on a
  // fresh iterator whether or not input arrived. The operator therefore guards
  // the final sweep on receivedInput_. This matters PER TASK, not just per
  // query: the operator runs per driver, so an unguarded sweep would push one
  // spurious identity row into the shuffle for every empty partition of an
  // otherwise non-empty table.
  auto empty = makeRowVector(rawInputType(), 0);
  assertFusedMatchesReference({empty}, /*numKeys=*/3);

  // The differential assertion is necessary but NOT sufficient here: it only
  // proves the two plans agree. The point of this test is the ABSOLUTE claim
  // that the agreed answer is zero rows. Pin the cardinality directly.
  auto fused = AssertQueryBuilder(
                   makeFusedPlan({empty}, 3, rollupSets(3), 0))
                   .copyResults(pool());
  ASSERT_EQ(fused->size(), 0)
      << "empty input must produce zero rows, per Spark's grouping-sets "
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
  assertFusedMatchesReferenceWithAggregates(
      input, 3, {"sum(v) as s"}, {{BIGINT()}}, {"sum(s) as s"}, {"s"});
}

TEST_F(MultiGroupingSetAggregationTest, aggregateStructIntermediate) {
  // avg(DOUBLE): intermediate is ROW(DOUBLE, BIGINT). A struct intermediate is
  // where a wrong column index shows up as a type error rather than a silent
  // wrong number.
  auto input = makeInput(8, 1'000, 5, 40, 400);
  assertFusedMatchesReferenceWithAggregates(
      input, 3, {"avg(w) as m"}, {{DOUBLE()}}, {"avg(m) as m"}, {"m"});
}

TEST_F(MultiGroupingSetAggregationTest, aggregateVariableWidthExternalMemory) {
  // array_agg(BIGINT): variable width, and the one that reports
  // accumulatorUsesExternalMemory() == true. That flag is what makes the
  // "does a drain net-free memory?" question sharp: draining such a node copies
  // out of the HashStringAllocator BEFORE resetTable frees it, so peak memory
  // during a drain exceeds the table size. This test establishes CORRECTNESS
  // only; the reclaim measurement needs a memory-pressure harness.
  auto input = makeInput(8, 1'000, 5, 40, 400);
  assertFusedMatchesReferenceWithAggregates(
      input,
      3,
      {"array_agg(v) as l"},
      {{BIGINT()}},
      {"array_agg(l) as l"},
      {"array_sort(l) as l"});
}

TEST_F(MultiGroupingSetAggregationTest, aggregateVariableWidthUnderFlush) {
  // The risky combination: variable-width external-memory state on a node that
  // flushes repeatedly. BLOCKED on the reference-plan segfault, see the
  // baseline note.
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

// ===========================================================================
// MULTI-COLUMN PARTIAL BUFFERS (deployment blocker B1)
// ===========================================================================
//
// The accumulator shapes above use PRESTO functions. Presto's sum(DECIMAL)
// carries a single VARBINARY intermediate and Presto avg(DOUBLE) a
// ROW(DOUBLE,BIGINT) struct, so avg already proves the operator merges a
// two-FIELD intermediate correctly. What those do NOT reproduce is the exact
// Spark aggregate-buffer layout that q67 and every real Gluten query use:
//
//   spark sum(DECIMAL(p,s)) -> intermediate ROW(DECIMAL(min(38,p+10),s), boolean)
//                              == (sum, isEmpty)      -- q67's aggregate
//   spark avg(DOUBLE)       -> intermediate ROW(DOUBLE, BIGINT)
//                              == (sum, count)
//   spark avg(DECIMAL(p,s)) -> intermediate ROW(DECIMAL(...), BIGINT)
//
// These are the shapes the B1 report calls "more than one aggBufferAttribute".
// In VELOX every one of them is a SINGLE ROW-typed column -- the multi-column
// flattening is a Spark/Gluten artifact that Gluten's own
// VeloxHashAggregateExecTransformer re-packs into a struct (applyExtractStruct)
// before the data ever reaches this operator. So the operator's contract
// (ROW(k1..kn, acc1..accm), one intermediate column per aggregate) already
// covers them, and the question these tests answer is whether the merge, the
// derive/project reshaping and the flush path stay correct when that one column
// is a genuine Spark (sum,isEmpty)/(sum,count) struct.
//
// The functions are registered under a "spark_" prefix so they coexist with the
// Presto functions the rest of the file uses; nothing here overwrites sum/avg.
class MultiGroupingSetSparkBufferTest : public MultiGroupingSetAggregationTest {
 protected:
  void SetUp() override {
    MultiGroupingSetAggregationTest::SetUp();
    functions::aggregate::sparksql::registerAggregateFunctions(
        "spark_", /*withCompanionFunctions=*/true, /*overwrite=*/true);
  }

  // Same as makeInput but adds a DECIMAL(7,2) column "d". Velox's
  // partialAggregation rejects a computed argument (the aggregate input must be
  // a field access, constant or lambda -- AggregateInfo.cpp:81), so the decimal
  // input has to exist as a real column rather than an inline
  // cast(v as decimal(7,2)). Values are kept well inside decimal(7,2)'s
  // +/-99999.99 range so the raw column never overflows.
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
      std::vector<int64_t> k1(batchSize), k2(batchSize), k3(batchSize),
          v(batchSize), d(batchSize);
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
};

TEST_F(MultiGroupingSetSparkBufferTest, decimalSum) {
  // q67's aggregate exactly: sum over decimal(7,2), whose Spark intermediate is
  // the two-field (sum, isEmpty) struct. This is the case the Gluten rule
  // EXCLUDED (singleColumnBuffers) and the number-one deployment blocker.
  // Differential vs Expand+kIntermediate at the default budget, where the
  // reference plan is healthy.
  auto input = makeDecimalInput(10, 1'000, 5, 23, 97);
  assertFusedMatchesReferenceWithAggregates(
      input,
      3,
      {"spark_sum(d) as s"},
      {{DECIMAL(7, 2)}},
      {"spark_sum(s) as s"},
      {"s"});
}

TEST_F(MultiGroupingSetSparkBufferTest, avgDouble) {
  // Spark avg(DOUBLE): the (sum, count) buffer. Same field count as Presto avg
  // but the Spark function, so this pins the Spark-side merge algebra rather
  // than assuming it matches Presto's.
  auto input = makeInput(10, 1'000, 5, 23, 97);
  assertFusedMatchesReferenceWithAggregates(
      input,
      3,
      {"spark_avg(w) as m"},
      {{DOUBLE()}},
      {"spark_avg(m) as m"},
      {"m"});
}

TEST_F(MultiGroupingSetSparkBufferTest, avgDecimal) {
  // Spark avg over decimal: intermediate ROW(DECIMAL(...), BIGINT). A different
  // two-field mix (decimal + bigint) than either sum-decimal or avg-double.
  //
  // Uses DECIMAL(12,2) rather than (7,2): Velox's Spark decimal-avg final
  // divide requires the result precision to exceed 11 (DecimalUtil, precision
  // > 11), so avg over decimal(7,2) is rejected by the FUNCTION itself before
  // the operator is reached. That is a property of the aggregate, not of the
  // fused node; a wider input keeps the ROW(DECIMAL,BIGINT) buffer under test
  // without tripping it.
  auto input = makeDecimalInput(
      10, 1'000, 5, 23, 97, /*seed=*/1234, /*decimalType=*/DECIMAL(12, 2));
  assertFusedMatchesReferenceWithAggregates(
      input,
      3,
      {"spark_avg(d) as m"},
      {{DECIMAL(12, 2)}},
      {"spark_avg(m) as m"},
      {"m"});
}

TEST_F(MultiGroupingSetSparkBufferTest, mixedMultiAndSingleColumn) {
  // The real q67-shaped mix: a multi-field decimal sum, a multi-field avg, and a
  // single-column bigint sum in one node. A wrong per-aggregate channel offset
  // (the most likely multi-column wiring bug) shows up here as a type error or a
  // cross-contaminated column that a single-aggregate test would miss.
  auto input = makeDecimalInput(10, 1'000, 5, 23, 97);
  assertFusedMatchesReferenceWithAggregates(
      input,
      3,
      {"spark_sum(d) as s",
       "spark_avg(w) as m",
       "spark_sum(v) as t"},
      {{DECIMAL(7, 2)}, {DOUBLE()}, {BIGINT()}},
      {"spark_sum(s) as s", "spark_avg(m) as m", "spark_sum(t) as t"},
      {"s", "m", "t"});
}

TEST_F(MultiGroupingSetSparkBufferTest, mixedMultiColumnCube) {
  // Multi-field buffers on a genuine LATTICE (CUBE, 8 sets, most with several
  // candidate parents), so the struct intermediate is reshaped by derive()
  // across non-trivial parent->child edges, not just a rollup chain.
  auto input = makeDecimalInput(8, 1'000, 4, 12, 40);
  const auto spec = AggSpec{
      {"spark_sum(d) as s", "spark_avg(w) as m"},
      {{DECIMAL(7, 2)}, {DOUBLE()}},
      {"spark_sum(s) as s", "spark_avg(m) as m"},
      {"s", "m"}};
  assertFusedMatchesReferenceForSets(
      input, 3, cubeSets(3), /*childGroupedSet=*/0, {}, spec);
}

TEST_F(MultiGroupingSetSparkBufferTest, multiColumnBudgetSweep) {
  // Flush correctness for the multi-field buffers. Fused-vs-fused across a
  // budget sweep (the reference plan cannot be lowered -- see the baseline
  // note), so the (sum,isEmpty)/(sum,count) structs are drained out of the hash
  // table, re-installed into children and merged repeatedly. k3 is high
  // cardinality so the finest node overflows a small budget and the drain
  // cycle recurs rather than firing once.
  auto input = makeDecimalInput(
      /*numBatches=*/40,
      /*batchSize=*/2'000,
      /*k1Cardinality=*/8,
      /*k2Cardinality=*/120,
      /*k3Cardinality=*/5'000);
  const auto spec = AggSpec{
      {"spark_sum(d) as s",
       "spark_avg(w) as m",
       "spark_sum(v) as t"},
      {{DECIMAL(7, 2)}, {DOUBLE()}, {BIGINT()}},
      {"spark_sum(s) as s", "spark_avg(m) as m", "spark_sum(t) as t"},
      {"s", "m", "t"}};
  assertFusedSelfConsistentAcrossBudgets(
      input,
      3,
      rollupSets(3),
      spec,
      {4096, 65536, 262144, 1LL << 20, 4LL << 20, 16LL << 20});
}

TEST_F(MultiGroupingSetSparkBufferTest, decimalSumAbsoluteCorrectness) {
  // The differential tests prove the two plans AGREE; this pins the ABSOLUTE
  // answer for the decimal-sum rollup so "q67 decimal sum works in the operator"
  // is a checked claim, not an agreement between two possibly-wrong plans. The
  // grand-total row (gid == numKeys) of a ROLLUP is the sum over every input
  // row, which we compute independently with a plain global aggregation.
  auto input = makeDecimalInput(6, 1'000, 4, 20, 200);

  auto fused = AssertQueryBuilder(makeFusedPlan(
                                      input,
                                      3,
                                      rollupSets(3),
                                      /*childGroupedSet=*/0,
                                      AggSpec{
                                          {"spark_sum(d) as s"},
                                          {{DECIMAL(7, 2)}},
                                          {"spark_sum(s) as s"},
                                          {"s"}}))
                   .copyResults(pool());

  // Independent grand total: sum over all rows, no grouping.
  auto grandTotal = AssertQueryBuilder(
                        PlanBuilder(pool())
                            .values(input)
                            .singleAggregation(
                                {}, {"spark_sum(d) as s"})
                            .planNode())
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
      ASSERT_TRUE(fusedS->equalValueAt(expectedS.get(), i, 0))
          << "grand-total decimal sum mismatch at fused row " << i;
    }
  }
  EXPECT_EQ(grandRows, 1) << "rollup must emit exactly one grand-total row";
}

} // namespace

// =============================================================================
// Q67-PROFILE BENCHMARK
// =============================================================================
// Measures the FUSED operator against the plan Gluten produces today
// (Expand + kIntermediate aggregation) on data with TPC-DS q67's REAL
// cardinality profile, measured from SF10 dsdgen data:
//
//     5,342,291 rows into the rollup / 4,804,937 distinct 8-key groups
//     = 1.11 rows per group -- the finest grain is essentially unique.
//
// That profile is why a JVM-side lazy-expand rewrite cannot help q67: there is
// nothing for a pre-aggregation to collapse. The fused operator does not depend
// on fine-grain reduction -- the lattice collapses the COARSE levels, which is
// the larger term, and the bypass lane removes the finest table entirely.
//
// Scope: the rollup STAGE only, 3 keys / 4 sets rather than q67's 8 keys /
// 9 sets. Fewer sets means LESS amplification to remove, so this UNDERSTATES
// the q67 benefit.
TEST_F(MultiGroupingSetAggregationTest, zzBenchQ67Profile) {
  const int32_t numBatches = 40;
  const vector_size_t batchSize = 25'000; // 1M rows
  auto input = makeInput(numBatches, batchSize, 100, 3'000, 900'000);

  const auto rows = numBatches * batchSize;
  auto groups = AssertQueryBuilder(
                    PlanBuilder(pool())
                        .values(input)
                        .singleAggregation({"k1", "k2", "k3"}, {"count(1)"})
                        .planNode())
                    .copyResults(pool())
                    ->size();
  fprintf(
      stderr,
      "\n[q67-profile] %d rows, %d finest groups, %.2f rows/group\n",
      rows,
      (int)groups,
      (double)rows / groups);

  auto timeIt = [&](const core::PlanNodePtr& plan) {
    auto t0 = std::chrono::steady_clock::now();
    auto r = AssertQueryBuilder(plan).copyResults(pool());
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
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

// ===========================================================================
// Flush correctness -- FUSED vs FUSED, no reference plan
// ===========================================================================
//
// Every existing flush test is DIFFERENTIAL against Expand + kIntermediate, and
// that reference plan dies at any lowered budget (see the baseline note at the
// top of this file). So the flush path has had zero working coverage.
//
// This test removes the reference plan from the loop entirely. The SAME fused
// plan is run twice: once at a budget large enough that no pressure drain can
// fire, and once at a budget small enough to force many. Aggregation is exact,
// so flush timing is not allowed to change the answer -- the two results must
// be identical as multisets. Nothing here can be poisoned by the crashing
// reference plan, and there is no dependency on the q67 parquet.
//
// The budget is swept so the failure THRESHOLD is visible rather than a single
// pass/fail.
//
// ---------------------------------------------------------------------------
// WHAT THIS TEST CURRENTLY CATCHES (diagnosed 2026-07-21, NOT yet fixed)
// ---------------------------------------------------------------------------
// It fails, and the defect is NOT in this operator's flush path. Run with
// FLUSH_TRACE=1 for per-operator row accounting; at budget=4096 it reports
//
//   Values                 in=0      out=80000
//   PartialAggregation     in=80000  out=6000     <-- 74000 rows vanish HERE
//   GroupingSetAggregation in=6000   out=8926
//
// The upstream child HashAggregation swallows 74000 of the 80000 rows it was
// handed. The mechanism is HashAggregation's abandoned-partial-aggregation
// path, which buffers exactly one batch in `input_` (HashAggregation.cpp:194)
// while `needsInput()` (HashAggregation.h:50) tests only `!noMoreInput_ &&
// !partialFull_` and so keeps returning TRUE. Whenever the downstream operator
// declines input -- which this operator legitimately does while a drain is in
// flight or output is parked (MultiGroupingSetAggregation.cpp:460) -- the
// Driver walks back upstream and feeds the child another batch, overwriting the
// undrained one. RowNumber.h:40 documents this exact hazard and guards against
// it with `input_ == nullptr`; HashProbe and MergeJoin guard the same way;
// HashAggregation does not.
//
// So a fix belongs in HashAggregation, not here -- but this test is the right
// assertion either way, and it must go green before the operator ships.

TEST_F(MultiGroupingSetAggregationTest, flushCorrectnessSmallBudget) {
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
        {core::QueryConfig::kMaxPartialAggregationMemory,
         fmt::format("{}", budget)},
        {core::QueryConfig::kMaxExtendedPartialAggregationMemory,
         fmt::format("{}", budget)},
    };
    std::shared_ptr<Task> task;
    auto result = AssertQueryBuilder(
                      makeFusedPlan(input, 3, sets, /*childGroupedSet=*/0))
                      .configs(configs)
                      .copyResults(pool(), task);
    // Per-operator row accounting, off unless FLUSH_TRACE is exported. This is
    // how the truncation is attributed to an operator rather than guessed at.
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

  // Reference = the fused plan itself, at a budget no drain can reach.
  const int64_t kLargeBudget = 1LL << 30;
  auto expected = runAt(kLargeBudget);
  fprintf(
      stderr,
      "[flush-sweep] expected (budget=1GiB) rows=%d\n",
      (int)expected->size());
  fflush(stderr);

  // Measured 2026-07-21: rows collapse to 6966 / 18944 / 34847 against an
  // expected 80355 at the first three budgets, and are correct from 1 MiB up.
  // The row count FALLS, i.e. output is being LOST, which is the same signature
  // as q67 at PARTIAL_MEM=4 MiB (458,520 rows instead of 5,752,455).
  std::vector<int64_t> budgets = {
      4096, 65536, 262144, 1LL << 20, 4LL << 20, 16LL << 20};
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
    const bool equal = (actual->size() == expected->size()) &&
        assertEqualResults({expected}, {actual});
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
    EXPECT_TRUE(equal) << "fused plan disagreed with itself at budget " << budget
                       << " bytes";
  }
}

// ===========================================================================
// ARBITRATION SAFETY (deployment blocker B3)
// ===========================================================================
//
// canReclaim() returns true, so under cluster memory pressure the arbitrator
// WILL call Operator::MemoryReclaimer::reclaim on this operator. That path
// wraps our reclaim() in memory::ScopedReclaimedBytesRecorder and asserts
// VELOX_CHECK_GE(reclaimedBytes, 0) (Operator.cpp:795) -- a NET-ALLOCATING
// reclaim fails the whole QUERY, not just the reclaim. Before this suite that
// path had ZERO coverage; reclaim() had, by the operator's own admission, never
// executed.
//
// These tests exercise the EXACT production reclaim path deterministically:
//   * The query runs under a QueryCtx whose root pool carries a MemoryReclaimer,
//     which is what makes Task wire an Operator::MemoryReclaimer onto the
//     operator's leaf pool (Task::createNodeReclaimer + Operator::
//     maybeSetReclaimer). Without it -- as with a bare cursor -- the operator
//     pool has no reclaimer and reclaim() is never reachable at all.
//   * The plan is truncated at the operator (no blocking final aggregation
//     downstream), so its flushed PARTIAL batches reach the cursor one at a
//     time. At a tiny partial-aggregation budget the operator flushes and
//     retains empty slot arrays repeatedly, so every pull catches it mid-build
//     holding live AND retained-but-empty tables.
//   * After each batch the task is paused and pool->reclaim() is invoked on the
//     operator pool -- byte for byte the call SharedArbitrator makes, including
//     the ScopedReclaimedBytesRecorder + VELOX_CHECK_GE wrapper. A
//     net-allocating reclaim would fail the task here.
// Correctness is checked by finalising the collected partial batches and
// comparing to a large-budget run that never reclaimed. The build is Release
// (-DNDEBUG), so the TestValue mid-driver idiom AggregationTest uses is
// unavailable; pausing the task and calling the operator pool's reclaimer is the
// release-safe equivalent of the identical path.

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
      if (name.size() >= kSuffix.size() &&
          name.compare(
              name.size() - kSuffix.size(), kSuffix.size(), kSuffix) == 0) {
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
  // The fused plan TRUNCATED at the operator -- no final aggregation, so the
  // operator's flushed partial batches reach the cursor directly. Mirrors the
  // addNode() half of makeFusedPlan().
  core::PlanNodePtr makeOperatorOnlyPlan(
      const std::vector<RowVectorPtr>& input,
      int32_t numKeys,
      const std::vector<Set>& sets,
      const AggSpec& spec) {
    PlanBuilder builder(pool());
    auto child = makeChild(builder, input, numKeys, spec);
    const auto* childAgg =
        dynamic_cast<const core::AggregationNode*>(child.get());
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
  RowVectorPtr finalize(
      const std::vector<RowVectorPtr>& partialBatches,
      int32_t numKeys,
      const AggSpec& spec) {
    return AssertQueryBuilder(
               PlanBuilder(pool())
                   .values(partialBatches)
                   .finalAggregation(
                       appendGid(keyNames(numKeys)), spec.final, spec.rawTypes)
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
    // Root pool WITH a reclaimer -- the precondition for the operator pool to
    // get an Operator::MemoryReclaimer at all.
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
        {core::QueryConfig::kMaxPartialAggregationMemory,
         fmt::format("{}", budget)},
        {core::QueryConfig::kMaxExtendedPartialAggregationMemory,
         fmt::format("{}", budget)},
    };
    auto cursor = TaskCursor::create(params);

    // The cursor copies each batch into the task's own pool, which dies with
    // the cursor at the end of this function. Deep-copy into the test's pool so
    // the collected batches outlive the arbitration task.
    auto deepCopy = [&](const RowVectorPtr& src) {
      auto dst = std::static_pointer_cast<RowVector>(
          BaseVector::create(src->type(), src->size(), pool()));
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
      // Pause the task so the driver is off-thread and pauseRequested() holds,
      // exactly the state Operator::MemoryReclaimer::reclaim asserts, then run
      // the CHECK-wrapped reclaim on the operator's own pool. The arbitration
      // context is what SharedArbitrator establishes around a reclaim; without
      // it MemoryReclaimer::run's underMemoryArbitration() check fires.
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
        if (auto it = op.runtimeStats.find("gsagg.reclaim.count");
            it != op.runtimeStats.end()) {
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

    // Reference: full fused plan at a large budget, never reclaimed.
    auto expected =
        AssertQueryBuilder(
            makeFusedPlan(input, 3, sets, /*childGroupedSet=*/0, spec))
            .config(
                core::QueryConfig::kMaxPartialAggregationMemory,
                fmt::format("{}", 1LL << 30))
            .copyResults(pool());

    // 16KB budget: forces repeated flushes on a 3-key rollup, which is what
    // leaves the retained-but-empty tables reclaim()'s Phase 1 frees.
    auto run =
        runWithReclaimEachBatch(input, 3, sets, spec, /*budget=*/16 << 10);

    // (1) reclaim() actually executed under a real pause, many times.
    EXPECT_GT(run.reclaimPoints, 0) << "operator pool never reclaimed";
    EXPECT_GT(run.reclaimCalls, 0)
        << "operator reclaim() body never ran (no reclaimer wired?)";
    // (2) implied by reaching here: no VELOX_CHECK_GE trip, no crash.
    // (3) correctness preserved across the reclaims.
    ASSERT_FALSE(run.partialBatches.empty());
    auto finalized = finalize(run.partialBatches, 3, spec);
    EXPECT_TRUE(assertEqualResults({expected}, {finalized}))
        << "results changed under memory-pressure reclaim";

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
  // sum(BIGINT): fixed-width accumulator. The base case for the checked reclaim
  // path, and the one where Phase 1 most reliably frees retained slot arrays.
  assertArbitrationSafe(
      AggSpec{{"sum(v) as s"}, {{BIGINT()}}, {"sum(s) as s"}, {"s"}},
      "sum(bigint)");
}

TEST_F(MultiGroupingSetArbitrationTest, reclaimStructIntermediate) {
  // avg(DOUBLE): ROW(DOUBLE, BIGINT) intermediate -- a multi-field accumulator
  // extracted as a struct, the q67-shaped buffer layout.
  assertArbitrationSafe(
      AggSpec{{"avg(w) as m"}, {{DOUBLE()}}, {"avg(m) as m"}, {"m"}},
      "avg(double)");
}

TEST_F(MultiGroupingSetArbitrationTest, reclaimVariableWidthExternalMemory) {
  // array_agg(BIGINT): the variable-width, accumulatorUsesExternalMemory()==true
  // shape -- previously the scariest reclaim case, because a DRAIN would copy
  // out of the HashStringAllocator before freeing it. reclaim() no longer
  // drains, so even this shape is net non-allocating; this test proves the
  // CHECK holds for it.
  assertArbitrationSafe(
      AggSpec{
          {"array_agg(v) as l"},
          {{BIGINT()}},
          {"array_agg(l) as l"},
          {"array_sort(l) as l"}},
      "array_agg(bigint)");
}

} // namespace facebook::velox::exec
