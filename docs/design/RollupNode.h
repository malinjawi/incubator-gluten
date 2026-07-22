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
#pragma once

#include <algorithm>
#include <unordered_set>

#include <fmt/format.h> // fmt::format, used by the gid uniquifier below.

#include "velox/core/PlanNode.h"
// resolveIntermediateType is declared here (velox/exec/AggregateFunctionRegistry.h:57-60)
// and is public. Pulling an exec/ header into a node header is heavier than the
// usual core/-only diet, but it is the single source of truth that keeps this
// node's output type and RollupOperator::makeNaturalType in agreement; the
// alternative (trusting call->type()) is what a previous revision got wrong.
#include "velox/exec/AggregateFunctionRegistry.h"

namespace gluten {

/// Fused rollup plan node (draft).
///
/// Replaces the three-operator shape
///   PartialAgg(finest) -> Expand(n+1 projections) -> PartialMerge
/// with a single node that consumes finest-grain intermediate accumulators and
/// emits the union of all n+1 grouping levels, still as intermediate
/// accumulators, ready for the post-shuffle final aggregation.
///
/// Contract with the producer (enforced with VELOX_CHECKs in
/// RollupOperator::initialize()):
///   source()->outputType() == ROW(k1..kn, acc1..accm)
/// i.e. the rollup keys come first, in hierarchy order (coarsest key k1 first),
/// followed by one intermediate-typed column per aggregate. The JVM rule is
/// responsible for producing that projection.
///
/// Output type is uniform across levels:
///   ROW(k1..kn (all nullable), acc1..accm, gid BIGINT)
///
/// PlanNode contract (velox/core/PlanNode.h:156): four pure virtuals —
/// outputType(), sources(), name(), addDetails(). Everything else has a base
/// implementation, including accept() (PlanNode.h:184-186, which exists
/// specifically so custom extensions need not override it) and serialize()
/// (PlanNode.h:166).
class RollupNode : public facebook::velox::core::PlanNode {
 public:
  /// One grouping level. Levels are stored finest-first: levels_[0] has all n
  /// keys live (the bypass lane) and levels_[n] is the grand total.
  ///
  /// gid and keyIsNull are supplied verbatim by the JVM side (extracted from
  /// the literal projections of the original Substrait ExpandRel). We
  /// deliberately do not recompute Spark's GROUPING_ID bitmask in C++.
  struct Level {
    /// Number of live grouping keys: the prefix k1..k[numKeys] survives, the
    /// remaining keys are projected to NULL.
    int32_t numKeys;

    /// Spark's grouping-id literal for this level, emitted in the trailing gid
    /// column.
    int64_t gid;

    /// Size n. keyIsNull[j] == true means key j is NULL at this level. For pure
    /// ROLLUP this is exactly (j >= numKeys), but we carry it explicitly so the
    /// same structure generalises to CUBE / GROUPING SETS without a format
    /// change, and so we never disagree with the JVM about null placement.
    std::vector<bool> keyIsNull;
  };

  /// \param groupingKeys the full hierarchy k1..kn as field accesses into
  ///        source->outputType(). Order is significant: k1 is the coarsest.
  /// \param levels finest-first, size n+1, levels[i].numKeys == n - i.
  /// \param aggregateNames output names, size m.
  /// \param aggregates size m. Each Aggregate::call must be expressed over the
  ///        *intermediate* column (as Velox does for a kIntermediate
  ///        AggregationNode): result type is the intermediate type and the
  ///        single argument is the state column.
  ///
  ///        NAME CONTRACT — call->name() must be the BASE, UNSUFFIXED Velox
  ///        aggregate function name ("sum", not "sum_partial" / "sum_merge"),
  ///        and rawInputTypes must be the true RAW argument types. That pair is
  ///        what identifies the function (PlanNode.h:1121-1124,
  ///        Aggregate.h:343-346), and it is what both
  ///        exec::resolveIntermediateType and exec::Aggregate::create are handed
  ///        in RollupOperator::makeAggregateInfos (RollupOperator.cpp:173,
  ///        :184-189) — the merge behaviour comes from the CALLER
  ///        (GroupingSet isRawInput=false / isPartial=true), not from a
  ///        companion suffix. Note this differs from an ordinary Gluten
  ///        AggregationNode, whose call names are already companion-rewritten;
  ///        RollupTranslator rebuilds the list from the Substrait measures for
  ///        exactly this reason. See the long comment there.
  ///
  ///        v1 requires mask == nullptr, sortingKeys empty and
  ///        distinct == false; the planner falls back otherwise (see
  ///        DESIGN.md §6).
  RollupNode(
      facebook::velox::core::PlanNodeId id,
      std::vector<facebook::velox::core::FieldAccessTypedExprPtr> groupingKeys,
      std::vector<Level> levels,
      std::vector<std::string> aggregateNames,
      std::vector<facebook::velox::core::AggregationNode::Aggregate> aggregates,
      facebook::velox::core::PlanNodePtr source)
      : PlanNode(std::move(id)),
        groupingKeys_{std::move(groupingKeys)},
        levels_{std::move(levels)},
        aggregateNames_{std::move(aggregateNames)},
        aggregates_{std::move(aggregates)},
        sources_{std::move(source)},
        outputType_{makeOutputType(groupingKeys_, aggregateNames_, aggregates_, sources_[0])} {
    VELOX_CHECK_GE(groupingKeys_.size(), 2, "Fused rollup requires at least two keys");
    VELOX_CHECK_EQ(levels_.size(), groupingKeys_.size() + 1);
    VELOX_CHECK_EQ(aggregateNames_.size(), aggregates_.size());
    for (auto i = 0; i < levels_.size(); ++i) {
      VELOX_CHECK_EQ(
          levels_[i].numKeys,
          static_cast<int32_t>(groupingKeys_.size()) - i,
          "Levels must be ordered finest-first with a strict prefix hierarchy");
      VELOX_CHECK_EQ(levels_[i].keyIsNull.size(), groupingKeys_.size());
    }
  }

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "Rollup";
  }

  /// The full key hierarchy, coarsest first.
  const std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& groupingKeys() const {
    return groupingKeys_;
  }

  /// Finest-first, size n+1.
  const std::vector<Level>& levels() const {
    return levels_;
  }

  const std::vector<std::string>& aggregateNames() const {
    return aggregateNames_;
  }

  const std::vector<facebook::velox::core::AggregationNode::Aggregate>& aggregates() const {
    return aggregates_;
  }

  const facebook::velox::RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  /// Index of the trailing grouping-id column in outputType().
  facebook::velox::column_index_t gidChannel() const {
    return static_cast<facebook::velox::column_index_t>(outputType_->size() - 1);
  }

  /// The operator only ever emits partial state, which the post-shuffle final
  /// aggregation merges. Duplicate group states are therefore legal output, and
  /// levels flush rather than spill. See DESIGN.md §4.
  bool canSpill(const facebook::velox::core::QueryConfig& /*config*/) const override {
    return false;
  }

  /// Following the in-tree Gluten precedent for custom nodes
  /// (CudfVectorStream.h:153-155). See DESIGN.md §5.2 (serde) — this costs
  /// us plan tracing/replay on rollup stages.
  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("Rollup plan node is not serializable");
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "hierarchy: [";
    for (auto i = 0; i < groupingKeys_.size(); ++i) {
      if (i > 0) {
        stream << ", ";
      }
      stream << groupingKeys_[i]->name();
    }
    stream << "] levels: [";
    for (auto i = 0; i < levels_.size(); ++i) {
      if (i > 0) {
        stream << ", ";
      }
      stream << levels_[i].numKeys << "k/gid=" << levels_[i].gid;
    }
    stream << "] aggregates: " << aggregateNames_.size();
  }

  /// ROW(k1..kn nullable, acc1..accm intermediate, gid BIGINT).
  ///
  /// The intermediate type per aggregate comes from
  /// facebook::velox::exec::resolveIntermediateType(call->name(), rawInputTypes),
  /// which is PUBLIC — velox/exec/AggregateFunctionRegistry.h:58. (An earlier
  /// revision of this draft claimed it was file-local to AggregateInfo.cpp and
  /// fell back to trusting call->type(); that was wrong on both counts. Using
  /// the registry is what guarantees this node's output type and
  /// RollupOperator::makeNaturalType agree.)
  ///
  /// NULLABILITY (the "make every key column nullable" step): VERIFIED NO-OP.
  /// A coarse level substitutes NULL for the keys it masks out, so every key
  /// column in the output must accept nulls. In Spark that would require
  /// rewriting the field's nullable flag; in Velox it requires nothing at all.
  /// `velox/type/Type.h` has no notion of nullability anywhere — grep for
  /// "nullable" in that header returns zero hits, there is no Type::asNullable(),
  /// and nullability is purely a property of a *vector*'s null buffer, not of its
  /// TypePtr. So the key types are copied through verbatim and
  /// RollupOperator::project's BaseVector::createNullConstant(inputType->childAt(j), ...)
  /// (RollupOperator.cpp:627-628) is already well typed against them. This
  /// comment exists in place of the dead `asNullable()` loop an earlier revision
  /// of the draft implied was needed.
  ///
  /// COLUMN NAMING. Names here are effectively free-form: Gluten's Substrait
  /// reader resolves every downstream field reference POSITIONALLY and then
  /// recovers the name from the input type
  /// (`makeFieldAccessExpr(inputColumnType->nameOf(idx), ...)`,
  /// cpp/velox/substrait/SubstraitToVeloxExpr.cc:219), and the core::ExpandNode
  /// this node replaces does not carry semantic names either — it labels its
  /// output `n{planNodeId}_{idx}` via SubstraitParser::makeNodeName
  /// (SubstraitToVeloxPlan.cc:930-936, SubstraitParser.cc:174-176). There is no
  /// "spark_grouping_id" string anywhere in the Gluten native or JVM tree, so
  /// there is nothing to match. What the contract DOES require is that gid is the
  /// LAST column (gidChannel() above depends on it) and that no two names
  /// collide, because a duplicate name would make a FieldAccessTypedExpr built
  /// off this type ambiguous. Hence the uniquifying loop below.
  static facebook::velox::RowTypePtr makeOutputType(
      const std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& groupingKeys,
      const std::vector<std::string>& aggregateNames,
      const std::vector<facebook::velox::core::AggregationNode::Aggregate>& aggregates,
      const facebook::velox::core::PlanNodePtr& source) {
    // Static, and called from the member-initialiser list before any member is
    // alive, so this body touches nothing but its arguments.
    //
    // FIXED (adversarial review): the n >= 2 check lives HERE as well as in the
    // constructor body. outputType_ is the last member, so makeOutputType runs
    // before any statement of the constructor body; with n == 1 the body's
    // VELOX_CHECK_GE(groupingKeys_.size(), 2) is only reached if every check
    // below happens to pass first. Duplicating it makes the n == 1 rejection
    // (DESIGN.md §6 fallback) deterministic and gives the same message
    // regardless of which check wins the race.
    VELOX_CHECK_GE(
        groupingKeys.size(), 2, "Fused rollup requires at least two keys");
    VELOX_CHECK_NOT_NULL(source, "Rollup requires a source plan node");
    VELOX_CHECK_EQ(
        aggregateNames.size(),
        aggregates.size(),
        "Rollup aggregate names and aggregates must correspond");

    const auto& inputType = source->outputType();
    VELOX_CHECK_EQ(
        inputType->size(),
        groupingKeys.size() + aggregates.size(),
        "Rollup source must produce exactly ROW(k1..kn, acc1..accm)");

    std::vector<std::string> names;
    std::vector<facebook::velox::TypePtr> types;
    names.reserve(inputType->size() + 1);
    types.reserve(inputType->size() + 1);

    // k1..kn, in hierarchy order, straight off the source. Taking the type from
    // the source rather than from groupingKeys[i]->type() is deliberate: it is
    // the same expression RollupOperator::project uses for the masked-out
    // columns, so the two cannot drift.
    for (size_t i = 0; i < groupingKeys.size(); ++i) {
      VELOX_CHECK_NOT_NULL(groupingKeys[i]);
      VELOX_CHECK_EQ(
          inputType->nameOf(i),
          groupingKeys[i]->name(),
          "Rollup key {} must be at source channel {}",
          groupingKeys[i]->name(),
          i);
      names.push_back(groupingKeys[i]->name());
      types.push_back(inputType->childAt(i));
    }

    // acc1..accm at their INTERMEDIATE types. Same registry call, same
    // arguments, as RollupOperator::makeNaturalType (RollupOperator.cpp:121-122)
    // and RollupOperator::makeAggregateInfos (:173). That shared source of truth
    // is what guarantees the cascade pipe types line up: level i's naturalType
    // has to be exactly level i+1's declared input type.
    for (size_t i = 0; i < aggregates.size(); ++i) {
      VELOX_CHECK_NOT_NULL(aggregates[i].call);
      names.push_back(aggregateNames[i]);
      types.push_back(facebook::velox::exec::resolveIntermediateType(
          aggregates[i].call->name(), aggregates[i].rawInputTypes));
    }

    // FIXED (adversarial review): the uniquifier below only protected the gid
    // column, but the same hazard applies to the key/accumulator names, which
    // come from two independent sources (source->outputType() and
    // aggregateNames). An aggregate named after a grouping key produces a
    // ROW type with two identically named children, which makes
    // FieldAccessTypedExpr resolution against this type ambiguous — silently
    // resolving to the first match. Fail loudly instead; the planner controls
    // both name lists and can always disambiguate.
    {
      std::unordered_set<std::string> seen;
      for (const auto& n : names) {
        VELOX_CHECK(
            seen.insert(n).second,
            "Rollup output type would contain a duplicate column name '{}'; "
            "grouping key names and aggregate names must be disjoint",
            n);
      }
    }

    // Trailing gid. Must stay last — gidChannel() is defined as size() - 1.
    std::string gidName{"gid"};
    for (int32_t suffix = 0;
         std::find(names.begin(), names.end(), gidName) != names.end();) {
      gidName = fmt::format("gid_{}", ++suffix);
    }
    names.push_back(std::move(gidName));
    types.push_back(facebook::velox::BIGINT());

    return facebook::velox::ROW(std::move(names), std::move(types));
  }

  const std::vector<facebook::velox::core::FieldAccessTypedExprPtr> groupingKeys_;
  const std::vector<Level> levels_;
  const std::vector<std::string> aggregateNames_;
  const std::vector<facebook::velox::core::AggregationNode::Aggregate> aggregates_;
  const std::vector<facebook::velox::core::PlanNodePtr> sources_;
  const facebook::velox::RowTypePtr outputType_;
};

using RollupNodePtr = std::shared_ptr<const RollupNode>;

} // namespace gluten
