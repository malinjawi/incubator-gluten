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
#include <optional>
#include <unordered_set>

#include <fmt/format.h>

#include "velox/core/PlanNode.h"
// resolveIntermediateType is declared here (AggregateFunctionRegistry.h:57-60)
// and is public. Pulling an exec/ header into a node header is heavier than the
// usual core/-only diet, but it is the single source of truth that keeps this
// node's output type and MultiGroupingSetAggregation::makeNaturalType in
// agreement; the alternative (trusting call->type()) is what an earlier
// revision got wrong.
#include "velox/exec/AggregateFunctionRegistry.h"
#include "operators/rollup/GroupingSetLattice.h"

namespace facebook::velox::exec {

/// Fused grouping-set aggregation plan node.
///
/// Replaces the three-operator shape
///   PartialAgg(finest) -> Expand(n+1 projections) -> PartialMerge
/// with a single node that consumes finest-grain intermediate accumulators and
/// emits the union of all grouping sets, still as intermediate accumulators,
/// ready for the post-shuffle final aggregation.
///
/// REPRESENTATION (HYBRID_DESIGN.md §1). A grouping set is a BOOLEAN MASK over
/// the grouping keys plus an explicit grouping-id value. This generalises the
/// prefix-ROLLUP-only shape it replaces: ROLLUP, CUBE, arbitrary GROUPING SETS
/// and multiple ROLLUPs in one GROUP BY are all one code path, because the
/// operator's derivation forest (GroupingSetLattice.h) is defined by mask
/// containment and degenerates to a cascade for a chain and to a flat fan-out
/// for an antichain.
///
/// Contract with the producer (enforced with VELOX_CHECKs in
/// MultiGroupingSetAggregation::initialize()):
///   source()->outputType() == ROW(k1..kn, acc1..accm)
/// i.e. the grouping keys come first, in the declared order, followed by one
/// intermediate-typed column per aggregate.
///
/// Output type is uniform across sets:
///   ROW(k1..kn, acc1..accm, gid BIGINT)
///
/// OPEN -- UPSTREAMING BLOCKERS. This node is currently shaped for exactly one
/// consumer (Gluten, which builds it in process from Substrait). Before it can
/// go to Velox as a first-class node it needs:
///   1. To live in velox/core/PlanNode.h, namespace core. Every production node
///      does; the only `public core::PlanNode` types outside core/ are internal
///      scaffolding (OperatorUtils' BlockedNode, trace's DummySourceNode) and a
///      doc example. PlanNodeVisitor and the serde/toString/summary tests are
///      all organised around core/.
///   2. (1) is still blocked by outputType_ calling resolveIntermediateType()
///      from the member-initializer list, so constructing the node requires a
///      populated aggregate registry and forces this exec/ -> core/ include
///      inversion. That call is now DETERMINISTIC given the registered
///      functions (it depends on which functions exist, never on registration
///      order or process), so the node is well-defined for its in-process
///      Gluten consumer; see makeOutputType(). The residual constraint -- a
///      planner-only or serde-only process with an empty registry cannot build
///      it -- is what still blocks a move to core::. The upstream fix is for the
///      TRANSLATOR to build each CallTypedExpr with the correct intermediate
///      type, as AggregationNode at kIntermediate does.
///   3. RESOLVED. outputType_ no longer reads source()->outputType(); key types
///      come from groupingKeys[i]->type(), exactly as AggregationNode's
///      getAggregationOutputType does. The constructor still VELOX_CHECKs that
///      those agree with the source, but the OUTPUT TYPE is derived purely from
///      the node's own fields, so it survives a plan boundary intact.
///   4. A Builder (AggregationNode and ExpandNode both have one), an
///      accept(PlanNodeVisitor&) override, and an addSummaryDetails() override
///      (addDetails() emits an unbounded per-set bitstring, so the default
///      truncation garbles wide CUBEs).
///   5. RESOLVED for single-process use. serialize()/create() now round-trip
///      the node (registered from registerMultiGroupingSetAggregation() under
///      "GroupingSetAggregationNode"), covered by a PlanNodeSerdeTest-style
///      round-trip. The registration lives on the exec side rather than in
///      core::PlanNode::registerSerDe() precisely because the node is in exec/.
///   6. VELOX_USER_CHECK rather than VELOX_CHECK for planner-supplied
///      invariants, matching AggregationNode's constructor and every
///      Builder::build().
///
/// Also declare, rather than bury: supportsBarrier() defaults to false here
/// while AggregationNode returns true, and canSpill() is false. Both are
/// deliberate (this operator flushes rather than spills, and implements no
/// barrier drain), but substituting this node for PartialAgg+Expand+PartialMerge
/// is a capability regression on both axes for that pipeline.
class GroupingSetAggregationNode : public core::PlanNode {
 public:
  /// One grouping set.
  ///
  /// gid and keyIsActive are supplied verbatim by the planner (extracted from
  /// the literal projections of the original Substrait ExpandRel). We
  /// deliberately do not recompute Spark's GROUPING_ID bitmask in C++: its bit
  /// order is version-dependent, and a masked-NULL key is indistinguishable at
  /// the output from a genuine data NULL, so gid is the only disambiguator.
  /// Any design that derives gid from the mask in C++ will eventually disagree
  /// with Spark. Keep it as data.
  struct Set {
    /// Size n. keyIsActive[j] == true means key j participates in this set's
    /// grouping; a key that is not active is projected to NULL in the output.
    std::vector<bool> keyIsActive;

    /// The planner's grouping-id literal for this set, emitted in the trailing
    /// gid column.
    int64_t gid{0};

    GroupingSetMask mask() const {
      GroupingSetMask m{0};
      for (size_t j = 0; j < keyIsActive.size(); ++j) {
        if (keyIsActive[j]) {
          m |= (GroupingSetMask{1} << j);
        }
      }
      return m;
    }

    int32_t numActiveKeys() const {
      return static_cast<int32_t>(
          std::count(keyIsActive.begin(), keyIsActive.end(), true));
    }
  };

  /// \param groupingKeys the full key list k1..kn as field accesses into
  ///        source->outputType(), at channels 0..n-1.
  /// \param groupingSets one entry per emitted grouping set. Order is free --
  ///        the operator computes its own topological order over the lattice.
  /// \param aggregateNames output names, size m.
  /// \param aggregates size m.
  ///
  ///        NAME CONTRACT -- call->name() must be the BASE, UNSUFFIXED Velox
  ///        aggregate function name ("sum", not "sum_partial" / "sum_merge"),
  ///        and rawInputTypes must be the true RAW argument types. That pair is
  ///        what identifies the function (PlanNode.h:1121-1124,
  ///        Aggregate.h:343-346) and it is what both resolveIntermediateType
  ///        and Aggregate::create are handed. The merge behaviour comes from
  ///        the CALLER (GroupingSet isRawInput=false / isPartial=true), not
  ///        from a companion suffix. A companion-suffixed name breaks
  ///        resolveIntermediateType, which would be asked to match raw args
  ///        against a signature declared over the intermediate type (avg: raw
  ///        DOUBLE vs ROW(DOUBLE, BIGINT)). This differs from an ordinary
  ///        Gluten AggregationNode, whose call names are already
  ///        companion-rewritten, so the translator must rebuild the list from
  ///        the Substrait measures.
  ///
  ///        v1 requires mask == nullptr, sortingKeys empty and
  ///        distinct == false (HYBRID_DESIGN.md §6.1); the operator CHECKs it.
  /// \param childGroupedSet index of the grouping set at whose grain the child
  ///        operator already delivers rows, if the planner can prove it
  ///        (HYBRID_DESIGN.md §3.2). That set then runs with NO HASH TABLE.
  ///        Correctness does not depend on the claim -- a violated hint costs
  ///        downstream rows, not wrong answers, because the emitted stream is
  ///        partial state that a final aggregation merges.
  GroupingSetAggregationNode(
      core::PlanNodeId id,
      std::vector<core::FieldAccessTypedExprPtr> groupingKeys,
      std::vector<Set> groupingSets,
      std::vector<std::string> aggregateNames,
      std::vector<core::AggregationNode::Aggregate> aggregates,
      std::optional<int32_t> childGroupedSet,
      core::PlanNodePtr source)
      : PlanNode(std::move(id)),
        groupingKeys_{std::move(groupingKeys)},
        groupingSets_{std::move(groupingSets)},
        aggregateNames_{std::move(aggregateNames)},
        aggregates_{std::move(aggregates)},
        childGroupedSet_{childGroupedSet},
        sources_{std::move(source)},
        outputType_{
            makeOutputType(groupingKeys_, aggregateNames_, aggregates_)} {
    const auto numKeys = static_cast<int32_t>(groupingKeys_.size());
    VELOX_CHECK_GE(numKeys, 1, "A grouping-set aggregation needs a key");
    VELOX_CHECK_LE(
        numKeys, 64, "A grouping-set aggregation supports at most 64 keys");
    VELOX_CHECK(
        !groupingSets_.empty(),
        "A grouping-set aggregation needs at least one grouping set");
    VELOX_CHECK_EQ(aggregateNames_.size(), aggregates_.size());

    // Producer contract: source must be ROW(k1..kn, acc1..accm) with the keys
    // at channels 0..n-1 and matching types. This is validated HERE, against
    // the source, but is deliberately NOT part of output-type derivation:
    // makeOutputType() reads only the node's own fields (item 2 of the audit),
    // so a node is self-describing across a plan boundary. The operator's
    // initialize() re-asserts the same contract at execution time.
    VELOX_CHECK_NOT_NULL(sources_[0], "GroupingSetAggregation requires a source");
    const auto& inputType = sources_[0]->outputType();
    VELOX_CHECK_EQ(
        inputType->size(),
        groupingKeys_.size() + aggregates_.size(),
        "GroupingSetAggregation source must produce ROW(k1..kn, acc1..accm)");
    for (size_t i = 0; i < groupingKeys_.size(); ++i) {
      VELOX_CHECK_NOT_NULL(groupingKeys_[i]);
      VELOX_CHECK_EQ(
          inputType->nameOf(i),
          groupingKeys_[i]->name(),
          "Grouping key {} must be at source channel {}",
          groupingKeys_[i]->name(),
          i);
      VELOX_CHECK(
          inputType->childAt(i)->equivalent(*groupingKeys_[i]->type()),
          "Grouping key {} type disagrees with source channel {}",
          groupingKeys_[i]->name(),
          i);
    }

    // Duplicate masks are a planner bug: two sets at the same grain with
    // different gids would each be a lattice root and each scan the raw input,
    // doing identical work twice. Reject rather than paper over
    // (HYBRID_DESIGN.md §2.2 step 3).
    std::unordered_set<GroupingSetMask> seen;
    for (const auto& set : groupingSets_) {
      VELOX_CHECK_EQ(
          set.keyIsActive.size(),
          groupingKeys_.size(),
          "Every grouping-set mask must be as wide as the key list");
      VELOX_CHECK(
          seen.insert(set.mask()).second,
          "Duplicate grouping-set mask; two sets at the same grain must be "
          "merged by the planner");
    }

    if (childGroupedSet_.has_value()) {
      VELOX_CHECK_GE(*childGroupedSet_, 0);
      VELOX_CHECK_LT(
          *childGroupedSet_, static_cast<int32_t>(groupingSets_.size()));
    }
  }

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "GroupingSetAggregation";
  }

  const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys() const {
    return groupingKeys_;
  }

  const std::vector<Set>& groupingSets() const {
    return groupingSets_;
  }

  const std::vector<std::string>& aggregateNames() const {
    return aggregateNames_;
  }

  const std::vector<core::AggregationNode::Aggregate>& aggregates() const {
    return aggregates_;
  }

  const std::optional<int32_t>& childGroupedSet() const {
    return childGroupedSet_;
  }

  const RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  /// Index of the trailing grouping-id column in outputType().
  column_index_t gidChannel() const {
    return static_cast<column_index_t>(outputType_->size() - 1);
  }

  /// The operator only ever emits partial state, which the post-shuffle final
  /// aggregation merges. Duplicate group states are therefore legal output, and
  /// sets flush rather than spill (HYBRID_DESIGN.md §4.5).
  bool canSpill(const core::QueryConfig& /*config*/) const override {
    return false;
  }

  /// Round-trips the node to/from folly::dynamic. Registered under
  /// "GroupingSetAggregationNode" from registerMultiGroupingSetAggregation()
  /// (exec side, since the node lives in exec/). create() reconstructs the node
  /// through the public constructor, so every constructor VELOX_CHECK runs on
  /// deserialize and outputType_ is re-derived identically -- the aggregate
  /// intermediate types come back through resolveIntermediateType(), which
  /// requires the deserializing process to have the aggregates registered (the
  /// same registry constraint construction already imposes).
  folly::dynamic serialize() const override {
    // PlanNode::serialize() writes name (== "GroupingSetAggregationNode"), id
    // and the serialized sources.
    auto obj = PlanNode::serialize();
    obj["groupingKeys"] = ISerializable::serialize(groupingKeys_);
    obj["aggregateNames"] = ISerializable::serialize(aggregateNames_);
    obj["aggregates"] = folly::dynamic::array;
    for (const auto& aggregate : aggregates_) {
      obj["aggregates"].push_back(aggregate.serialize());
    }
    obj["groupingSets"] = folly::dynamic::array;
    for (const auto& set : groupingSets_) {
      folly::dynamic s = folly::dynamic::object();
      s["gid"] = set.gid;
      folly::dynamic active = folly::dynamic::array;
      for (const bool b : set.keyIsActive) {
        active.push_back(b);
      }
      s["keyIsActive"] = std::move(active);
      obj["groupingSets"].push_back(std::move(s));
    }
    if (childGroupedSet_.has_value()) {
      obj["childGroupedSet"] = *childGroupedSet_;
    }
    return obj;
  }

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context) {
    auto sources =
        ISerializable::deserialize<std::vector<core::PlanNode>>(
            obj["sources"], context);
    VELOX_CHECK_EQ(
        1, sources.size(), "GroupingSetAggregation has exactly one source");

    auto groupingKeys =
        ISerializable::deserialize<std::vector<core::FieldAccessTypedExpr>>(
            obj["groupingKeys"], context);
    auto aggregateNames = ISerializable::deserialize<std::vector<std::string>>(
        obj["aggregateNames"]);

    std::vector<core::AggregationNode::Aggregate> aggregates;
    aggregates.reserve(obj["aggregates"].size());
    for (const auto& aggregate : obj["aggregates"]) {
      aggregates.push_back(
          core::AggregationNode::Aggregate::deserialize(aggregate, context));
    }

    std::vector<Set> groupingSets;
    groupingSets.reserve(obj["groupingSets"].size());
    for (const auto& s : obj["groupingSets"]) {
      Set set;
      set.gid = s["gid"].asInt();
      set.keyIsActive.reserve(s["keyIsActive"].size());
      for (const auto& b : s["keyIsActive"]) {
        set.keyIsActive.push_back(b.asBool());
      }
      groupingSets.push_back(std::move(set));
    }

    std::optional<int32_t> childGroupedSet;
    if (obj.count("childGroupedSet")) {
      childGroupedSet = static_cast<int32_t>(obj["childGroupedSet"].asInt());
    }

    return std::make_shared<GroupingSetAggregationNode>(
        obj["id"].asString(),
        std::move(groupingKeys),
        std::move(groupingSets),
        std::move(aggregateNames),
        std::move(aggregates),
        childGroupedSet,
        std::move(sources[0]));
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "keys: [";
    for (size_t i = 0; i < groupingKeys_.size(); ++i) {
      if (i > 0) {
        stream << ", ";
      }
      stream << groupingKeys_[i]->name();
    }
    stream << "] sets: [";
    for (size_t i = 0; i < groupingSets_.size(); ++i) {
      if (i > 0) {
        stream << ", ";
      }
      stream << "0b";
      for (auto j = static_cast<int32_t>(groupingKeys_.size()) - 1; j >= 0;
           --j) {
        stream << (groupingSets_[i].keyIsActive[j] ? '1' : '0');
      }
      stream << "/gid=" << groupingSets_[i].gid;
    }
    stream << "] aggregates: " << aggregateNames_.size();
    if (childGroupedSet_.has_value()) {
      stream << " bypass: " << *childGroupedSet_;
    }
  }

  /// ROW(k1..kn, acc1..accm intermediate, gid BIGINT).
  ///
  /// NULLABILITY (the "make every key column nullable" step): VERIFIED NO-OP.
  /// A coarse set substitutes NULL for the keys it masks out, so every key
  /// column in the output must accept nulls. In Spark that would require
  /// rewriting the field's nullable flag; in Velox it requires nothing at all
  /// -- velox/type/Type.h has no notion of nullability anywhere, and
  /// nullability is purely a property of a vector's null buffer, not of its
  /// TypePtr. So key types are copied through verbatim and the operator's
  /// BaseVector::createNullConstant(inputType->childAt(j), ...) is already well
  /// typed against them.
  static RowTypePtr makeOutputType(
      const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys,
      const std::vector<std::string>& aggregateNames,
      const std::vector<core::AggregationNode::Aggregate>& aggregates) {
    // Static, and called from the member-initialiser list before any member is
    // alive, so this body touches nothing but its arguments. It reads ONLY the
    // node's own fields -- groupingKeys, aggregateNames, aggregates -- and never
    // the source (item 2 of the audit): a node's output type must be derivable
    // without a live producer for it to be trustworthy across a plan boundary.
    // The source/key consistency this used to lean on is enforced instead by
    // the constructor body.
    VELOX_CHECK_GE(
        groupingKeys.size(), 1, "A grouping-set aggregation needs a key");
    VELOX_CHECK_EQ(
        aggregateNames.size(),
        aggregates.size(),
        "Aggregate names and aggregates must correspond");

    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(groupingKeys.size() + aggregates.size() + 1);
    types.reserve(groupingKeys.size() + aggregates.size() + 1);

    // k1..kn taken from each field access's OWN name and type. AggregationNode's
    // getAggregationOutputType does exactly this; the type a FieldAccessTypedExpr
    // carries is the type of the source channel it references, so this is the
    // same value the old source->childAt(i) produced, without needing the
    // source. The constructor body VELOX_CHECKs that this agrees with the source.
    for (size_t i = 0; i < groupingKeys.size(); ++i) {
      VELOX_CHECK_NOT_NULL(groupingKeys[i]);
      names.push_back(groupingKeys[i]->name());
      types.push_back(groupingKeys[i]->type());
    }

    // acc1..accm at their INTERMEDIATE types, resolved through the registry --
    // the same call the operator's makeNaturalType makes. That shared source of
    // truth is what guarantees the derivation pipe types line up: a parent's
    // natural type must be exactly what its children were constructed over.
    //
    // REGISTRY DEPENDENCY (item 1). resolveIntermediateType() is a pure lookup
    // keyed by (base function name, raw argument types); it depends on WHICH
    // functions are registered, never on registration order, and returns the
    // same type in every process that has the aggregate registered. The node it
    // yields is therefore deterministic given a populated registry -- the only
    // context this node runs in (Gluten builds it in-process after Velox's
    // aggregate registration). The residual constraint (a planner-only or
    // serde-only process with an empty registry cannot construct the node) is a
    // documented upstreaming blocker, not a deployability one; see the class
    // header. It is kept here rather than pushed to the caller so that
    // makeOutputType and makeNaturalType cannot disagree.
    for (size_t i = 0; i < aggregates.size(); ++i) {
      VELOX_CHECK_NOT_NULL(aggregates[i].call);
      names.push_back(aggregateNames[i]);
      types.push_back(resolveIntermediateType(
          aggregates[i].call->name(), aggregates[i].rawInputTypes));
    }

    // Duplicate output column names make a FieldAccessTypedExpr built off this
    // type silently ambiguous (it resolves to the first match). The key names
    // and the aggregate names come from two independent sources, so check.
    {
      std::unordered_set<std::string> seen;
      for (const auto& n : names) {
        VELOX_CHECK(
            seen.insert(n).second,
            "GroupingSetAggregation output type would contain a duplicate "
            "column name '{}'; key names and aggregate names must be disjoint",
            n);
      }
    }

    // Trailing gid. Must stay last -- gidChannel() is defined as size() - 1.
    std::string gidName{"gid"};
    for (int32_t suffix = 0;
         std::find(names.begin(), names.end(), gidName) != names.end();) {
      gidName = fmt::format("gid_{}", ++suffix);
    }
    names.push_back(std::move(gidName));
    types.push_back(BIGINT());

    return ROW(std::move(names), std::move(types));
  }

  const std::vector<core::FieldAccessTypedExprPtr> groupingKeys_;
  const std::vector<Set> groupingSets_;
  const std::vector<std::string> aggregateNames_;
  const std::vector<core::AggregationNode::Aggregate> aggregates_;
  const std::optional<int32_t> childGroupedSet_;
  const std::vector<core::PlanNodePtr> sources_;
  const RowTypePtr outputType_;
};

using GroupingSetAggregationNodePtr =
    std::shared_ptr<const GroupingSetAggregationNode>;

} // namespace facebook::velox::exec
