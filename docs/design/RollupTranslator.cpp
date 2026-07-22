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

// Registration and Substrait wiring for the fused rollup operator (draft).
//
// Two halves:
//   Part A — the PlanNodeTranslator and where it gets registered.
//   Part B — where the RollupNode is produced from the Substrait plan.
//
// Part A is complete and follows the in-tree precedent exactly. Part B is a
// sketch against SubstraitToVeloxPlan.cc, with the extraction helpers stubbed.

#include "velox/exec/Operator.h"

#include "RollupNode.h"
#include "RollupOperator.h"

namespace gluten {

// ===========================================================================
// Part A — operator registration
// ===========================================================================

/// Maps RollupNode to RollupOperator. Same shape as Gluten's only existing
/// custom-operator translator, CudfVectorStreamOperatorTranslator
/// (cpp/velox/operators/plannodes/CudfVectorStream.h:208-218).
///
/// LocalPlanner.cpp:707-720 walks the built-in dynamic_pointer_cast chain first
/// and falls through to Operator::fromPlanNode (:716), which consults every
/// registered translator in order. Returning nullptr means "not mine".
class RollupOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override {
    if (auto rollupNode = std::dynamic_pointer_cast<const RollupNode>(node)) {
      return std::make_unique<RollupOperator>(id, ctx, rollupNode);
    }
    return nullptr;
  }

  // maxDrivers: intentionally not overridden. This operator is pre-shuffle and
  // emits partial state, so it parallelises exactly like a partial aggregation —
  // no serialisation constraint. Returning nullopt (the base default,
  // Operator.h:159-162) lets detail::maxDrivers (LocalPlanner.cpp:322) use the
  // pipeline-wide value.

  // toJoinBridge / toOperatorSupplier: not applicable.
};

/// Call once at backend init, next to the existing
///   velox::exec::Operator::registerOperator(
///       std::make_unique<CudfVectorStreamOperatorTranslator>());
/// at cpp/velox/compute/VeloxBackend.cc:183.
///
/// Unlike the cudf one this is unconditional — there is no build flag. Whether
/// the fused path is *used* is decided on the JVM side by the config flag; the
/// native side simply knows how to run the node if it is asked to.
///
/// registerOperator appends to a static vector (Operator.cpp:233-236), so
/// calling it twice would double-register. VeloxBackend init runs once per
/// process, but a folly::call_once here would be cheap insurance.
void registerRollupOperator() {
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<RollupOperatorTranslator>());
}

// ===========================================================================
// Part B — Substrait -> RollupNode
// ===========================================================================
//
// Sketch of the change to cpp/velox/substrait/SubstraitToVeloxPlan.cc. No new
// Substrait message is introduced. The JVM rule keeps emitting the existing
//
//     ExpandRel  { fields: n+1 duplicate-sets, one per grouping level }
//       -> AggregateRel { finest grain, allowFlush= }
//
// shape and marks it. We detect the marker at the top of
//
//     core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(
//         const ::substrait::ExpandRel& expandRel)      // :899-938
//
// and, when present, build a RollupNode instead of a core::ExpandNode. The
// unmarked path is untouched, so this cannot regress non-rollup Expand.
//
// Why marker-on-existing-rel rather than a new rel:
//   * no protobuf change, so no cross-version negotiation;
//   * forward/backward safe — an older native library ignores the marker and
//     runs the correct, slower Expand + PartialMerge plan;
//   * the cost decision stays on the JVM side, where the statistics are. (The
//     ClickHouse backend infers lazy expand purely from child-rel shape; that
//     works but puts the policy in the wrong layer and is harder to disable per
//     query.)
//
// The marker uses the established idiom,
//   SubstraitParser::configSetInOptimization(rel.advanced_extension(), "allowFlush=")
//     // SubstraitToVeloxPlan.cc:279-280
//   SubstraitParser::configSetInOptimization(aggRel.advanced_extension(), "isStreaming=")
//     // SubstraitToVeloxPlan.cc:598-599
// with a new key "isRollup=".
//
// RESOLVED (was OPEN QUESTION 3): ExpandRel DOES carry an advanced_extension —
// `substrait.extensions.AdvancedExtension advanced_extension = 10;`
// (substrait/algebra.proto, message ExpandRel). The JVM plumbing already exists
// too: ExpandRelNode has an AdvancedExtensionNode-taking constructor
// (ExpandRelNode.java:36-41) and RelBuilder.makeExpandRel has the matching
// overload (RelBuilder.java:222-230). PLANNER.md's claim that "the transform
// layer that builds ExpandRel needs to learn to carry an AdvancedExtension" is
// wrong — it already can.
//
// One thing to be careful about: ExpandExecTransformer already uses that slot on
// the VALIDATION path, to ship the input type struct
// (ExpandExecTransformer.scala:88-95). That uses the `enhancement` field; the
// marker idiom uses `optimization` (SubstraitParser.cc:281-289 reads
// extension.optimization()). They do not collide, but the rule must use
// ExtensionBuilder.makeAdvancedExtension(optimization, enhancement) rather than
// overwriting the node.
//
// Also note the marker convention is not just "key present": configSetInOptimization
// requires the character immediately after the key to be '1'
// (SubstraitParser.cc:285). The rule must emit "isRollup=1", not "isRollup=true".

// When this block moves into SubstraitToVeloxPlan.cc it needs two includes that
// file does not have today:
//   #include "velox/exec/AggregateFunctionRegistry.h"  // resolveIntermediateType
//   #include "RollupNode.h"
// SubstraitParser.h is already included there, and functionMap_ / nextPlanNodeId()
// / exprConverter_ are existing members of SubstraitToVeloxPlanConverter.
// toRollupPlan itself must be declared in SubstraitToVeloxPlan.h next to the
// other toVeloxPlan overloads.

#if 0 // Illustrative — belongs in SubstraitToVeloxPlan.cc, not here.

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ExpandRel& expandRel) {
  core::PlanNodePtr childNode;
  if (expandRel.has_input()) {
    childNode = toVeloxPlan(expandRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in ExpandRel.");
  }

  if (SubstraitParser::configSetInOptimization(
          expandRel.advanced_extension(), "isRollup=")) {
    return toRollupPlan(expandRel, childNode);
  }

  // ... existing ExpandNode path, unchanged (SubstraitToVeloxPlan.cc:906-937).
}

/// Builds a RollupNode from the marked ExpandRel plus its already-converted
/// child (which is the finest-grain flushable partial AggregationNode).
core::PlanNodePtr SubstraitToVeloxPlanConverter::toRollupPlan(
    const ::substrait::ExpandRel& expandRel,
    const core::PlanNodePtr& childNode) {
  // The child must be the finest-grain partial aggregation the JVM rule
  // inserted; its grouping keys are the full rollup hierarchy, in order, and its
  // output is ROW(k1..kn, acc1..accm) — the precondition RollupOperator checks.
  auto childAgg = std::dynamic_pointer_cast<const core::AggregationNode>(childNode);
  VELOX_CHECK_NOT_NULL(
      childAgg, "isRollup= requires an AggregateRel child");

  const auto& groupingKeys = childAgg->groupingKeys();
  // int32_t, not size_t: every index comparison below is against a protobuf
  // *_size() (int) or an int32_t count, and mixing them is a warning farm.
  const auto numKeys = static_cast<int32_t>(groupingKeys.size());

  // Every grouping level is already fully described by the ExpandRel's own
  // projections: each duplicate entry is either a field selection or a literal
  // (SubstraitToVeloxPlan.cc:917-925 rejects anything else). A literal in a key
  // position is Spark's NULL mask; the trailing literal is the grouping id. So
  // we recover the exact per-level gid and null placement Spark computed,
  // without reimplementing the bitmask in C++.
  //
  // INDEXING — this matters and an earlier revision of this draft had it
  // transposed. The substrait message names `fields` as if it were column-major
  // (ExpandField -> SwitchingField -> duplicates, one expression per
  // duplicate_id), but Gluten writes it LEVEL-major: ExpandRelNode.java:60-69
  // emits one ExpandField per projection SET, whose `duplicates` are that set's
  // COLUMNS, and SubstraitToVeloxPlan.cc:912-928 reads it back the same way.
  // So:
  //     expandRel.fields(level).switching_field().duplicates(column)
  // and expandRel.fields_size() is the number of grouping levels, n + 1.
  VELOX_CHECK_EQ(
      expandRel.fields_size(),
      numKeys + 1,
      "A ROLLUP over {} keys must have {} grouping levels",
      numKeys,
      numKeys + 1);

  // The operator's precondition (RollupNode.h:31-36) is that the child emits
  // ROW(k1..kn, acc1..accm) in hierarchy order. Assert it HERE as well as in
  // RollupOperator::initialize(), because everything below reads the Expand
  // projections by *referenced field index* and that only means "key j" if the
  // keys really do sit at channels 0..n-1.
  const auto& childOutput = childAgg->outputType();
  VELOX_CHECK_EQ(
      childOutput->size(),
      numKeys + childAgg->aggregates().size(),
      "Rollup child must emit exactly the rollup keys followed by the aggregate states");
  for (auto j = 0; j < numKeys; ++j) {
    if (childOutput->nameOf(j) != groupingKeys[j]->name()) {
      return nullptr; // Not the projection the rollup rule promises — fall back.
    }
  }
  const auto numAggregates = static_cast<int32_t>(childAgg->aggregates().size());

  std::vector<RollupNode::Level> levels;
  levels.reserve(expandRel.fields_size());

  for (const auto& projectionSet : expandRel.fields()) {
    const auto& duplicates = projectionSet.switching_field().duplicates();
    // A projection set must cover every key slot, every accumulator slot, and
    // the trailing gid.
    VELOX_CHECK_EQ(duplicates.size(), numKeys + numAggregates + 1);

    RollupNode::Level level;
    // FIXED (was: read duplicates[j] positionally as "key slot j"). Slot
    // POSITION is not a reliable proxy for key identity. Vanilla Spark builds
    // the Expand output as `child.output ++ groupByAttrs ++ Seq(gid)`
    // (Analyzer.constructExpand), i.e. the nulled grouping attributes come
    // AFTER the pass-through columns, not before them. Our rule reshapes the
    // child, but hard-coding a positional layout would silently mis-decode the
    // moment that reshaping changes. What is invariant is the *referenced field
    // index*: a selection of child channel j < numKeys is key j, a selection of
    // channel numKeys + a is accumulator a.
    level.keyIsNull.assign(numKeys, true);
    std::vector<bool> accSeen(numAggregates, false);

    for (auto slot = 0; slot + 1 < duplicates.size(); ++slot) {
      const auto& expr = duplicates[slot];
      if (expr.has_selection()) {
        uint32_t fieldIndex = 0;
        // SubstraitParser.h:53-55: returns false for nested/subfield segments,
        // which cannot appear here.
        if (!SubstraitParser::parseReferenceSegment(
                expr.selection().direct_reference(), fieldIndex)) {
          return nullptr;
        }
        if (fieldIndex < static_cast<uint32_t>(numKeys)) {
          level.keyIsNull[fieldIndex] = false;
        } else if (fieldIndex < static_cast<uint32_t>(numKeys + numAggregates)) {
          accSeen[fieldIndex - numKeys] = true;
        } else {
          return nullptr; // References something outside the contract shape.
        }
      } else if (expr.has_literal() && expr.literal().has_null()) {
        // A typed NULL literal is Spark's mask for a key that is dead at this
        // level. Check for null explicitly rather than assuming
        // "literal => null": a non-null literal in a key slot is not a rollup.
      } else {
        return nullptr; // Not a rollup shape — caller falls back.
      }
    }

    // Every accumulator must survive at every level — a level that drops one
    // could not be merged by the cascade.
    for (auto a = 0; a < numAggregates; ++a) {
      if (!accSeen[a]) {
        return nullptr;
      }
    }

    level.numKeys = 0;
    for (auto j = 0; j < numKeys; ++j) {
      if (!level.keyIsNull[j]) {
        ++level.numKeys;
      }
    }

    // Spark's GROUPING_ID for this level, taken verbatim from the trailing
    // literal — we deliberately never recompute the bitmask in C++
    // (RollupNode.h:52-53).
    //
    // Type note: grouping_id() is LongType by default, but
    // `spark.sql.legacy.integerGroupingId=true` makes it IntegerType, and the
    // Expand gid literal follows the same type. Accept both and widen; the node
    // always emits BIGINT (RollupOperator.cpp:636-637).
    const auto& gidSlot = duplicates[duplicates.size() - 1];
    if (!gidSlot.has_literal()) {
      return nullptr;
    }
    if (gidSlot.literal().has_i64()) {
      level.gid = SubstraitParser::getLiteralValue<int64_t>(gidSlot.literal());
    } else if (gidSlot.literal().has_i32()) {
      level.gid = SubstraitParser::getLiteralValue<int32_t>(gidSlot.literal());
    } else {
      return nullptr; // Not the shape we expect — fall back.
    }
    levels.push_back(std::move(level));
  }

  // Validate the prefix property before committing to the fused operator. If the
  // levels are not a strict descending prefix chain this is CUBE or an explicit
  // GROUPING SETS list, which v1 does not fuse. Falling back here (rather than
  // failing) makes the native side robust to a planner bug.
  std::sort(levels.begin(), levels.end(), [](const auto& a, const auto& b) {
    return a.numKeys > b.numKeys;
  });
  for (auto i = 0; i < levels.size(); ++i) {
    if (levels[i].numKeys != static_cast<int32_t>(numKeys) - i) {
      // Not a rollup prefix hierarchy — build the plain ExpandNode instead.
      return nullptr; // caller falls through to the existing path
    }
    // numKeys alone is not enough: {k1, k3} and {k1, k2} both have numKeys == 2
    // but only the latter is a prefix. The operator derives level i from level
    // i-1 by dropping the LAST live key, so the live set must be exactly the
    // prefix k1..k[numKeys].
    for (auto j = 0; j < static_cast<int32_t>(numKeys); ++j) {
      if (levels[i].keyIsNull[j] != (j >= levels[i].numKeys)) {
        return nullptr; // CUBE or explicit GROUPING SETS — fall back.
      }
    }
  }

  // -------------------------------------------------------------------------
  // Aggregate rebuild
  // -------------------------------------------------------------------------
  // The fused operator merges states: it never sees raw input, and it emits
  // states. We must hand RollupNode an Aggregate list expressed for that job.
  //
  // WHICH FLAVOUR OF THE FUNCTION? There are two self-consistent answers and
  // this draft previously gestured at both. Pinning it down:
  //
  //  (A) BASE name, driven by the step.  name = "sum", argTypes = the true raw
  //      input types, and the instance is created with
  //      Aggregate::create(name, kPartial, rawInputTypes, intermediateType).
  //      Aggregate.h:343-352 is explicit that `argTypes` are ALWAYS the raw
  //      input types ("Raw input types. Combined with the function name,
  //      uniquely identifies the function") and that "Partial and intermediate
  //      aggregations create functions using kPartial". Whether raw values or
  //      states arrive is then decided by the CALLER: GroupingSet's
  //      isRawInput = false routes to addIntermediateResults
  //      (GroupingSet.cpp:351-355) and isPartial = true routes the output to
  //      extractAccumulators (GroupingSet.cpp:871-876). This is byte-for-byte
  //      what Velox itself does for a kIntermediate AggregationNode — see
  //      toAggregateInfo, AggregateInfo.cpp:83-105, which calls
  //      resolveIntermediateType(name, rawInputTypes) and then
  //      Aggregate::create(name, isPartialOutput(step) ? kPartial : kSingle,
  //      rawInputTypes, ...).
  //
  //  (B) The "_merge" COMPANION, driven as a standalone function.  name =
  //      "sum_merge", argTypes = {intermediateType}. The companion is a real
  //      registered aggregate whose signature takes and returns the
  //      intermediate type (AggregateCompanionSignatures.cpp:125-138) and whose
  //      factory wraps the base function in an
  //      AggregateCompanionAdapter::MergeFunction
  //      (AggregateCompanionAdapter.cpp:290-330).
  //
  // We take (A), and RollupOperator::makeAggregateInfos is written to match:
  // it calls resolveIntermediateType(call->name(), rawInputTypes) at
  // RollupOperator.cpp:173 and Aggregate::create(name, kPartial,
  // rawInputTypes, ...) at :184-189. Reasons (A) wins:
  //
  //   * (B) would double-wrap. GroupingSet already drives the merge path
  //     directly; going through MergeFunction adds an adapter whose whole
  //     purpose is to make a merge look like a single-step function to a caller
  //     that does NOT know about steps. We are exactly the caller that does.
  //   * (B) breaks the ONE invariant both files depend on:
  //     resolveIntermediateType(name, rawInputTypes) must succeed. Under (B)
  //     the name is the companion but rawInputTypes are still raw, and those
  //     do not match the companion's signature for any function whose
  //     intermediate type differs from its input type — avg is the immediate
  //     counterexample (raw DOUBLE, intermediate ROW(DOUBLE, BIGINT), and
  //     avg_merge's declared argument is the ROW). We would have to carry two
  //     different "rawInputTypes" meanings, one per file.
  //   * (A) keeps Aggregate::rawInputTypes meaning what PlanNode.h:1121-1124
  //     says it means, so companion resolution downstream still works.
  //
  // CONSEQUENCE FOR THE NAME WE STORE: it must be the BASE, UNSUFFIXED Velox
  // function name. We cannot reuse childAgg's own call names, because Gluten
  // has already rewritten those into companion names: the child is the finest
  // partial aggregate, so its measures carry phase
  // INITIAL_TO_INTERMEDIATE -> Step::kPartial (toAggregationFunctionStep,
  // SubstraitToVeloxPlan.cc:288-306) -> "<base>_partial"
  // (toAggregationFunctionName, SubstraitToVeloxPlan.cc:308-351). Feeding
  // "sum_partial" to Aggregate::create with raw arg types would resolve the
  // PARTIAL companion, which expects raw input — precisely the wrong flavour.
  //
  // So we go back to the proto and recover the base name the same way the
  // AggregateRel path does (SubstraitToVeloxPlan.cc:585-590), rather than
  // string-stripping a suffix off the converted node.
  VELOX_CHECK(
      expandRel.input().has_aggregate(),
      "isRollup= requires the ExpandRel's input to be an AggregateRel");
  const auto& childAggRel = expandRel.input().aggregate();
  VELOX_CHECK_EQ(childAggRel.measures().size(), numAggregates);

  std::vector<core::AggregationNode::Aggregate> aggregates;
  aggregates.reserve(numAggregates);

  for (auto i = 0; i < numAggregates; ++i) {
    const auto& measure = childAggRel.measures()[i];
    const auto& sAggFunc = measure.measure();

    // v1 admits none of these. The JVM rule is supposed to fall back before
    // emitting the marker, but a planner bug here would produce silently wrong
    // results, so refuse the fused shape rather than trusting it. Returning
    // nullptr (not failing) keeps the native side robust: the caller builds the
    // ordinary ExpandNode.
    if (measure.has_filter() && measure.filter().ByteSizeLong() > 0) {
      return nullptr; // masked aggregate
    }
    if (sAggFunc.sorts_size() > 0) {
      return nullptr; // order-sensitive aggregate
    }
    if (sAggFunc.invocation() ==
        ::substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT) {
      return nullptr; // DISTINCT aggregate
    }

    // Base (unsuffixed) Velox function name, and the raw argument types, both
    // read from the function map exactly as toVeloxPlan(AggregateRel) does
    // (SubstraitToVeloxPlan.cc:586-592).
    const auto baseFuncName =
        SubstraitParser::findVeloxFunction(functionMap_, sAggFunc.function_reference());
    const std::vector<TypePtr> rawInputTypes = SubstraitParser::sigToTypes(
        SubstraitParser::findFunctionSpec(functionMap_, sAggFunc.function_reference()));

    // The single source of truth for the state type, shared with
    // RollupOperator::makeNaturalType (RollupOperator.cpp:123-124) and
    // makeAggregateInfos (:187). exec::resolveIntermediateType is public —
    // velox/exec/AggregateFunctionRegistry.h:57-60.
    const auto intermediateType =
        exec::resolveIntermediateType(baseFuncName, rawInputTypes);

    // The state column this aggregate merges. Under the precondition checked
    // above it is child output channel numKeys + i.
    const auto stateChannel = numKeys + i;
    const auto& stateName = childOutput->nameOf(stateChannel);

    // Cross-check the child really is a partial aggregation emitting this
    // state type. If the child were kSingle/kFinal its output column would be
    // the FINAL type and merging it would be nonsense.
    if (!childOutput->childAt(stateChannel)->equivalent(*intermediateType)) {
      return nullptr;
    }

    // call: name = base, result type = intermediate type, single argument =
    // the state column. Note the argument list is what
    // AggregateInfo/our makeAggregateInfos turns into input CHANNELS; the
    // operator does not re-evaluate it, but keeping it well formed means the
    // node prints and reasons correctly.
    auto call = std::make_shared<const core::CallTypedExpr>(
        intermediateType,
        std::vector<core::TypedExprPtr>{
            std::make_shared<const core::FieldAccessTypedExpr>(
                intermediateType, stateName)},
        baseFuncName);

    // rawInputTypes are carried through UNCHANGED — they are what identifies
    // the function (PlanNode.h:1121-1124, Aggregate.h:343-346), and they are
    // what both resolveIntermediateType and Aggregate::create are given in
    // RollupOperator.cpp. mask / sortingKeys / sortingOrders / distinct are all
    // left default-empty; RollupOperator::makeAggregateInfos VELOX_CHECKs that
    // (RollupOperator.cpp:174-178).
    aggregates.emplace_back(core::AggregationNode::Aggregate{
        call, rawInputTypes, /*mask=*/nullptr, /*sortingKeys=*/{}, /*sortingOrders=*/{}});
  }

  return std::make_shared<RollupNode>(
      nextPlanNodeId(),
      groupingKeys,
      std::move(levels),
      childAgg->aggregateNames(),
      std::move(aggregates),
      childNode);
  // Note: processEmit(expandRel.common(), ...) still applies if the rel carries
  // an emit, exactly as the ExpandNode path does.
}

#endif // Illustrative

} // namespace gluten
