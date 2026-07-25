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
#include "RollupOperator.h"

#include <algorithm>
#include <exception>
#include <limits>

#include <folly/ScopeGuard.h>

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/VectorHasher.h"
#include "velox/type/Variant.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;

namespace gluten {

namespace {
constexpr int32_t kBypassLevel = 0;

/// True when extracting a column of this type out of a RowContainer copies a
/// bounded, per-row-constant number of bytes into the result vector — i.e. the
/// extraction cost cannot scale with data the container holds out of line in
/// its HashStringAllocator.
///
/// Fixed-width primitives qualify trivially (RowContainer::extractValuesNoNulls
/// writes one word per row, RowContainer.h:1198-1229). ROW qualifies iff all of
/// its children do: extraction descends into the children and allocates one
/// fixed-width buffer per leaf, which is what keeps avg's ROW(DOUBLE, BIGINT)
/// intermediate on the fast path. VARCHAR/VARBINARY/ARRAY/MAP never qualify —
/// their extraction path memcpys out-of-line bytes into freshly allocated
/// vector buffers (FlatVector.cpp:113-121 for strings, ContainerRowSerde for
/// the complex kinds).
///
/// Deliberately conservative: a false negative only costs Phase-2 reclaim
/// (Phase 1 still runs), whereas a false positive risks failing the query.
bool isExtractBounded(const TypePtr& type) {
  if (type->kind() == TypeKind::ROW) {
    for (auto i = 0; i < type->size(); ++i) {
      if (!isExtractBounded(type->childAt(i))) {
        return false;
      }
    }
    return true;
  }
  return type->isFixedWidth();
}
} // namespace

RollupOperator::RollupOperator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const RollupNodePtr& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "Rollup",
          // No spill: this operator emits partial state and flushes instead.
          // See DESIGN.md §4.
          std::nullopt),
      node_{node},
      numKeys_{static_cast<int32_t>(node->groupingKeys().size())},
      numAggregates_{static_cast<int32_t>(node->aggregates().size())},
      maxPartialAggregationMemoryUsage_{static_cast<int64_t>(
          driverCtx->queryConfig().maxPartialAggregationMemoryUsage())},
      maxExtendedPartialAggregationMemoryUsage_{static_cast<int64_t>(
          driverCtx->queryConfig().maxExtendedPartialAggregationMemoryUsage())},
      abandonPartialAggregationMinRows_{
          driverCtx->queryConfig().abandonPartialAggregationMinRows()},
      abandonPartialAggregationMinPct_{
          driverCtx->queryConfig().abandonPartialAggregationMinPct()},
      // See the derivation comment on findFullLevel().
      operatorBudgetCeilingBytes_{maxExtendedPartialAggregationMemoryUsage_} {
  // Nothing that allocates from pool() may happen here (Operator.h:207-215).
}

void RollupOperator::initialize() {
  Operator::initialize();

  const auto& nodeLevels = node_->levels();
  const auto& inputType = node_->inputType();

  // Precondition: the producer must hand us ROW(k1..kn, acc1..accm) in
  // hierarchy order. The JVM rule owns this projection; if it drifts we want a
  // loud failure, not silently wrong keys.
  VELOX_CHECK_EQ(
      inputType->size(),
      numKeys_ + numAggregates_,
      "Rollup input must be exactly the rollup keys followed by the aggregate states");
  for (auto i = 0; i < numKeys_; ++i) {
    VELOX_CHECK_EQ(
        inputType->nameOf(i),
        node_->groupingKeys()[i]->name(),
        "Rollup key {} is not at input channel {}",
        node_->groupingKeys()[i]->name(),
        i);
  }

  levels_.resize(nodeLevels.size());
  for (auto i = 0; i < nodeLevels.size(); ++i) {
    auto& level = levels_[i];
    level.numKeys = nodeLevels[i].numKeys;
    level.gid = nodeLevels[i].gid;
    level.keyIsNull = nodeLevels[i].keyIsNull;
    level.maxPartialBytes = maxPartialAggregationMemoryUsage_;
    level.naturalType =
        (i == kBypassLevel) ? inputType : makeNaturalType(level.numKeys);
  }

  // Level 0 is the bypass lane and gets no table: rows arriving from the child
  // partial aggregate are already grouped at the finest grain, so we tag and
  // forward them. This is the single biggest table we never build.
  for (auto i = 1; i < levels_.size(); ++i) {
    levels_[i].aggregates = makeAggregateInfos(i);

    // Decide, before the infos are moved into the GroupingSet, whether
    // reclaim() may run its Phase-2 cascade drain. The hazard is that a drain's
    // parked output approaches the size of the container being freed, because
    // GroupingSet::extractGroups (GroupingSet.cpp:847-888) copies out of the
    // RowContainer into fresh vector buffers. Phase 2 in that shape risks
    // tripping VELOX_CHECK_GE(reclaimedBytes, 0) (Operator.cpp:788-795), which
    // fails the QUERY rather than just the reclaim — an unacceptable trade for
    // memory we can also get from the always-safe Phase 1.
    //
    // The TYPES of the extracted row, not the accumulator flags, are the
    // load-bearing signal. Two reasons the flags alone are not enough:
    //
    //  1. extractGroups extracts the GROUPING KEYS first (GroupingSet.cpp:
    //     856-863) via RowContainer::extractColumn -> extractString, and
    //     FlatVector<StringView>::set memcpys every non-inline string into a
    //     fresh buffer (FlatVector.cpp:113-121). A rollup on VARCHAR keys with
    //     only count()/sum() aggregates has no variable-width ACCUMULATOR at
    //     all, yet the parked batch still allocates the full key bytes.
    //  2. isFixedSize() is not a reliable proxy for "the accumulator holds no
    //     out-of-line bytes". min(varchar)/max(varchar) store a
    //     SingleValueAccumulator, i.e. an unbounded HashStringAllocator
    //     position, and MinMaxAggregateBase.cpp never overrides isFixedSize()
    //     — so it reports true (Aggregate.h:83 default) with
    //     accumulatorUsesExternalMemory() false (Aggregate.h:71 default). The
    //     original predicate pair admitted exactly the shape it existed to
    //     exclude.
    //
    // levels_[i].naturalType is precisely the row extractGroups produces for
    // this level: ROW(k1..kL, acc1..accm) with the resolved intermediate types.
    // Testing it covers keys and accumulators in one pass. ROW is treated
    // recursively rather than via Type::isFixedWidth() (which is false for any
    // ROW) so that avg's ROW(DOUBLE, BIGINT) intermediate stays on the fast
    // path; only VARCHAR/VARBINARY/ARRAY/MAP make a level unsafe.
    //
    // The two accumulator flags are kept as an additional disqualifier: they
    // catch functions whose intermediate type is fixed-width but whose
    // accumulator still tracks a variable footprint or external memory.
    if (!isExtractBounded(levels_[i].naturalType)) {
      phase2ReclaimSafe_ = false;
    }
    for (const auto& info : levels_[i].aggregates) {
      // makeAggregateInfos() constructs the instance with exec::Aggregate::create
      // and makeGroupingSet(i) below is what std::moves the vector away, so
      // reading info.function here is both non-null and correctly ordered. The
      // null test is belt-and-braces, not load-bearing.
      VELOX_CHECK_NOT_NULL(
          info.function, "Rollup aggregate info has no Aggregate instance");
      if (!info.function->isFixedSize() ||
          info.function->accumulatorUsesExternalMemory()) {
        phase2ReclaimSafe_ = false;
        break;
      }
    }

    makeGroupingSet(i);
  }

  addRuntimeStat("rollup.numLevels", RuntimeCounter(levels_.size()));
  addRuntimeStat(
      "rollup.phase2ReclaimSafe", RuntimeCounter(phase2ReclaimSafe_ ? 1 : 0));
}

RowTypePtr RollupOperator::makeNaturalType(int32_t numKeys) const {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(numKeys + numAggregates_);
  types.reserve(numKeys + numAggregates_);

  const auto& inputType = node_->inputType();
  for (auto i = 0; i < numKeys; ++i) {
    names.push_back(inputType->nameOf(i));
    types.push_back(inputType->childAt(i));
  }
  for (auto i = 0; i < numAggregates_; ++i) {
    names.push_back(node_->aggregateNames()[i]);
    // Intermediate type of aggregate i, resolved from the registry rather than
    // trusted from the call's declared return type — exec::resolveIntermediateType
    // is public (velox/exec/AggregateFunctionRegistry.h:58). Using the same
    // source of truth here and in makeAggregateInfos is what guarantees the
    // cascade pipe types line up.
    types.push_back(exec::resolveIntermediateType(
        node_->aggregates()[i].call->name(), node_->aggregates()[i].rawInputTypes));
  }
  return ROW(std::move(names), std::move(types));
}

std::vector<exec::AggregateInfo> RollupOperator::makeAggregateInfos(int32_t index) {
  auto& level = levels_[index];
  // Level `index` consumes the natural output of level `index - 1`.
  const auto& levelInputType = levels_[index - 1].naturalType;
  const auto numLevelKeys = level.numKeys;

  // NOTE (was: build a synthetic kIntermediate AggregationNode and call
  // exec::toAggregateInfo). That does not work: toAggregateInfo's first act is
  //     const auto& inputType = aggregationNode.sources()[0]->outputType();
  // (AggregateInfo.cpp:49), so passing source = nullptr is an unconditional
  // null dereference, and there is no cheap real node to pass whose outputType
  // is this level's input type.
  //
  // Instead we build AggregateInfo by hand. This is not a workaround, it is the
  // in-tree pattern for a non-AggregationNode aggregation consumer:
  // ColumnStatsCollector::createAggregates (ColumnStatsCollector.cpp:78-121)
  // does exactly this and then feeds the result to a GroupingSet
  // (ColumnStatsCollector.cpp:123-152). It also removes the dependency on
  // toAggregateInfo's expressionEvaluator out-parameter entirely.
  //
  // resolveIntermediateType IS public — it is declared in
  // velox/exec/AggregateFunctionRegistry.h:58, not file-local to
  // AggregateInfo.cpp as an earlier revision of this draft claimed.
  const auto accBase = levels_[index - 1].numKeys;

  std::vector<exec::AggregateInfo> infos;
  infos.reserve(numAggregates_);
  for (auto i = 0; i < numAggregates_; ++i) {
    const auto& aggregate = node_->aggregates()[i];
    // BASE, unsuffixed function name — see the name contract on the RollupNode
    // constructor (RollupNode.h) and the flavour discussion in
    // RollupTranslator::toRollupPlan. `name` + rawInputTypes is what identifies
    // the function (Aggregate.h:343-346); the merge semantics come from the
    // GroupingSet flags below, not from a "_merge" companion. A companion name
    // would break the very next line, because resolveIntermediateType would be
    // asked to match raw argument types against a companion signature declared
    // over the intermediate type (avg: raw DOUBLE vs ROW(DOUBLE, BIGINT)).
    const auto& name = aggregate.call->name();

    // v1 admits none of these; the planner falls back. Check rather than assume,
    // because a planner bug here would silently produce wrong results.
    VELOX_CHECK_NULL(aggregate.mask, "Rollup does not support masked aggregates");
    VELOX_CHECK(
        aggregate.sortingKeys.empty(),
        "Rollup does not support order-sensitive aggregates");
    VELOX_CHECK(!aggregate.distinct, "Rollup does not support DISTINCT aggregates");

    exec::AggregateInfo info;
    // The single input of a merge level is the accumulator column, which sits at
    // (number of keys of the level below) + aggregate index in this level's
    // input.
    info.inputs = {static_cast<column_index_t>(accBase + i)};
    info.constantInputs = {nullptr};
    info.mask = std::nullopt;
    info.intermediateType = exec::resolveIntermediateType(name, aggregate.rawInputTypes);

    // Output channel within naturalType(numLevelKeys) = ROW(k1..kL, acc1..accm).
    info.output = static_cast<column_index_t>(numLevelKeys + i);

    // kPartial => the function's result type is the intermediate type, i.e.
    // extractAccumulators on the way out. Combined with GroupingSet's
    // isRawInput = false this gives addIntermediateResults on the way in:
    // states in, states out. rawInputTypes are passed unchanged because
    // Aggregate::create always resolves the signature from raw argument types
    // regardless of step.
    info.function = exec::Aggregate::create(
        name,
        core::AggregationNode::Step::kPartial,
        aggregate.rawInputTypes,
        info.intermediateType,
        operatorCtx_->driverCtx()->queryConfig());

    // Consistency check: the level input column we bound must actually be the
    // intermediate type of this aggregate.
    VELOX_CHECK(
        levelInputType->childAt(accBase + i)->equivalent(*info.intermediateType),
        "Rollup level {} aggregate {} expects intermediate type {} but input channel {} is {}",
        index,
        name,
        info.intermediateType->toString(),
        accBase + i,
        levelInputType->childAt(accBase + i)->toString());

    infos.push_back(std::move(info));
  }
  return infos;
}

void RollupOperator::makeGroupingSet(int32_t index) {
  auto& level = levels_[index];
  const auto& levelInputType = levels_[index - 1].naturalType;

  std::vector<column_index_t> keyChannels;
  keyChannels.reserve(level.numKeys);
  for (auto i = 0; i < level.numKeys; ++i) {
    keyChannels.push_back(i);
  }

  // Empty hashers => global aggregation (GroupingSet.cpp:62). That is the
  // grand-total level, and it needs no special casing anywhere else.
  auto hashers = exec::createVectorHashers(levelInputType, keyChannels);

  level.groupingSet = std::make_unique<exec::GroupingSet>(
      levelInputType,
      std::move(hashers),
      /*preGroupedKeys=*/std::vector<column_index_t>{},
      /*groupingKeyOutputProjections=*/std::vector<column_index_t>{},
      std::move(level.aggregates),
      // Rollup keys may legitimately be NULL in the data; never drop those rows.
      /*ignoreNullKeys=*/false,
      // isPartial: emit accumulators, not final values, and enable the
      // isPartialFull() budget check.
      /*isPartial=*/true,
      // isRawInput: false — every level merges states, never raw values.
      /*isRawInput=*/false,
      /*globalGroupingSets=*/std::vector<vector_size_t>{},
      /*groupIdChannel=*/std::nullopt,
      // No spill by design (DESIGN.md §4); we flush instead.
      /*spillConfig=*/nullptr,
      // Both of these are INHERITED Operator members, not our own — see the
      // long note where the shadowing declarations used to be in
      // RollupOperator.h. nonReclaimableSection_ must be the base's, because
      // that is the one Operator::MemoryReclaimer::reclaim tests.
      &nonReclaimableSection_,
      &operatorCtx_->driverCtx()->queryConfig(),
      pool(),
      spillStats_.get());
}

bool RollupOperator::needsInput() const {
  return state_ == State::kAccumulating && outputQueue_.empty();
}

void RollupOperator::addInput(RowVectorPtr input) {
  VELOX_CHECK(state_ == State::kAccumulating);

  // FIXED (adversarial review) — a zero-row batch must be dropped WHOLESALE,
  // not merely kept from arming receivedInput_. The previous revision fell
  // through and pushed project(kBypassLevel, input) onto outputQueue_, so
  // getOutput() would hand the Driver a non-null RowVector of size 0. That
  // violates the operator contract stated verbatim in Values.cpp:40-41
  // ("Operator::getOutput is expected to return nullptr or a non-empty
  // vector"), which is why Values itself filters empty vectors out of its own
  // value list. Driver::runInternal only tests `if (intermediateResult)` — it
  // does NOT re-check size (Driver.cpp:695, :705) — so the empty vector would
  // propagate downstream and be counted as an output batch by
  // addOutputVector(). The feedLevel() fan-out below is likewise pure overhead
  // on zero rows.
  //   Note the early return is ABOVE the assignment, so receivedInput_ means
  // "addInput() was called with at least one ROW", not "addInput() was called".
  // That is the meaning noMoreInput() needs (a zero-row call feeds nothing to
  // any level, so it must not suppress the empty-input guard), and it matches
  // the declaration comment in RollupOperator.h. Keeping the flag explicit
  // rather than deriving it: the equivalence is not local.
  if (input->size() == 0) {
    return;
  }
  receivedInput_ = true;

  // (a) Bypass lane: the finest level is already grouped, so we tag it with
  // gid_n and emit it without touching a hash table.
  bypassRows_ += input->size();
  outputQueue_.push_back(project(kBypassLevel, input));

  // (b) Feed the same batch into the next coarser level. If that level has been
  // abandoned, feedLevel returns a pass-through that must continue down the
  // chain.
  RowVectorPtr carry = input;
  for (auto i = 1; i < levels_.size() && carry != nullptr; ++i) {
    auto passThrough = feedLevel(i, carry);
    if (passThrough == nullptr) {
      carry = nullptr;
    } else {
      // An abandoned level emits its own output rows too.
      outputQueue_.push_back(project(i, passThrough));
      carry = passThrough;
    }
  }

  // (c) Memory pressure: drain the finest over-budget level. Draining the
  // finest first is deliberate — it is the largest table and its output is the
  // input to everything coarser, so one drain does the most good.
  //
  // FIXED (adversarial review): this used to assign state_/drainLevel_
  // unconditionally. reclaim() can be entered re-entrantly from an allocation
  // made INSIDE the fan-out loop above (GroupingSet::addInput ->
  // ensureInputFits -> arbitration), and it may leave a drain in flight on some
  // level with a half-consumed RowContainerIterator. Retargeting drainLevel_ at
  // that point strands the remaining rows of the level being drained: level
  // i's drain would then feed NEW rows into the stranded level's table, merging
  // them into groups whose rows the abandoned iterator has already listed, and
  // those updates would never be emitted. That is silent data loss (invariant
  // I2). Only arm a new drain when nothing is in flight.
  if (state_ != State::kAccumulating) {
    return;
  }
  const auto full = findFullLevel();
  if (full >= 0) {
    state_ = State::kPressureDrain;
    drainLevel_ = full;
    ++cascadeFlushes_;
  }
}

RowVectorPtr RollupOperator::feedLevel(int32_t index, const RowVectorPtr& natural) {
  auto& level = levels_[index];
  level.numInputRows += natural->size();
  level.totalInputRows += natural->size();

  if (level.abandoned) {
    // BUG FIX: an abandoned level may NOT forward `natural` verbatim. `natural`
    // is shaped as levels_[index - 1].naturalType, i.e. ROW(k1..k[L+1], accs),
    // whereas this level's own natural shape is ROW(k1..k[L], accs). Forwarding
    // unchanged would (a) make project(index, ...) read the accumulator columns
    // one channel too far to the right and (b) hand levels_[index + 1]'s
    // GroupingSet a vector whose type is not the type it was constructed over.
    // Dropping the now-dead key column is pure column reshuffling — no data is
    // copied.
    return dropDeadKeyColumn(index, natural);
  }

  level.groupingSet->addInput(natural, /*mayPushdown=*/false);

  // Abandon valve, per level (HashAggregation.cpp:183-187 applied locally). An
  // abandoned merge level is a pure projection: since isRawInput is false, the
  // identity on state is a correct "aggregation". Its volume shifts to the next
  // coarser level, which is exactly what the unfused plan does anyway — so the
  // fused operator degrades to today's behaviour rather than off a cliff.
  if (shouldAbandon(level)) {
    level.abandoned = true;
    // The table still holds real state; it must be drained before we start
    // passing rows through, otherwise we would emit them out of order with
    // respect to their own accumulated groups. Both orders are correct (the
    // final aggregation merges), so we simply mark it and let the next drain
    // free the table.
    //
    // RESOLVED (was: force an immediate drain here, or defer?). DEFERRED.
    // findFullLevel() clause (2) now reports any abandoned level that still owns
    // a GroupingSet, so the very next pressure check picks it up and
    // advanceDrain() frees the table outright (freeTable = true, and the
    // GroupingSet is dropped). Rationale for not draining inline: feedLevel() is
    // called from the middle of addInput()'s fan-out loop and from the middle of
    // advanceDrain()'s cascade loop, so an inline drain would have to re-enter
    // the drain path while a RowContainerIterator on a *different* level is
    // half-consumed — precisely the class of bug the pendingFinalCascade_ fix
    // exists to prevent. Deferring keeps a single drain in flight at a time,
    // which is the invariant the whole state machine is built on.
    addRuntimeStat(
        fmt::format("rollup.L{}.abandoned", level.numKeys), RuntimeCounter(1));
  }
  return nullptr;
}

RowVectorPtr RollupOperator::dropDeadKeyColumn(
    int32_t index,
    const RowVectorPtr& natural) const {
  const auto& level = levels_[index];
  // Inlined rather than bound to a local: VELOX_DCHECK_EQ compiles out in
  // release builds, and a local used only here trips -Werror,-Wunused-variable.
  VELOX_DCHECK_EQ(
      natural->type()->size(), levels_[index - 1].naturalType->size());

  std::vector<VectorPtr> children;
  children.reserve(level.numKeys + numAggregates_);
  for (auto j = 0; j < level.numKeys; ++j) {
    children.push_back(natural->childAt(j));
  }
  const auto accBase = levels_[index - 1].numKeys;
  for (auto a = 0; a < numAggregates_; ++a) {
    children.push_back(natural->childAt(accBase + a));
  }
  return std::make_shared<RowVector>(
      pool(),
      level.naturalType,
      /*nulls=*/nullptr,
      natural->size(),
      std::move(children));
}

bool RollupOperator::shouldAbandon(const LevelState& level) const {
  // Mirrors HashAggregation's guard: the abandon valve is meaningless for a
  // global aggregation, and HashAggregation::abandonPartialAggregationEarly
  // VELOX_CHECKs !isGlobal_ (HashAggregation.cpp:183-187). The grand-total level
  // has numKeys == 0 and holds exactly one group, so there is nothing to give up
  // on.
  if (level.numKeys == 0) {
    return false;
  }
  if (level.groupingSet == nullptr || level.numInputRows <= abandonPartialAggregationMinRows_) {
    return false;
  }
  const auto numOutput = level.groupingSet->numDistinct();
  return 100 * numOutput / level.numInputRows >= abandonPartialAggregationMinPct_;
}

int64_t RollupOperator::totalLevelBytes() const {
  int64_t total{0};
  for (const auto& level : levels_) {
    if (level.groupingSet != nullptr) {
      total += static_cast<int64_t>(level.groupingSet->allocatedBytes());
    }
  }
  return total;
}

int32_t RollupOperator::findFullLevel() const {
  // ---------------------------------------------------------------------
  // RESOLVED (was OPEN QUESTION 1 / DESIGN.md §3.4) — budget partitioning.
  // ---------------------------------------------------------------------
  // Option (c): every level keeps the FULL per-level budget, and the operator
  // additionally enforces a ceiling on the sum. (b), splitting the budget n
  // ways, was rejected for the reason §3.4 gives: coarse levels are tiny, so an
  // even split starves the one or two levels that actually hold data while
  // handing megabytes to a table with 30 groups in it.
  //
  // DERIVATION OF THE CEILING — no new config key.
  //   max_partial_aggregation_memory          default 1<<24 = 16 MB
  //     (QueryConfig.h:343-349)
  //   max_extended_partial_aggregation_memory default 1<<26 = 64 MB
  //     (QueryConfig.h:351-357)
  // The extended value is already the sanctioned answer to "how much may ONE
  // partial aggregation operator's tables occupy, at most, once it has earned
  // the right to grow" — it is the cap HashAggregation's own
  // maybeIncreasePartialAggregationMemoryUsage doubles maxPartialBytes up to
  // (HashAggregation.cpp:313-348), which is exactly what maybeGrowBudget mirrors
  // here. This operator REPLACES one partial aggregation (the PartialMerge of
  // the three-operator shape, DESIGN.md §2.2), so the whole fused operator —
  // all n live level tables together — should live inside the same envelope a
  // single partial aggregation is allowed to reach. Hence
  // ceiling = max_extended_partial_aggregation_memory, with no new knob to
  // document, no new default to defend, and a strict improvement on the status
  // quo ante (n * 16 MB unbounded by anything).
  //
  // Interaction with maybeGrowBudget: a level may still double its own
  // maxPartialBytes up to the extended value, but the moment the SUM crosses the
  // ceiling the finest non-empty level is drained regardless. So per-level
  // growth stays available to the level that needs it while the operator total
  // stays bounded. The two triggers are checked in priority order below.

  // (1) Primary trigger, unchanged: a single level over its own budget.
  //     Finest-first scan — levels_ is already ordered finest to coarsest, and
  //     draining the finest does the most good (largest table, and its output is
  //     the input to everything coarser).
  for (auto i = 1; i < levels_.size(); ++i) {
    const auto& level = levels_[i];
    if (level.groupingSet != nullptr &&
        level.groupingSet->isPartialFull(level.maxPartialBytes)) {
      return i;
    }
  }

  // (2) Resolves the TODO at feedLevel(): a level that was abandoned mid-batch
  //     still holds real accumulated state in a table it will never add to
  //     again. DECISION: deferred, not an immediate forced drain inside
  //     feedLevel(). Immediate is easier to reason about but stalls addInput at
  //     an arbitrary point in the middle of a batch fan-out; deferred costs
  //     nothing in correctness (both emission orders are legal — the
  //     post-shuffle final aggregation merges them, DESIGN.md §4.0) and reuses
  //     the one drain path that is already tested. Reporting it here rather than
  //     waiting for the end-of-input cascade means the table is freed promptly:
  //     advanceDrain() resets an abandoned level with freeTable = true and drops
  //     the GroupingSet entirely (RollupOperator.cpp:456-463).
  for (auto i = 1; i < levels_.size(); ++i) {
    const auto& level = levels_[i];
    if (level.abandoned && level.groupingSet != nullptr) {
      return i;
    }
  }

  // (3) Operator-wide ceiling. Drain the finest level that actually holds
  //     something; an empty table contributes no bytes worth reclaiming and
  //     draining it would just churn.
  if (totalLevelBytes() > operatorBudgetCeilingBytes_) {
    for (auto i = 1; i < levels_.size(); ++i) {
      const auto& level = levels_[i];
      if (level.groupingSet != nullptr && level.groupingSet->numRows() > 0) {
        return i;
      }
    }
  }

  return -1;
}

RowVectorPtr RollupOperator::getOutput() {
  // 1. Bypass rows and any parked flush output first — cheapest, and it keeps
  //    the downstream pipeline busy while tables fill.
  if (!outputQueue_.empty()) {
    auto result = std::move(outputQueue_.front());
    outputQueue_.pop_front();
    return result;
  }

  // 2. A drain in progress, either pressure-triggered or the end-of-input
  //    cascade.
  if (state_ == State::kPressureDrain || state_ == State::kFinalCascade) {
    return advanceDrain();
  }

  return nullptr;
}

RowVectorPtr RollupOperator::advanceDrain() {
  VELOX_CHECK_GE(drainLevel_, 1);
  auto& level = levels_[drainLevel_];

  // BUG FIX: GroupingSet::getOutput does NOT allocate `result`. Both the hash
  // path (extractGroups -> result->resize / result->childAt, GroupingSet.cpp:847-888)
  // and the global path (getGlobalAggregationOutput -> result->childAt,
  // GroupingSet.cpp:671-707) write into a caller-supplied RowVector. Passing a
  // null RowVectorPtr, as an earlier revision of this draft did, is an
  // unconditional null dereference. HashAggregation calls prepareOutput() first
  // (HashAggregation.cpp:275-284, :396); this is the same thing, per level,
  // reusing the vector across batches when it is uniquely owned.
  const auto maxOutputRows = (level.numKeys == 0) ? 1 : outputBatchRows();
  if (level.output != nullptr) {
    VectorPtr reusable = std::move(level.output);
    BaseVector::prepareForReuse(reusable, maxOutputRows);
    level.output = std::static_pointer_cast<RowVector>(reusable);
  } else {
    level.output = std::static_pointer_cast<RowVector>(
        BaseVector::create(level.naturalType, maxOutputRows, pool()));
  }
  // FIXED (adversarial review): this used to bind a COPY of level.output and
  // pass that to getOutput. GroupingSet::getOutput takes `RowVectorPtr& result`
  // and some of its branches reassign it, so a copy silently discarded the
  // reassignment and left level.output pointing at a stale vector. Pass the
  // member itself and read it back afterwards.
  //
  // Byte cap: preferredOutputBatchBytes() is uint64_t (QueryConfig.h:478-484,
  // default 10 MiB) but GroupingSet::getOutput's maxOutputBytes parameter is
  // int32_t (GroupingSet.h:84-88). Clamp explicitly rather than rely on an
  // implicit narrowing conversion, which is a -Wconversion diagnostic and would
  // wrap to a negative cap if the config were ever raised past 2 GiB.
  const auto maxOutputBytes = static_cast<int32_t>(std::min<uint64_t>(
      operatorCtx_->driverCtx()->queryConfig().preferredOutputBatchBytes(),
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max())));

  const bool hasMore = level.groupingSet != nullptr &&
      level.groupingSet->getOutput(
          maxOutputRows, maxOutputBytes, level.iterator, level.output);
  RowVectorPtr natural = level.output;

  if (hasMore && natural != nullptr && natural->size() > 0) {
    level.numOutputRows += natural->size();
    level.totalOutputRows += natural->size();

    // Each drained batch is used twice: once as operator output (with this
    // level's gid and null mask), once as input to the next coarser level. This
    // is the cascade — each state flows down the hierarchy exactly once.
    auto output = project(drainLevel_, natural);

    // OPEN (adversarial review) — MID-CASCADE BUDGET OVERSHOOT. The fan-out
    // below calls GroupingSet::addInput on every coarser level, i.e. a drain
    // ALLOCATES into levels_[drainLevel_ + 1 .. n] while it frees
    // levels_[drainLevel_]. findFullLevel() is not consulted again until the
    // whole drain finishes (addInput is the only caller, and needsInput() is
    // false throughout), so a coarser level can sit arbitrarily far over its own
    // maxPartialBytes for the duration of a long final cascade. Bounded in
    // practice by the reduction ratio (a coarser level holds strictly fewer
    // groups), but not bounded by anything the code enforces. This is also the
    // reason reclaim()'s Phase 2 cannot be called net non-allocating by
    // construction: see the RESIDUAL RISK block on reclaim().
    if (drainLevel_ + 1 < static_cast<int32_t>(levels_.size())) {
      RowVectorPtr carry = natural;
      for (auto i = drainLevel_ + 1; i < levels_.size() && carry != nullptr; ++i) {
        auto passThrough = feedLevel(i, carry);
        if (passThrough == nullptr) {
          carry = nullptr;
        } else {
          outputQueue_.push_back(project(i, passThrough));
          carry = passThrough;
        }
      }
    }
    return output;
  }

  // This level is exhausted.
  ++level.flushTimes;
  recordLevelStats(level, drainLevel_);
  maybeGrowBudget(level);
  if (level.groupingSet != nullptr) {
    // freeTable = false mirrors HashAggregation::resetPartialOutputIfNeed
    // (HashAggregation.cpp:286-311): keep the allocation, drop the contents.
    // For an abandoned level we do want the memory back — and so do we when the
    // arbitrator is the one asking (reclaiming_), where retaining the allocation
    // would defeat the entire point of the call.
    // GroupingSet::resetTable forwards to HashTable::clear(freeTable)
    // (GroupingSet.cpp:914-918).
    //
    // FIXED (adversarial review) — third disjunct. resetTable(false) RETAINS
    // the RowContainer/HashTable allocation, so allocatedBytes() (and therefore
    // totalLevelBytes()) does NOT drop when a level is drained. That made
    // findFullLevel() clause (3) self-perpetuating: once the operator-wide sum
    // crossed operatorBudgetCeilingBytes_ it stayed across it forever, so every
    // subsequent addInput() armed another ceiling drain of the finest non-empty
    // level. Not a deadlock (clause 3 requires numRows() > 0, so it terminates
    // within a call), but a permanent degeneration to "flush after every
    // batch" — the exact performance cliff this operator exists to avoid. When
    // the ceiling is the thing being violated, give the pages back.
    const bool freeTable = level.abandoned || reclaiming_ ||
        totalLevelBytes() > operatorBudgetCeilingBytes_;
    level.groupingSet->resetTable(freeTable);
    if (level.abandoned) {
      level.groupingSet.reset();
    } else if (!freeTable &&
               level.groupingSet->isPartialFull(level.maxPartialBytes)) {
      // FIXED (adversarial review) — ZERO-ROW-BUT-STILL-FULL LEVEL.
      // resetTable(false) -> HashTable::clear(false) frees the RowContainer
      // arena (RowContainer::clear -> AllocationPool::clear, which releases
      // its allocations) but deliberately RETAINS the slot array, memset to
      // zero (HashTable.cpp:757-766). allocatedBytes() therefore does not fall
      // to zero: it falls to sizeof(char*) * capacity_ (HashTable.h:665-668).
      //
      // isPartialFull() is a pure function of allocatedBytes(), with NO row
      // predicate (GroupingSet.cpp:930-950). So a level can be reported full
      // while holding zero rows, and draining it again is a no-op that cannot
      // clear the condition. That is reachable: in kArray mode capacity_ is
      // driven by the key value RANGE, not by the row count (up to
      // kArrayHashMaxSize = 2<<20 slots = 16 MB of slot array,
      // HashTable.h:143), so any max_partial_aggregation_memory below 16 MB
      // leaves the emptied table permanently "full". The self-healing rehash
      // inside isPartialFull does not rescue us on the repeat: its
      // decideHashMode(0, .., disableRangeArrayHash=true) call early-returns
      // once disableRangeArrayHash_ has been set, which the FIRST such call
      // sets (HashTable.cpp:1763-1767).
      //
      // When the level is still over its own budget with nothing in it, the
      // retained slot array IS the problem. Give it back.
      level.groupingSet->resetTable(/*freeTable=*/true);
    }
  }
  level.iterator = exec::RowContainerIterator{};
  level.numInputRows = 0;
  level.numOutputRows = 0;

  if (state_ == State::kFinalCascade) {
    ++drainLevel_;
    if (drainLevel_ >= static_cast<int32_t>(levels_.size())) {
      state_ = State::kDone;
      drainLevel_ = -1;
      addRuntimeStat("rollup.bypassRows", RuntimeCounter(bypassRows_));
      addRuntimeStat("rollup.cascadeFlushes", RuntimeCounter(cascadeFlushes_));
    }
  } else if (pendingFinalCascade_) {
    // noMoreInput() landed mid-drain; now that the drain has finished, run the
    // full cascade from the finest table so nothing is left behind.
    pendingFinalCascade_ = false;
    state_ = State::kFinalCascade;
    drainLevel_ = 1;
  } else {
    // Pressure drain finished.
    //
    // FIXED (adversarial review) — MID-CASCADE BUDGET OVERSHOOT. The fan-out
    // above allocates into every coarser level while this one drains, so the
    // drain itself can push another level over budget. Previously we returned
    // straight to kAccumulating and waited for the next addInput() to notice,
    // which meant accepting a fresh batch into a structure already known to be
    // over budget — and, with the operator-wide ceiling, doing so repeatedly.
    // Re-check here instead and chain directly into the next drain, so the
    // operator never takes input while a level is over its limit.
    //
    // FIXED (adversarial review) — THE ORIGINAL TERMINATION ARGUMENT WAS
    // WRONG, AND THE CHAIN COULD LIVELOCK. It read: "each iteration drains a
    // level with numRows() > 0 down to zero rows, and findFullLevel() only
    // returns levels holding rows." Only clause (3) of findFullLevel() tests
    // numRows(). Clause (1) tests isPartialFull(), which is a pure function of
    // allocatedBytes() and has no row predicate at all, and clause (2) tests
    // `abandoned`. So findFullLevel() CAN return the level we just emptied.
    // When it did, the chain re-armed a drain on that level, advanceDrain()
    // took the exhausted path again on the first call, and re-armed again —
    // forever. getOutput() returns nullptr, needsInput() is false (state_ is
    // not kAccumulating), isFinished() is false and isBlocked() is
    // kNotBlocked, so Driver::runInternal spins the pipeline at 100% CPU. It
    // is worse than a stall: because the Driver only delivers noMoreInput()
    // to an operator inside the `if (needsInput)` branch (Driver.cpp:678-830),
    // an operator wedged with needsInput() == false never receives
    // end-of-input and the query never completes.
    //
    // The zero-row-but-still-full case is now defused at the source (see the
    // resetTable(true) above), but a termination proof must not depend on that
    // holding for every future clause of findFullLevel(). So the chain is also
    // bounded structurally: only chain STRICTLY COARSER than the level just
    // drained. That is sound, not merely conservative — a drain feeds
    // exclusively into levels > drainLevel_, so a drain cannot newly fill a
    // finer level; any finer level findFullLevel() reports was already full
    // when this drain was armed, and since findFullLevel() scans finest-first
    // it would have been chosen then. A report of next <= drainLevel_ is
    // therefore either the degenerate no-progress case or a stale condition,
    // and in both we fall back to kAccumulating — exactly the pre-chain
    // behaviour, which makes progress because addInput() consumes a batch on
    // every pass. drainLevel_ now strictly increases across a chain and is
    // bounded above by levels_.size(), so the chain is finite by construction.
    const auto next = findFullLevel();
    if (next > drainLevel_) {
      ++cascadeFlushes_;
      state_ = State::kPressureDrain;
      drainLevel_ = next;
    } else {
      state_ = State::kAccumulating;
      drainLevel_ = -1;
    }
  }
  return nullptr;
}

void RollupOperator::maybeGrowBudget(LevelState& level) {
  if (level.groupingSet == nullptr || level.abandoned) {
    return;
  }
  // HARD REQUIREMENT of the reclaim contract, not a policy preference: the body
  // below calls pool()->maybeReserve(), which raises this pool's RESERVATION,
  // and reclaimedBytes is computed as reservedBytesBefore - reservedBytesAfter
  // (ScopedReclaimedBytesRecorder, MemoryArbitrator.cpp:527-557). A single
  // successful maybeReserve() inside reclaim() can therefore turn the delta
  // negative on its own and trip the VELOX_CHECK_GE at Operator.cpp:788-795,
  // failing the query. Growing a budget while the arbitrator is reclaiming is
  // also backwards on the merits.
  if (reclaiming_) {
    return;
  }
  // Do not grow past the operator-wide ceiling either (DESIGN.md §3.4 option
  // (c), see findFullLevel()): the sum is what the ceiling bounds, so a level
  // that is already the sole occupant of the envelope has nothing to gain.
  if (totalLevelBytes() >= operatorBudgetCeilingBytes_) {
    return;
  }
  if (level.maxPartialBytes >= maxExtendedPartialAggregationMemoryUsage_) {
    return;
  }
  const auto grown =
      std::min(level.maxPartialBytes * 2, maxExtendedPartialAggregationMemoryUsage_);
  const auto toReserve = grown - level.maxPartialBytes;
  // Same guard HashAggregation uses before extending the limit
  // (HashAggregation.cpp:313-348).
  if (pool()->maybeReserve(toReserve)) {
    level.maxPartialBytes = grown;
  }
}

void RollupOperator::noMoreInput() {
  // FIXED (adversarial review) — IDEMPOTENCE. Nothing in Operator or Driver
  // promises a single delivery; HashBuild::noMoreInput() guards with exactly
  // this test (HashBuild.cpp:788-793) and HashProbe/HashBuild also call their
  // own noMoreInput() internally. Today the Driver's only call site is gated on
  // nextOp->needsInput() (Driver.cpp:683, :763-778) and needsInput() here is
  // `state_ == kAccumulating`, which no transition ever re-enters after this
  // function runs — so a second delivery is currently unreachable. That is a
  // non-local argument resting on another file's control flow, and the failure
  // it protects against is severe rather than cosmetic: a second call landing
  // in kFinalCascade or kDone would fall through to the tail below and reset
  // drainLevel_ to 1, abandoning a half-consumed RowContainerIterator (the
  // exact bug pendingFinalCascade_ exists to prevent) or resurrecting a
  // finished operator into a second full cascade — which, because levels_[n] is
  // global, would emit a duplicate grand-total row and flip isFinished() back
  // to false. It would also double-count the two runtime stats recorded below.
  // One branch buys immunity from all of it.
  if (noMoreInput_) {
    return;
  }
  Operator::noMoreInput();
  // BUG FIX: noMoreInput() can arrive while a pressure drain is in flight —
  // needsInput() is false during kPressureDrain, and the Driver is free to
  // deliver no-more-input at that point. Overwriting drainLevel_ with 1 would
  // (a) abandon the half-consumed RowContainerIterator of the level being
  // drained and (b) leave that level's remaining rows to be re-listed later
  // against a container that has since taken new input. Instead we let the
  // in-flight drain finish and start the final cascade when it completes.
  //
  // GUARD ORDERING (verified, adversarial review). This branch precedes the
  // !receivedInput_ early exit below and therefore bypasses it. That is safe
  // only because kPressureDrain is unreachable with receivedInput_ == false,
  // and it is: every route into kPressureDrain requires rows to exist.
  //   - addInput()/advanceDrain() arm it from findFullLevel(), whose three
  //     clauses need isPartialFull() (bytes in a table), an abandoned level, or
  //     numRows() > 0. shouldAbandon() returns false unless
  //     numInputRows > abandonPartialAggregationMinRows_, which is false at
  //     zero rows for every non-negative setting of that knob.
  //   - reclaim()'s Phase 2 arms it only after findFullLevel() or a fallback
  //     scan that itself requires numRows() > 0; otherwise it returns quietly.
  //   - rows reach a level only through feedLevel(), called from addInput()
  //     (which now returns early on size 0) and from advanceDrain() (which
  //     needs a level that already holds rows).
  // If a future clause in findFullLevel() drops the "holds rows" predicate,
  // this ordering must be revisited: a spurious drain would route an
  // empty-input driver into the full cascade and re-emit the grand-total row
  // the guard below exists to suppress.
  if (state_ == State::kPressureDrain) {
    pendingFinalCascade_ = true;
    return;
  }
  // Close the cascade from the finest table (level 1) up to the grand total.
  // Level 0 needs nothing: everything it had was emitted by the bypass lane as
  // it arrived.
  //
  // FIXED (was the highest correctness risk in this file) — EMPTY INPUT USED TO
  // EMIT A SPURIOUS GRAND-TOTAL ROW, one per driver. The guard is the
  // `receivedInput_` early exit directly below; the analysis that motivates it
  // is kept because the failure mode is subtle and easy to reintroduce.
  //   The final cascade always reaches levels_[n], whose GroupingSet is global
  //   (empty hashers). GroupingSet::getOutput dispatches to
  //   getGlobalAggregationOutput (GroupingSet.cpp:810-814), which calls
  //   initializeGlobalAggregation() and unconditionally extracts one row the
  //   first time the iterator is fresh (GroupingSet.cpp:671-707) — it does not
  //   care whether any input ever arrived. The plan this operator replaces does
  //   NOT do that: a GROUPED kIntermediate AggregationNode over zero rows emits
  //   zero rows, and Velox only synthesises a default global row when
  //   globalGroupingSets/groupIdChannel are set (PlanNode.h:1157-1166), which
  //   we deliberately leave empty in makeGroupingSet().
  //   The operator runs PER DRIVER, per task. So a query whose rollup stage has
  //   D drivers, of which E receive no rows, pushes E extra identity rows into
  //   the shuffle. For SUM/COUNT the post-shuffle final aggregation absorbs
  //   them (identity elements), so the values stay right — but the OUTPUT
  //   CARDINALITY of the grand-total group is only right by luck, and for a
  //   non-identity-absorbing aggregate (or an empty-input query, where Spark's
  //   answer for ROLLUP is a single grand-total row, not D of them) it is
  //   wrong.
  //   THE SEMANTICS QUESTION IS SETTLED, and it settles the fix with it. Spark
  //   emits ZERO rows for a grouping-sets/ROLLUP query over empty input — it
  //   never synthesises a grand-total row, because spark_grouping_id is itself
  //   a grouping key, so the aggregation is never keyless. Verified against the
  //   approved golden output in this repo,
  //   gluten-ut/spark40/src/test/resources/backends-velox/sql-tests/results/
  //   postgreSQL/groupingsets.sql.out:190-212: `select a, b, sum(v), count(*)
  //   from gstest_empty group by grouping sets ((a,b),())` produces no rows,
  //   as does `group by grouping sets ((),(),())`.
  //   So option (a) is correct and is applied here: with no input, drain
  //   nothing and finish. All per-level tables are necessarily empty in that
  //   case (nothing was ever fed to them), so the only thing the cascade could
  //   have produced is the synthetic global row we must not emit.
  if (!receivedInput_) {
    state_ = State::kDone;
    drainLevel_ = -1;
    addRuntimeStat("rollup.bypassRows", RuntimeCounter(bypassRows_));
    addRuntimeStat("rollup.cascadeFlushes", RuntimeCounter(cascadeFlushes_));
    return;
  }

  state_ = State::kFinalCascade;
  drainLevel_ = 1;
}

bool RollupOperator::isFinished() {
  return state_ == State::kDone && outputQueue_.empty();
}

void RollupOperator::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  // RESOLVED (was OPEN QUESTION 2). Reading Operator::MemoryReclaimer::reclaim
  // (Operator.cpp:745-804) answers both halves of the question:
  //
  //  * WHEN. The driver is guaranteed to be off-thread, suspended or terminated,
  //    and the task paused (the VELOX_CHECKs at Operator.cpp:758-765). So the
  //    operator is at a quiescent point and mutating its own state — including
  //    pushing onto outputQueue_ — is safe. There is no rule against producing
  //    vectors here.
  //  * THE ACTUAL CONSTRAINT. The call is wrapped in
  //    memory::ScopedReclaimedBytesRecorder and followed by
  //        VELOX_CHECK_GE(reclaimedBytes, 0, "Unexpected memory growth after
  //                       reclaim from operator memory pool {}")
  //    (Operator.cpp:790-795). Reclaim must therefore be NET NON-ALLOCATING on
  //    this operator's pool. Draining a level allocates the extracted RowVector
  //    (and, for variable-width accumulators, copies out of the string
  //    allocator) BEFORE resetTable frees the container — so a naive
  //    drain-and-park can transiently, and sometimes permanently, grow the pool
  //    and trip that check.
  //
  // The consequence is that "cascade a flush" is a legal reclaim strategy only
  // if the freed table bytes exceed the parked output bytes. That is the common
  // case (a RowContainer plus its hash table is far larger than one extracted
  // batch) but it is not guaranteed, particularly for a level whose reduction is
  // poor and whose accumulators are variable-width.
  //
  // Also note maybeSetReclaimer (Operator.cpp:113-121) installs the reclaimer
  // regardless of spillConfig, so overriding canReclaim() really does put this
  // operator in the arbitrator's reach.
  //
  // =========================================================================
  // WHAT THE ACCOUNTING ACTUALLY MEASURES — and why it is friendlier than the
  // design draft assumed.
  // =========================================================================
  // ScopedReclaimedBytesRecorder (MemoryArbitrator.cpp:527-557) is:
  //     before = pool_->reservedBytes()          // ctor
  //     *reclaimedBytes = before - pool_->reservedBytes();   // dtor
  // It samples RESERVED bytes, not USED bytes. That distinction is the whole
  // ballgame. Velox reserves in coarse quanta and holds the reservation above
  // actual usage, so a drain that frees a multi-megabyte RowContainer and
  // allocates a few hundred KB of output vectors typically does not move
  // reservedBytes upward at all — the output is carved out of reservation the
  // pool was already holding. The two things that DO move it are (i) an
  // allocation that overruns the current reservation and (ii) an explicit
  // pool()->maybeReserve(). We eliminate (ii) outright (maybeGrowBudget is a
  // no-op while reclaiming_, see the comment there) and bound (i) by the
  // phase structure below.
  //
  // =========================================================================
  // STRUCTURE: two phases, strictly non-allocating first.
  // =========================================================================
  // PHASE 1 is guaranteed net non-allocating and runs unconditionally. It frees
  //   RETAINED-BUT-EMPTY table allocations. Every completed steady-state flush
  //   resets its level with freeTable = false (mirroring
  //   HashAggregation::resetPartialOutputIfNeed, HashAggregation.cpp:286-311),
  //   which deliberately keeps the RowContainer and hash table allocated for
  //   reuse. Under arbitration that retention is exactly the memory we should
  //   give back first. Phase 1 allocates nothing whatsoever: it calls
  //   resetTable(freeTable = true) on tables with no live rows, then
  //   pool()->release() to hand the now-unused reservation back
  //   (MemoryPool::release(), MemoryPool.h:389-410 — "returns the reserved but
  //   not used memory in bytes that can be released by calling release()").
  //   If Phase 1 alone meets targetBytes we stop, and reclaim is then provably
  //   non-growing.
  //
  // PHASE 2 is the cascade drain, and it is where the residual risk lives. It
  //   drains ONE level in batches, parking each projected batch on outputQueue_
  //   and feeding each natural batch into the next coarser level, exactly as
  //   the steady-state path does — it literally calls advanceDrain(), so there
  //   is one drain implementation and one exactly-once guarantee, not two.
  //   Batch granularity is the mitigation §4 asks for: we re-check the
  //   reservation after every batch and stop the moment targetBytes is met, so
  //   the exposure is at most one output batch rather than a whole table.
  //
  // =========================================================================
  // RESIDUAL RISK (stated precisely, per the review's request).
  // =========================================================================
  // Phase 2 can still end net-growing in one specific shape: a level whose
  // reduction is poor AND whose extracted row carries out-of-line bytes —
  // variable-width GROUPING KEYS just as much as variable-width accumulators.
  // (phase2ReclaimSafe_, computed in initialize(), refuses Phase 2 for exactly
  // that shape; see the derivation there.) There,
  // GroupingSet::extractGroups copies out of the string allocator into fresh
  // vector buffers, so the parked output can approach the size of the container
  // being freed, while the container's own allocation is only returned at the
  // END of the drain (resetTable cannot run until the iterator is exhausted —
  // it would invalidate the RowContainerIterator mid-flight). If reservedBytes
  // rises above the entry value before that point, the VELOX_CHECK_GE at
  // Operator.cpp:788-795 fires and fails the QUERY, not just the reclaim.
  //
  // Guard, not a proof: `reservedAtEntry` is sampled at the top and re-checked
  // after every batch; if the reservation has risen at or above the entry value
  // we STOP immediately rather than continue into a growing drain. This bounds
  // the overshoot to a single batch, but it cannot undo an overshoot that has
  // already happened within that batch. The honest statement is therefore:
  // Phase 1 is safe by construction, Phase 2 is safe with high probability and
  // a one-batch worst case.
  //
  // The abort lever if measurement says otherwise (DESIGN.md §11 item 2, still
  // the highest-priority risk): delete Phase 2, keep Phase 1, and reclaim
  // becomes trivially correct at the cost of reclaiming much less. Failing
  // that, revert canReclaim() to the base default and accept that the
  // arbitrator may kill the query under pressure. Both are one-line changes,
  // which is deliberate.
  //
  // NOTE ON `stats`: memory::MemoryReclaimer::Stats has exactly four fields —
  // numNonReclaimableAttempts, reclaimExecTimeUs, reclaimedBytes,
  // reclaimWaitTimeUs (MemoryArbitrator.h:304-322). We write ONLY
  // numNonReclaimableAttempts. reclaimExecTimeUs and reclaimedBytes are already
  // accumulated for us by MemoryReclaimer::run (MemoryArbitrator.cpp:187-212),
  // which wraps this call; writing them here would double-count. Everything
  // else we want to record goes to addRuntimeStat, which is captured because
  // the caller installs a RuntimeStatWriterScopeGuard (Operator.cpp:786).

  // (b) Non-reclaimable section. Operator::MemoryReclaimer::reclaim already
  // checks op_->nonReclaimableSection_ (Operator.cpp:772-787) and returns early,
  // and — since the shadowing member was removed (see RollupOperator.h) — that
  // IS this same inherited flag, the one every level's GroupingSet was handed
  // and that GroupingSet internals raise during addInput. So this check is
  // strictly redundant under the stock Operator::MemoryReclaimer. It is kept
  // because it costs nothing and keeps the operator correct if it is ever
  // driven by a different reclaimer, and because the redundancy is only true so
  // long as the two flags stay the same object — which is exactly the thing the
  // shadow silently broke.
  if (nonReclaimableSection_) {
    ++stats.numNonReclaimableAttempts;
    return;
  }

  // (a) Any State is legal. The rules per state:
  //   kDone            — nothing to drain; Phase 1 still frees retained tables.
  //   kAccumulating    — no drain in flight (advanceDrain resets the iterator on
  //                      level exhaustion and only then returns to this state),
  //                      so Phase 2 may START a drain on any level.
  //   kPressureDrain   — a drain IS in flight on drainLevel_ with a
  //   kFinalCascade      half-consumed RowContainerIterator. Phase 2 must
  //                      CONTINUE that level and must not retarget drainLevel_;
  //                      doing so would strand the remaining rows of the level
  //                      being drained and break the "each state flows down
  //                      exactly once" invariant the correctness proof rests on
  //                      (DESIGN.md §4.0). Phase 1 skips drainLevel_ for the
  //                      same reason.
  // In every case we leave the state machine in a self-consistent state on exit:
  // if Phase 2 started or advanced a drain, state_/drainLevel_ describe it and
  // the normal getOutput() path finishes it. needsInput() is false throughout a
  // kPressureDrain, so no input can arrive into a half-drained level.
  ++numReclaims_;
  addRuntimeStat("rollup.reclaim.count", RuntimeCounter(numReclaims_));
  const int64_t reservedAtEntry = static_cast<int64_t>(pool()->reservedBytes());
  const auto reclaimedSoFar = [&]() -> int64_t {
    return reservedAtEntry - static_cast<int64_t>(pool()->reservedBytes());
  };

  // Record the net result on EVERY exit path, not just the one that runs the
  // full Phase 2. reclaim() has four early returns (target met by Phase 1,
  // kDone, !phase2ReclaimSafe_, nothing to drain) and without this guard three
  // of them reported no byte figure at all — which is precisely the case you
  // most need the number for when diagnosing an arbitration stall in
  // production. Cheap and total; the tail of Phase 2 no longer emits its own.
  const auto recordReclaimed = folly::makeGuard([&]() {
    if (std::uncaught_exceptions() > 0) {
      return;
    }
    addRuntimeStat(
        "rollup.reclaim.bytes",
        RuntimeCounter(
            std::max<int64_t>(reclaimedSoFar(), 0), RuntimeCounter::Unit::kBytes));
  });

  // ---------------------------------------------------------------------
  // PHASE 1 — free retained-but-empty table allocations. Allocates nothing.
  // ---------------------------------------------------------------------
  for (auto i = 1; i < static_cast<int32_t>(levels_.size()); ++i) {
    auto& level = levels_[i];
    if (level.groupingSet == nullptr) {
      continue;
    }
    // NEVER the grand total. Its accumulators do not live in a hash table at
    // all — a global GroupingSet keeps them in rows_ + stringAllocator_
    // (GroupingSet::allocatedBytes takes that branch when table_ == nullptr,
    // GroupingSet.cpp:957-961). resetTable() happens to be a harmless no-op
    // there (HashTable::clear is only reached via table_, GroupingSet.cpp:914-918),
    // but the function that WOULD free it, resetGlobalAggregation()
    // (GroupingSet.cpp:920-928), destroys the single running grand-total
    // accumulator. Calling that here would silently drop the level-0 result.
    // Skipping explicitly so the safety is stated, not accidental.
    if (level.numKeys == 0) {
      continue;
    }
    // Never touch the level currently being iterated.
    if (i == drainLevel_ &&
        (state_ == State::kPressureDrain || state_ == State::kFinalCascade)) {
      continue;
    }
    // numRows() is the RowContainer's live row count (GroupingSet.h:170-172);
    // allocatedBytes() (GroupingSet.h:92, GroupingSet.cpp:952-963) is what the
    // container still holds. Rows == 0 with bytes > 0 is precisely the
    // "retained for reuse after a flush" state.
    if (level.groupingSet->numRows() == 0 &&
        level.groupingSet->allocatedBytes() > 0) {
      level.groupingSet->resetTable(/*freeTable=*/true);
    }
  }
  // Hand back reservation the pool is holding but no longer using. This is the
  // step that converts a freed RowContainer into a reservedBytes() drop, which
  // is what ScopedReclaimedBytesRecorder actually measures.
  pool()->release();

  if (reclaimedSoFar() >= static_cast<int64_t>(targetBytes)) {
    addRuntimeStat(
        "rollup.reclaim.freedRetainedBytes",
        RuntimeCounter(reclaimedSoFar(), RuntimeCounter::Unit::kBytes));
    return;
  }

  // ---------------------------------------------------------------------
  // PHASE 2 — cascade drain. May allocate; guarded and batch-bounded.
  // ---------------------------------------------------------------------
  if (state_ == State::kDone) {
    // Everything has already been drained and Phase 1 freed what was retained.
    // (c) Nothing more can be freed — return without failing. A reclaim that
    // frees nothing is legal: the arbitrator reads the recorded delta (possibly
    // zero) and moves on to another pool. It must NOT throw.
    return;
  }

  // The residual risk documented above is confined to variable-width /
  // external-memory accumulators, so refuse Phase 2 exactly there rather than
  // gambling the query on a probabilistic guard. Fixed-width accumulators
  // extract into fixed-size buffers, which is the regime where "freed container
  // >> parked batch" holds comfortably. Phase 1 has already run either way.
  if (!phase2ReclaimSafe_) {
    addRuntimeStat("rollup.phase2ReclaimSkipped", RuntimeCounter(1));
    return;
  }

  if (state_ == State::kAccumulating) {
    // No iterator is in flight, so we may choose a level. Reuse the same policy
    // the pressure path uses, then fall back to "finest level holding anything"
    // when nothing is formally over budget — under arbitration the budgets are
    // beside the point.
    auto target = findFullLevel();
    if (target < 0) {
      for (auto i = 1; i < static_cast<int32_t>(levels_.size()); ++i) {
        if (levels_[i].groupingSet != nullptr &&
            levels_[i].groupingSet->numRows() > 0) {
          target = i;
          break;
        }
      }
    }
    if (target < 0) {
      // (c) Nothing holds any state. Return quietly.
      return;
    }
    state_ = State::kPressureDrain;
    drainLevel_ = target;
    ++cascadeFlushes_;
  }

  VELOX_CHECK(
      state_ == State::kPressureDrain || state_ == State::kFinalCascade,
      "Rollup reclaim reached the drain phase in an unexpected state");
  VELOX_CHECK_GE(drainLevel_, 1);

  // reclaiming_ changes two behaviours inside advanceDrain(): maybeGrowBudget()
  // becomes a no-op (mandatory — see the comment there), and a fully drained
  // level is reset with freeTable = true so the container is actually returned.
  reclaiming_ = true;
  // Exception safety: advanceDrain() can throw (an aggregate's extract path
  // can). Leaving reclaiming_ latched would silently disable budget growth for
  // the rest of the query, so clear it unconditionally on the way out. Note the
  // recorder treats an in-flight exception as "zero reclaimed"
  // (MemoryArbitrator.cpp:538-543), so we do not need to unwind anything else.
  const auto clearReclaiming =
      folly::makeGuard([this]() { reclaiming_ = false; });

  const int32_t startLevel = drainLevel_;
  int32_t batches = 0;
  while (state_ == State::kPressureDrain || state_ == State::kFinalCascade) {
    // Do not wander onto a second level. In kFinalCascade advanceDrain()
    // advances drainLevel_ on exhaustion; continuing would drain the entire
    // hierarchy inside one reclaim call, which is far more work than the
    // arbitrator asked for and multiplies the parked-output exposure.
    if (drainLevel_ != startLevel) {
      break;
    }

    auto batch = advanceDrain();
    ++batches;
    if (batch != nullptr) {
      // Park it. getOutput() drains outputQueue_ before anything else, so this
      // is picked up as soon as the driver resumes. This is legal precisely
      // because the driver is off-thread and the task paused
      // (Operator.cpp:758-765).
      outputQueue_.push_back(std::move(batch));
    } else {
      // advanceDrain returned nullptr: the level is exhausted and it has already
      // reset the table (freeTable = true, because reclaiming_ is set) and
      // updated state_/drainLevel_. Give the freed pages back before we
      // re-measure.
      pool()->release();
      break;
    }

    if (reclaimedSoFar() >= static_cast<int64_t>(targetBytes)) {
      break;
    }
    // THE GUARD. If the reservation is no longer below where we started, this
    // drain is not paying for itself; stop before the next batch rather than
    // walk into the VELOX_CHECK_GE.
    if (reclaimedSoFar() <= 0) {
      addRuntimeStat("rollup.reclaim.abortedOnGrowth", RuntimeCounter(1));
      break;
    }
  }

  pool()->release();

  addRuntimeStat("rollup.reclaim.batches", RuntimeCounter(batches));
  // rollup.reclaim.bytes is emitted by recordReclaimed on the way out.
}

void RollupOperator::recordLevelStats(const LevelState& level, int32_t /*index*/) {
  const auto prefix = fmt::format("rollup.L{}", level.numKeys);
  addRuntimeStat(prefix + ".inputRows", RuntimeCounter(level.totalInputRows));
  addRuntimeStat(prefix + ".outputRows", RuntimeCounter(level.totalOutputRows));
  addRuntimeStat(prefix + ".flushTimes", RuntimeCounter(level.flushTimes));
  addRuntimeStat(prefix + ".flushRowCount", RuntimeCounter(level.numOutputRows));
  if (level.numInputRows > 0) {
    addRuntimeStat(
        prefix + ".aggregationPct",
        RuntimeCounter(100 * level.numOutputRows / level.numInputRows));
  }
  if (level.groupingSet != nullptr) {
    addRuntimeStat(
        prefix + ".hashTableBytes",
        RuntimeCounter(level.groupingSet->allocatedBytes(), RuntimeCounter::Unit::kBytes));
  }
}

void RollupOperator::close() {
  for (auto& level : levels_) {
    level.groupingSet.reset();
    level.output.reset();
  }
  outputQueue_.clear();
  Operator::close();
}

RowVectorPtr RollupOperator::project(int32_t index, const RowVectorPtr& natural) const {
  const auto& level = levels_[index];
  VELOX_CHECK_NOT_NULL(natural);

  // Zero-copy for everything that carries data: the surviving key columns and
  // all accumulator columns are shared by reference with `natural`. Only the
  // masked-out key columns and the gid column are newly created, and both are
  // constants.
  //
  // The two factories used here are BaseVector::createNullConstant and
  // BaseVector::createConstant (velox/vector/BaseVector.h:601-610).
  const auto size = natural->size();
  const auto& inputType = node_->inputType();

  std::vector<VectorPtr> children;
  children.reserve(numKeys_ + numAggregates_ + 1);

  for (auto j = 0; j < numKeys_; ++j) {
    // keyIsNull is the JVM-supplied mask; for pure ROLLUP it agrees with
    // (j >= level.numKeys), which initialize() asserts.
    if (j < level.numKeys && !level.keyIsNull[j]) {
      children.push_back(natural->childAt(j));
    } else {
      // outputType_ makes every key column nullable, so this is well typed even
      // when the source key column is non-nullable.
      children.push_back(
          BaseVector::createNullConstant(inputType->childAt(j), size, pool()));
    }
  }

  for (auto a = 0; a < numAggregates_; ++a) {
    children.push_back(natural->childAt(level.numKeys + a));
  }

  children.push_back(BaseVector::createConstant(
      BIGINT(), Variant(level.gid), size, pool()));

  return std::make_shared<RowVector>(
      pool(), outputType_, /*nulls=*/nullptr, size, std::move(children));
}

} // namespace gluten
