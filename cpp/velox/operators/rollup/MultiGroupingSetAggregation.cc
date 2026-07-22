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
#include "operators/rollup/MultiGroupingSetAggregation.h"

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

namespace facebook::velox::exec {

MultiGroupingSetAggregation::MultiGroupingSetAggregation(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const GroupingSetAggregationNodePtr& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "GroupingSetAggregation",
          // No spill: this operator emits partial state and flushes instead.
          std::nullopt),
      node_{node},
      numKeys_{static_cast<int32_t>(node->groupingKeys().size())},
      numAggregates_{static_cast<int32_t>(node->aggregates().size())},
      maxPartialAggregationMemoryUsage_{static_cast<int64_t>(
          driverCtx->queryConfig().maxPartialAggregationMemoryUsage())},
      maxExtendedPartialAggregationMemoryUsage_{static_cast<int64_t>(
          driverCtx->queryConfig().maxExtendedPartialAggregationMemoryUsage())},
      abandonPartialAggregationMinPct_{
          driverCtx->queryConfig().abandonPartialAggregationMinPct()},
      operatorBudgetCeilingBytes_{maxExtendedPartialAggregationMemoryUsage_} {
  // Nothing that allocates from pool() may happen here (Operator.h:207-215).
}

void MultiGroupingSetAggregation::initialize() {
  Operator::initialize();

  const auto& sets = node_->groupingSets();
  const auto& inputType = node_->inputType();

  // Precondition: the producer must hand us ROW(k1..kn, acc1..accm). If it
  // drifts we want a loud failure, not silently wrong keys.
  VELOX_CHECK_EQ(
      inputType->size(),
      numKeys_ + numAggregates_,
      "GroupingSetAggregation input must be exactly the grouping keys "
      "followed by the aggregate states");
  for (auto i = 0; i < numKeys_; ++i) {
    VELOX_CHECK_EQ(
        inputType->nameOf(i),
        node_->groupingKeys()[i]->name(),
        "Grouping key {} is not at input channel {}",
        node_->groupingKeys()[i]->name(),
        i);
  }

  const auto numSets = static_cast<int32_t>(sets.size());
  std::vector<GroupingSetMask> masks;
  masks.reserve(numSets);

  nodes_.resize(numSets);
  for (auto i = 0; i < numSets; ++i) {
    auto& node = nodes_[i];
    node.gid = sets[i].gid;
    node.keyIsActive = sets[i].keyIsActive;
    for (auto j = 0; j < numKeys_; ++j) {
      if (node.keyIsActive[j]) {
        node.activeKeys.push_back(static_cast<column_index_t>(j));
      }
    }
    node.naturalType = makeNaturalType(node);
    node.maxPartialBytes = maxPartialAggregationMemoryUsage_;
    masks.push_back(sets[i].mask());
  }

  plan_ = buildDerivationPlan(masks, node_->childGroupedSet());

  // The derivation argument (HYBRID_DESIGN.md §6.2) needs `f: G(P) -> G(S)`,
  // which drops the keys in P \ S, to be a total function. That holds iff the
  // parent's mask is a strict superset. buildDerivationPlan establishes it;
  // assert it here so a future change to the lattice cannot silently produce a
  // wrong answer.
  for (auto i = 0; i < numSets; ++i) {
    const auto parent = plan_.parent[i];
    if (parent == kRawInputParent) {
      continue;
    }
    VELOX_CHECK(
        (masks[i] & ~masks[parent]) == 0 && masks[i] != masks[parent],
        "Grouping set {} is not a strict subset of its derivation parent {}",
        i,
        parent);
  }

  // Column selections. Every node receives batches of its own naturalType,
  // produced by derive() from either the raw input (a root) or its parent's
  // naturalType.
  for (auto i = 0; i < numSets; ++i) {
    auto& node = nodes_[i];
    const auto parent = plan_.parent[i];
    if (parent == kRawInputParent) {
      // Key j of the raw input sits at channel j; accumulators start at
      // numKeys_.
      node.deriveKeyChannels = node.activeKeys;
      node.deriveAccBase = static_cast<column_index_t>(numKeys_);
    } else {
      // The parent's naturalType lists ITS active keys in ascending key order,
      // so this node's key j is at the position of j within the parent's
      // activeKeys.
      const auto& parentKeys = nodes_[parent].activeKeys;
      node.deriveKeyChannels.reserve(node.activeKeys.size());
      for (auto key : node.activeKeys) {
        const auto it = std::find(parentKeys.begin(), parentKeys.end(), key);
        VELOX_CHECK(
            it != parentKeys.end(),
            "Grouping set {} has an active key its parent does not",
            i);
        node.deriveKeyChannels.push_back(
            static_cast<column_index_t>(it - parentKeys.begin()));
      }
      node.deriveAccBase = static_cast<column_index_t>(parentKeys.size());
    }
  }

  // Tables. The bypassed set gets none: rows arriving from the child partial
  // aggregate are already grouped at that grain, so we tag and forward them.
  // At q67's measured 1.11 rows/group the finest set holds 83.5% of all the
  // state in the operator and its table does essentially nothing -- this is the
  // single biggest table we never build.
  for (auto i = 0; i < numSets; ++i) {
    auto& node = nodes_[i];
    if (plan_.bypass[i]) {
      node.bypass = true;
      continue;
    }
    node.aggregates = makeAggregateInfos(i);
    for (const auto& info : node.aggregates) {
      VELOX_CHECK_NOT_NULL(
          info.function, "Aggregate info has no Aggregate instance");
    }

    makeGroupingSet(i);
  }

  addRuntimeStat("gsagg.numSets", RuntimeCounter(numSets));
  int32_t numRoots{0};
  for (auto i = 0; i < numSets; ++i) {
    if (plan_.parent[i] == kRawInputParent) {
      ++numRoots;
    }
  }
  // The headline diagnostic for the lattice: numRoots == numSets means the plan
  // degenerated to a flat fan-out and the raw input is scanned once per set.
  addRuntimeStat("gsagg.numLatticeRoots", RuntimeCounter(numRoots));
}

RowTypePtr MultiGroupingSetAggregation::makeNaturalType(
    const NodeState& node) const {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(node.activeKeys.size() + numAggregates_);
  types.reserve(node.activeKeys.size() + numAggregates_);

  const auto& inputType = node_->inputType();
  for (auto channel : node.activeKeys) {
    names.push_back(inputType->nameOf(channel));
    types.push_back(inputType->childAt(channel));
  }
  for (auto i = 0; i < numAggregates_; ++i) {
    names.push_back(node_->aggregateNames()[i]);
    // Intermediate type resolved from the registry rather than trusted from the
    // call's declared return type. Using the same source of truth here and in
    // makeAggregateInfos and in the node's output type is what guarantees the
    // derivation pipe types line up.
    types.push_back(resolveIntermediateType(
        node_->aggregates()[i].call->name(),
        node_->aggregates()[i].rawInputTypes));
  }
  return ROW(std::move(names), std::move(types));
}

std::vector<AggregateInfo> MultiGroupingSetAggregation::makeAggregateInfos(
    int32_t index) {
  auto& node = nodes_[index];
  // The GroupingSet is constructed over this node's own naturalType, so the
  // accumulator columns sit immediately after this node's active keys, both on
  // the way in and on the way out.
  const auto accBase = node.numActive();

  std::vector<AggregateInfo> infos;
  infos.reserve(numAggregates_);
  for (auto i = 0; i < numAggregates_; ++i) {
    const auto& aggregate = node_->aggregates()[i];
    // BASE, unsuffixed function name -- see the name contract on the node's
    // constructor. `name` + rawInputTypes is what identifies the function; the
    // merge semantics come from the GroupingSet flags below, not from a
    // "_merge" companion. A companion name would break the very next line,
    // because resolveIntermediateType would be asked to match raw argument
    // types against a companion signature declared over the intermediate type
    // (avg: raw DOUBLE vs ROW(DOUBLE, BIGINT)).
    const auto& name = aggregate.call->name();

    // The operator emits PARTIAL STATE, and every mechanism here -- flushing a
    // table mid-input, the bypass lane, and deriving a child from an incomplete
    // installment of its parent's state -- rests on duplicate (keys, gid) rows
    // being legal, which requires a decomposable aggregate. Check rather than
    // trust the planner: a planner bug here would silently produce wrong
    // results.
    VELOX_CHECK_NULL(
        aggregate.mask, "GroupingSetAggregation does not support masked "
                        "aggregates");
    VELOX_CHECK(
        aggregate.sortingKeys.empty(),
        "GroupingSetAggregation does not support order-sensitive aggregates");
    VELOX_CHECK(
        !aggregate.distinct,
        "GroupingSetAggregation does not support DISTINCT aggregates");

    AggregateInfo info;
    info.inputs = {static_cast<column_index_t>(accBase + i)};
    info.constantInputs = {nullptr};
    info.mask = std::nullopt;
    info.intermediateType =
        resolveIntermediateType(name, aggregate.rawInputTypes);
    info.output = static_cast<column_index_t>(accBase + i);

    // kPartial => the function's result type is the intermediate type, i.e.
    // extractAccumulators on the way out. Combined with GroupingSet's
    // isRawInput = false this gives addIntermediateResults on the way in:
    // states in, states out. rawInputTypes are passed unchanged because
    // Aggregate::create always resolves the signature from raw argument types
    // regardless of step.
    info.function = Aggregate::create(
        name,
        core::AggregationNode::Step::kPartial,
        aggregate.rawInputTypes,
        info.intermediateType,
        operatorCtx_->driverCtx()->queryConfig());

    VELOX_CHECK(
        node.naturalType->childAt(accBase + i)
            ->equivalent(*info.intermediateType),
        "Grouping set {} aggregate {} expects intermediate type {} but "
        "channel {} is {}",
        index,
        name,
        info.intermediateType->toString(),
        accBase + i,
        node.naturalType->childAt(accBase + i)->toString());

    infos.push_back(std::move(info));
  }
  return infos;
}

void MultiGroupingSetAggregation::makeGroupingSet(int32_t index) {
  auto& node = nodes_[index];

  std::vector<column_index_t> keyChannels;
  keyChannels.reserve(node.numActive());
  for (auto i = 0; i < node.numActive(); ++i) {
    keyChannels.push_back(static_cast<column_index_t>(i));
  }

  // Empty hashers => global aggregation. That is the grand-total set, and it
  // needs no special casing anywhere else.
  auto hashers = createVectorHashers(node.naturalType, keyChannels);

  node.groupingSet = std::make_unique<GroupingSet>(
      node.naturalType,
      std::move(hashers),
      /*preGroupedKeys=*/std::vector<column_index_t>{},
      /*groupingKeyOutputProjections=*/std::vector<column_index_t>{},
      std::move(node.aggregates),
      // Grouping keys may legitimately be NULL in the data; never drop those
      // rows. gid, not null-ness, is what distinguishes a masked key from a
      // data NULL at the output.
      /*ignoreNullKeys=*/false,
      // isPartial: emit accumulators, not final values, and enable the
      // isPartialFull() budget check.
      /*isPartial=*/true,
      // isRawInput: false -- every node merges states, never raw values.
      /*isRawInput=*/false,
      /*globalGroupingSets=*/std::vector<vector_size_t>{},
      /*groupIdChannel=*/std::nullopt,
      // No spill by design; we flush instead.
      /*spillConfig=*/nullptr,
      // Both of these are INHERITED Operator members, not our own.
      // nonReclaimableSection_ must be the base's, because that is the one
      // Operator::MemoryReclaimer::reclaim tests.
      &nonReclaimableSection_,
      &operatorCtx_->driverCtx()->queryConfig(),
      pool(),
      spillStats_.get());
}

// ===========================================================================
// Shaping
// ===========================================================================

RowVectorPtr MultiGroupingSetAggregation::derive(
    int32_t index,
    const RowVectorPtr& source) const {
  const auto& node = nodes_[index];
  std::vector<VectorPtr> children;
  children.reserve(node.deriveKeyChannels.size() + numAggregates_);
  for (auto channel : node.deriveKeyChannels) {
    children.push_back(source->childAt(channel));
  }
  for (auto a = 0; a < numAggregates_; ++a) {
    children.push_back(source->childAt(node.deriveAccBase + a));
  }
  return std::make_shared<RowVector>(
      pool(),
      node.naturalType,
      /*nulls=*/nullptr,
      source->size(),
      std::move(children));
}

RowVectorPtr MultiGroupingSetAggregation::project(
    int32_t index,
    const RowVectorPtr& natural) const {
  const auto& node = nodes_[index];
  VELOX_CHECK_NOT_NULL(natural);

  // Zero-copy for everything that carries data: the active key columns and all
  // accumulator columns are shared by reference with `natural`. Only the masked
  // key columns and gid are newly created, and both are constants.
  const auto size = natural->size();
  const auto& inputType = node_->inputType();

  std::vector<VectorPtr> children;
  children.reserve(numKeys_ + numAggregates_ + 1);

  int32_t active = 0;
  for (auto j = 0; j < numKeys_; ++j) {
    if (node.keyIsActive[j]) {
      children.push_back(natural->childAt(active));
      ++active;
    } else {
      children.push_back(
          BaseVector::createNullConstant(inputType->childAt(j), size, pool()));
    }
  }

  for (auto a = 0; a < numAggregates_; ++a) {
    children.push_back(natural->childAt(active + a));
  }

  children.push_back(
      BaseVector::createConstant(BIGINT(), Variant(node.gid), size, pool()));

  return std::make_shared<RowVector>(
      pool(), outputType_, /*nulls=*/nullptr, size, std::move(children));
}

// ===========================================================================
// Input
// ===========================================================================

bool MultiGroupingSetAggregation::needsInput() const {
  return !noMoreInput_ && drainStack_.empty() && outputQueue_.empty();
}

void MultiGroupingSetAggregation::feedChildren(
    int32_t index,
    const RowVectorPtr& natural) {
  for (auto child : plan_.children[index]) {
    feedNode(child, derive(child, natural));
  }
}

void MultiGroupingSetAggregation::feedNode(
    int32_t index,
    const RowVectorPtr& natural) {
  auto& node = nodes_[index];
  node.numInputRows += natural->size();
  node.totalInputRows += natural->size();

  if (node.groupingSet == nullptr) {
    // Bypass lane, or a node that has abandoned aggregation and had its table
    // drained and dropped. Either way this node is a pure projection: tag the
    // rows with its gid and mask, emit them, and pass the SAME batch on to its
    // children.
    //
    // This is correct without any reshaping precisely because a node's input
    // type and its natural (output) type are the same -- see "The type pipe" on
    // the class. And it is correct semantically because the node merges states
    // (isRawInput = false), so the identity IS a valid aggregation of them;
    // the volume simply shifts to the children, which is what the unfused plan
    // does anyway.
    if (node.bypass) {
      bypassRows_ += natural->size();
    }
    node.numOutputRows += natural->size();
    node.totalOutputRows += natural->size();
    outputQueue_.push_back(project(index, natural));
    feedChildren(index, natural);
    return;
  }

  // There is no "abandoned but not yet drained" state to handle here. The only
  // write to `abandoned` is in maybeAbandonAfterDrain(), called from
  // completeNodeDrain(), which drops `groupingSet` a few lines later in the
  // same call. So {abandoned && groupingSet != nullptr} never survives that
  // call, and the branch above has already returned for every abandoned node.
  // (This is also why FlushReason::kAbandoned is never returned; it is retained
  // only because maybeGrowBudget() reads `node.abandoned` inside that window.)
  VELOX_DCHECK(!node.abandoned);

  node.groupingSet->addInput(natural, /*mayPushdown=*/false);
  // NOTE: deliberately no abandon check here. See maybeAbandonAfterDrain() on
  // the class -- a prefix of the stream cannot identify this node's reduction,
  // so the decision is deferred to the end of a pressure drain, where it is
  // measured rather than extrapolated.
}

void MultiGroupingSetAggregation::maybeAbandonAfterDrain(int32_t index) {
  auto& node = nodes_[index];
  // The abandon valve is meaningless for a global aggregation, and
  // HashAggregation::abandonPartialAggregationEarly VELOX_CHECKs !isGlobal_.
  // The grand-total set holds exactly one group; there is nothing to give up
  // on.
  if (node.numActive() == 0 || node.groupingSet == nullptr || node.abandoned) {
    return;
  }
  // Only a PRESSURE drain measures the steady state. kEndOfInput says nothing
  // about a future the node does not have; kReclaim is the arbitrator's
  // decision, not the data's; kAbandoned is already this state.
  if (node.reason != FlushReason::kSoftBudget &&
      node.reason != FlushReason::kHardCap &&
      node.reason != FlushReason::kOperatorCeiling) {
    return;
  }
  // numInputRows / numOutputRows are reset on every completeNodeDrain, so at
  // this point they are exactly this cycle's absorbed rows and emitted groups.
  // For a node that owns a table and has not abandoned, feedNode() routes every
  // row into groupingSet->addInput() and never touches numOutputRows, so the
  // only contributions to numOutputRows are advanceDrain()'s batches.
  //
  // Evidence is POOLED across cycles rather than required to arrive as a run of
  // consecutive bad ones. A node sitting near the threshold -- q67's 7-key node
  // measures 78% against a limit of 80 at a 64 MB budget -- oscillates either
  // side of it, and a consecutive-run rule is defeated by exactly that
  // oscillation while a pooled ratio reads it correctly. Pooling also uses every
  // sample instead of discarding the ones before the last reset.
  // A cycle that absorbed nothing is not evidence, and must not be counted as
  // one. Two reasons, one of them a crash. (1) Semantics: pooling it would let
  // empty cycles satisfy kAbandonMinEvidenceCycles and dilute the evidence bar
  // this fix exists to raise. (2) Safety: the pooled ratio below divides by
  // abandonWindowInRows, so a window made up entirely of zero-input cycles is
  // an integer division by zero -- SIGFPE, not a wrong answer. That window is
  // hard to reach because shouldFreeTable()'s ZERO-ROW-BUT-STILL-FULL disjunct
  // frees the retained slot array and clears the condition, but "hard to reach"
  // is not "unreachable": a node fed only by a parent's drain output takes zero
  // rows in any cycle its parent does not drain in, and both kSoftBudget and
  // kHardCap are pure allocatedBytes() predicates with no row count in them.
  // recordNodeStats() guards the identical division at the reduction-pct stat;
  // this one was left unguarded when the row floor -- which incidentally
  // excluded the numInputRows == 0 case -- was removed.
  if (node.numInputRows == 0) {
    return;
  }
  node.abandonWindowInRows += node.numInputRows;
  node.abandonWindowOutRows += node.numOutputRows;
  if (++node.abandonWindowCycles < kAbandonMinEvidenceCycles) {
    return;
  }
  const auto pooledPct =
      100 * node.abandonWindowOutRows / node.abandonWindowInRows;
  if (pooledPct < abandonPartialAggregationMinPct_) {
    // The node is earning its table at this budget. Retire the accumulated
    // suspicion and start a fresh window -- reduction can change with the data,
    // and this decision is irreversible, so it must be made on recent evidence.
    node.abandonWindowCycles = 0;
    node.abandonWindowInRows = 0;
    node.abandonWindowOutRows = 0;
    return;
  }
  node.abandoned = true;
  addRuntimeStat(
      fmt::format("gsagg.set{}.abandoned", index), RuntimeCounter(1));
}

void MultiGroupingSetAggregation::addInput(RowVectorPtr input) {
  VELOX_CHECK(drainStack_.empty());
  VELOX_CHECK(!noMoreInput_);

  // A zero-row batch is dropped WHOLESALE. Operator::getOutput is contractually
  // required to return nullptr or a NON-EMPTY vector (Values.cpp:40-41), and
  // Driver::runInternal only tests `if (intermediateResult)` -- it does not
  // re-check size -- so an empty vector would propagate downstream and be
  // counted as an output batch.
  //
  // Note the early return is ABOVE the assignment, so receivedInput_ means
  // "addInput() was called with at least one ROW", not "addInput() was called".
  // That is the meaning noMoreInput() needs.
  if (input->size() == 0) {
    return;
  }
  receivedInput_ = true;

  // Fan out to the lattice roots. Every root consumes the WHOLE batch; memory
  // pressure is not acted on until afterwards. (The single-shared-table
  // prototype broke out of its per-set loop the moment the table filled,
  // permanently dropping the remainder of the batch for the remaining sets,
  // with no resume cursor -- a silent wrong answer. Consuming the batch
  // completely and arming pressure afterwards is the fix, generalised.)
  for (auto i : plan_.order) {
    if (plan_.parent[i] == kRawInputParent) {
      feedNode(i, derive(i, input));
    }
  }

  // Now pressure. Scanning in topological order and taking the first hit means
  // we drain a parent before a child where both are full, which is the more
  // useful drain: the parent's output is the child's input.
  for (auto i : plan_.order) {
    const auto reason = flushReason(i);
    if (reason != FlushReason::kNone) {
      pushDrain(i, reason);
      break;
    }
  }
}

void MultiGroupingSetAggregation::noMoreInput() {
  // IDEMPOTENCE. Nothing in Operator or Driver promises a single delivery;
  // HashBuild::noMoreInput() guards with exactly this test. A second delivery
  // must not restart the final sweep.
  if (noMoreInput_) {
    return;
  }
  Operator::noMoreInput();

  // Unlike the prototype, there is nothing to defer here. A drain in flight
  // keeps its own iterator on its own node, so setting the sweep cursor now is
  // harmless: getOutput() finishes drainStack_ before it looks at the sweep.
  // The prototype's pendingFinalCascade_ existed only to protect a single
  // shared drain cursor.
  //
  // EMPTY INPUT. Spark emits ZERO rows for a grouping-sets/ROLLUP query over
  // empty input -- it never synthesises a grand-total row, because
  // spark_grouping_id is itself a grouping key so the aggregation is never
  // keyless. Verified against the approved golden output
  // gluten-ut/spark40/.../postgreSQL/groupingsets.sql.out:190-212. But a global
  // GroupingSet (no hashers) WOULD emit its single identity row on a fresh
  // iterator whether or not input arrived, and this operator runs per driver --
  // so an unguarded sweep pushes one spurious row into the shuffle for every
  // empty partition. Skip the sweep entirely; all tables are necessarily empty
  // in that case anyway.
  if (!receivedInput_) {
    finalSweepPos_ = static_cast<int32_t>(plan_.order.size());
    recordFinalStats();
    return;
  }
  finalSweepPos_ = 0;
}

// ===========================================================================
// Flush policy
// ===========================================================================

int64_t MultiGroupingSetAggregation::totalNodeBytes() const {
  int64_t total{0};
  for (const auto& node : nodes_) {
    if (node.groupingSet != nullptr) {
      total += static_cast<int64_t>(node.groupingSet->allocatedBytes());
    }
  }
  return total;
}

bool MultiGroupingSetAggregation::overOperatorCeiling() const {
  return totalNodeBytes() > operatorBudgetCeilingBytes_;
}

MultiGroupingSetAggregation::FlushReason
MultiGroupingSetAggregation::flushReason(int32_t index) {
  auto& node = nodes_[index];
  if (node.groupingSet == nullptr || node.draining) {
    return FlushReason::kNone;
  }
  if (node.abandoned) {
    // UNREACHABLE in the current implementation: completeNodeDrain() drops
    // `groupingSet` in the same call that sets `abandoned`, so the branch above
    // has already returned kNone. Kept as a guard rather than deleted so that a
    // future deferred-abandon variant fails safe instead of feeding a table
    // that will never be read.
    return FlushReason::kAbandoned;
  }
  if (node.numActive() == 0) {
    // The global (grand total) node is NEVER flushed mid-query. Its state does
    // not live in a hash table, so resetTable() is a no-op on it and the only
    // function that would reset it, resetGlobalAggregation(), destroys the
    // running accumulator. Draining it twice without a reset would emit the
    // same partial state twice and double-count downstream. It holds exactly
    // one group, so there is nothing to gain either. Final sweep only.
    return FlushReason::kNone;
  }
  if (static_cast<int64_t>(node.groupingSet->allocatedBytes()) >
      kHardCapMultiple * node.maxPartialBytes) {
    return FlushReason::kHardCap;
  }
  if (node.groupingSet->isPartialFull(node.maxPartialBytes)) {
    return FlushReason::kSoftBudget;
  }
  if (node.groupingSet->numRows() > 0 && overOperatorCeiling()) {
    return FlushReason::kOperatorCeiling;
  }
  return FlushReason::kNone;
}

bool MultiGroupingSetAggregation::shouldFreeTable(
    int32_t index,
    FlushReason reason) {
  auto& node = nodes_[index];
  if (reason == FlushReason::kAbandoned ||
      reason == FlushReason::kEndOfInput) {
    return true;
  }
  // CEILING SELF-PERPETUATION. resetTable(false) RETAINS the
  // RowContainer/HashTable allocation, so allocatedBytes() does not drop when a
  // node is drained. Without this disjunct, once the operator-wide sum crossed
  // the ceiling it stayed across it forever and every subsequent addInput()
  // armed another ceiling drain -- a permanent degeneration to "flush after
  // every batch", the exact cliff this operator exists to avoid. When the
  // ceiling is the thing being violated, give the pages back.
  if (overOperatorCeiling()) {
    return true;
  }
  // ZERO-ROW-BUT-STILL-FULL. resetTable(false) -> HashTable::clear(false)
  // frees the RowContainer arena but deliberately RETAINS the slot array,
  // memset to zero. allocatedBytes() therefore falls only to
  // sizeof(char*) * capacity_, and isPartialFull() is a pure function of
  // allocatedBytes() with NO row predicate -- so a node can be reported full
  // while holding zero rows. That is reachable: in kArray mode capacity_ is
  // driven by the key value RANGE, not the row count (up to 16 MB of slot
  // array), so any max_partial_aggregation_memory below 16 MB leaves an
  // emptied table permanently "full". The self-healing rehash inside
  // isPartialFull does not rescue the repeat, because disableRangeArrayHash_
  // latches on the first call. When the node is still over its own budget with
  // nothing in it, the retained slot array IS the problem.
  return node.groupingSet->isPartialFull(node.maxPartialBytes);
}

void MultiGroupingSetAggregation::maybeGrowBudget(NodeState& node) {
  if (node.groupingSet == nullptr || node.abandoned) {
    return;
  }
  if (overOperatorCeiling()) {
    return;
  }
  if (node.maxPartialBytes >= maxExtendedPartialAggregationMemoryUsage_) {
    return;
  }
  const auto grown = std::min(
      node.maxPartialBytes * 2, maxExtendedPartialAggregationMemoryUsage_);
  const auto toReserve = grown - node.maxPartialBytes;
  if (pool()->maybeReserve(toReserve)) {
    node.maxPartialBytes = grown;
  }
}

// ===========================================================================
// Drain
// ===========================================================================

void MultiGroupingSetAggregation::pushDrain(
    int32_t index,
    FlushReason reason) {
  auto& node = nodes_[index];
  VELOX_CHECK(!node.draining);
  VELOX_CHECK_NOT_NULL(node.groupingSet);
  node.draining = true;
  node.reason = reason;
  drainStack_.push_back(index);
  ++numFlushes_;
}

bool MultiGroupingSetAggregation::nodeHasOutput(int32_t index) const {
  const auto& node = nodes_[index];
  if (node.groupingSet == nullptr || node.draining) {
    return false;
  }
  if (node.numActive() == 0) {
    // A global GroupingSet has no table, so numRows() is 0 even when it holds a
    // running accumulator. What it holds is exactly what has been fed to it
    // since the last reset -- and it is never reset mid-query (see
    // flushReason).
    return node.numInputRows > 0;
  }
  return node.groupingSet->numRows() > 0;
}

RowVectorPtr MultiGroupingSetAggregation::getOutput() {
  // 1. Bypass rows, pass-through rows and parked flush output first -- cheapest,
  //    and it keeps the downstream pipeline busy while tables fill.
  if (!outputQueue_.empty()) {
    auto result = std::move(outputQueue_.front());
    outputQueue_.pop_front();
    return result;
  }

  // 2. Nothing in flight: either keep accumulating, or advance the final sweep.
  if (drainStack_.empty()) {
    if (!noMoreInput_) {
      return nullptr;
    }
    const auto numSets = static_cast<int32_t>(plan_.order.size());
    while (finalSweepPos_ >= 0 && finalSweepPos_ < numSets) {
      const auto index = plan_.order[finalSweepPos_++];
      if (nodeHasOutput(index)) {
        pushDrain(index, FlushReason::kEndOfInput);
        break;
      }
    }
    if (drainStack_.empty()) {
      recordFinalStats();
      return nullptr;
    }
  }

  return advanceDrain();
}

RowVectorPtr MultiGroupingSetAggregation::advanceDrain() {
  const auto index = drainStack_.back();
  auto& node = nodes_[index];
  VELOX_CHECK_NOT_NULL(node.groupingSet);

  // GroupingSet::getOutput does NOT allocate `result`: both the hash path and
  // the global path write into a caller-supplied RowVector. HashAggregation
  // calls prepareOutput() first; this is the same thing, per node, reusing the
  // vector across batches when it is uniquely owned.
  const auto maxOutputRows = (node.numActive() == 0) ? 1 : outputBatchRows();
  if (node.output != nullptr) {
    VectorPtr reusable = std::move(node.output);
    BaseVector::prepareForReuse(reusable, maxOutputRows);
    node.output = std::static_pointer_cast<RowVector>(reusable);
  } else {
    node.output = std::static_pointer_cast<RowVector>(
        BaseVector::create(node.naturalType, maxOutputRows, pool()));
  }
  // NOTE: node.output is NOT uniquely owned here -- project() shares its
  // columns with the batch already handed downstream -- so prepareForReuse
  // reallocates rather than reusing.

  // preferredOutputBatchBytes() is uint64_t but GroupingSet::getOutput's
  // maxOutputBytes parameter is int32_t. Clamp explicitly rather than rely on
  // an implicit narrowing conversion, which would wrap to a negative cap if the
  // config were ever raised past 2 GiB.
  const auto maxOutputBytes = static_cast<int32_t>(std::min<uint64_t>(
      operatorCtx_->driverCtx()->queryConfig().preferredOutputBatchBytes(),
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max())));

  // Pass the MEMBER, not a copy: getOutput takes `RowVectorPtr&` and some of
  // its branches reassign it.
  const bool hasMore = node.groupingSet->getOutput(
      maxOutputRows, maxOutputBytes, node.iterator, node.output);
  RowVectorPtr natural = node.output;

  if (!hasMore || natural == nullptr || natural->size() == 0) {
    completeNodeDrain(index);
    return nullptr;
  }

  node.numOutputRows += natural->size();
  node.totalOutputRows += natural->size();

  // Each drained batch is used twice: once as operator output (with this set's
  // gid and null mask), once as input to every child. Deriving a child from a
  // PARTIAL, incomplete installment of this node's state is legal -- see
  // HYBRID_DESIGN.md §6.2 -- which is exactly why no "one drain in flight" rule
  // is needed.
  auto output = project(index, natural);

  // NOTE: this loop is HASHING, and it runs inside getOutput(). A drained
  // parent batch is fed straight into its child's table here, so every probe
  // below the root is charged to getOutput, not addInput.
  for (auto child : plan_.children[index]) {
    feedNode(child, derive(child, natural));
    // A child fed during a parent's drain is allowed to overshoot its SOFT
    // budget until the parent's drain completes: it cannot exceed G(child)
    // entries, which is by definition what it would hold at end of input
    // anyway. Only a HARD cap nests a drain. The nested drain is safe because
    // the child has its own iterator and its own output vector, and it is
    // guaranteed to terminate because the child is strictly later than the
    // parent in plan_.order, so drainStack_ is strictly increasing in
    // topological rank.
    if (flushReason(child) == FlushReason::kHardCap) {
      pushDrain(child, FlushReason::kHardCap);
      ++numNestedDrains_;
    }
  }

  return output;
}

void MultiGroupingSetAggregation::completeNodeDrain(int32_t index) {
  auto& node = nodes_[index];
  ++node.flushTimes;
  recordNodeStats(index);
  // Evaluate the abandon valve on the cycle that just completed, BEFORE the
  // counters are reset below and before the budget is grown -- growing the
  // budget of a node we are about to abandon would reserve memory it will
  // never use (maybeGrowBudget already no-ops once `abandoned` is set).
  maybeAbandonAfterDrain(index);
  maybeGrowBudget(node);

  // An abandoning node is dropping its table on the next line but one; give the
  // pages back rather than retaining an allocation for a table that is about to
  // be destroyed.
  const bool freeTable = node.abandoned || shouldFreeTable(index, node.reason);
  // resetTable(false) mirrors HashAggregation::resetPartialOutputIfNeed: keep
  // the allocation, drop the contents.
  node.groupingSet->resetTable(freeTable);
  if (node.abandoned) {
    // From here on this node is a pure projection; drop the GroupingSet so
    // flushReason() stops reporting it.
    node.groupingSet.reset();
  }

  node.iterator = RowContainerIterator{};
  node.numInputRows = 0;
  node.numOutputRows = 0;
  node.draining = false;
  node.reason = FlushReason::kNone;

  VELOX_CHECK(!drainStack_.empty());
  VELOX_CHECK_EQ(drainStack_.back(), index);
  drainStack_.pop_back();
}

bool MultiGroupingSetAggregation::isFinished() {
  return noMoreInput_ && drainStack_.empty() &&
      finalSweepPos_ >= static_cast<int32_t>(plan_.order.size()) &&
      outputQueue_.empty();
}

// ===========================================================================
// Reclaim
// ===========================================================================

bool MultiGroupingSetAggregation::reclaimableBytes(
    uint64_t& reclaimableBytes) const {
  // Advertise ONLY what reclaim() will actually free -- the retained-but-empty
  // table allocations -- not the whole reservation. The Operator default
  // (Operator.h:406) reports pool()->reservedBytes() whenever canReclaim() is
  // true; for this operator that is a large over-estimate, because the bulk of
  // the reservation is LIVE accumulator state that reclaim() must not touch.
  // Over-advertising makes the arbitrator pick this pool, get back ~0, and skip
  // a pool that could have paid -- the exact defect the audit flagged. Reporting
  // the true freeable figure lets the arbitrator deprioritise us when we hold
  // nothing reclaimable.
  reclaimableBytes = 0;
  if (nonReclaimableSection_) {
    return false;
  }
  for (const auto& node : nodes_) {
    // Same predicate reclaim()'s Phase 1 uses: skip null/global/in-flight nodes,
    // count only tables that hold zero rows but still retain an allocation.
    if (node.groupingSet == nullptr || node.numActive() == 0 || node.draining) {
      continue;
    }
    if (node.groupingSet->numRows() == 0) {
      reclaimableBytes +=
          static_cast<uint64_t>(node.groupingSet->allocatedBytes());
    }
  }
  return true;
}

void MultiGroupingSetAggregation::reclaim(
    uint64_t /*targetBytes*/,
    memory::MemoryReclaimer::Stats& stats) {
  // B3 DECISION -- reclaim frees RETAINED-BUT-EMPTY tables ONLY; it never drains
  // live state. This is the one behaviour that satisfies BOTH invariants the
  // arbitration contract imposes, and the choice is deliberate over the more
  // aggressive drain-to-reclaim design that preceded it.
  //
  //   1. NEVER WRONG ANSWERS. The loop below frees a table only when it already
  //      holds ZERO rows (its groups were extracted by a prior flush). Live
  //      accumulator state is never read, extracted or dropped, so results are
  //      bit-identical to a run in which the arbitrator never called us.
  //
  //   2. NEVER TRIP VELOX_CHECK_GE(reclaimedBytes, 0). The call is wrapped in
  //      memory::ScopedReclaimedBytesRecorder (MemoryArbitrator.cpp:527) and
  //      followed by VELOX_CHECK_GE(reclaimedBytes, 0) at Operator.cpp:795,
  //      which fails the whole QUERY -- not just the reclaim -- on net growth.
  //      reclaimedBytes is reservedBytes(entry) - reservedBytes(exit). This body
  //      performs ZERO allocations: resetTable(freeTable=true) frees a
  //      RowContainer and pool()->release() hands slack back; neither can raise
  //      reservedBytes. The delta is therefore >= 0 by construction, whether or
  //      not anything was freeable. The safety is structural, not probabilistic.
  //
  // WHY NOT DRAIN UNDER ARBITRATION. An earlier "Phase 2" drained a node to free
  // its whole table. It was removed because it cannot be GUARANTEED net
  // non-allocating on this pool, and "safe with high probability" is not good
  // enough when the failure mode is a query-killing CHECK:
  //   - Draining extracts groups out of the RowContainer into fresh output
  //     vectors (GroupingSet::extractGroups). For poor-reduction data -- q67
  //     averages 1.11 rows/group -- the parked output approaches the size of the
  //     freed table, and variable-width keys or accumulators push it over.
  //   - advanceDrain() additionally cascades a drained parent's rows INTO
  //     descendant hash tables on this same pool. That is net growth no
  //     per-batch guard can undo, because the overshoot happens WHILE the batch
  //     that triggers the check is being produced.
  // Under real pressure this operator therefore behaves like any non-spillable
  // operator: it returns what it can free for free, and if that is not enough
  // the arbitrator aborts the query with a capacity error rather than crashing.
  //
  // NOTE ON `stats`: we write ONLY numNonReclaimableAttempts. reclaimedBytes and
  // reclaimExecTimeUs are accumulated by MemoryReclaimer::run around this call.

  if (nonReclaimableSection_) {
    ++stats.numNonReclaimableAttempts;
    return;
  }

  ++numReclaims_;
  // Running total: override, do not accumulate. See recordNodeStats().
  setRuntimeStat("gsagg.reclaim.count", RuntimeMetric(numReclaims_));
  const int64_t reservedAtEntry = static_cast<int64_t>(pool()->reservedBytes());

  // Free retained-but-empty tables. A node in the numRows()==0 &&
  // allocatedBytes()>0 state is holding a slot array retained for reuse after a
  // steady-state flush -- in kArray mode up to 16MB of it (see the
  // "zero-row-but-still-full" note in shouldFreeTable()). That is pure
  // reclaimable slack: freeing it drops nothing the query still needs.
  for (auto i = 0; i < static_cast<int32_t>(nodes_.size()); ++i) {
    auto& node = nodes_[i];
    if (node.groupingSet == nullptr) {
      continue;
    }
    // NEVER the grand total (numActive()==0): its single accumulator does not
    // live in a hash table, and the function that would free it,
    // resetGlobalAggregation(), destroys the running accumulator.
    if (node.numActive() == 0) {
      continue;
    }
    // NEVER a node currently being iterated: its table backs an in-flight drain.
    if (node.draining) {
      continue;
    }
    if (node.groupingSet->numRows() == 0 &&
        node.groupingSet->allocatedBytes() > 0) {
      node.groupingSet->resetTable(/*freeTable=*/true);
    }
  }
  // Convert the freed RowContainers into a reservedBytes() drop -- this is the
  // step the recorder actually measures.
  pool()->release();

  const int64_t reclaimed =
      reservedAtEntry - static_cast<int64_t>(pool()->reservedBytes());
  addRuntimeStat(
      "gsagg.reclaim.bytes",
      RuntimeCounter(
          std::max<int64_t>(reclaimed, 0), RuntimeCounter::Unit::kBytes));
}

// ===========================================================================
// Stats and teardown
// ===========================================================================

void MultiGroupingSetAggregation::recordNodeStats(int32_t index) {
  const auto& node = nodes_[index];
  const auto prefix = fmt::format("gsagg.set{}", index);
  // totalInputRows/totalOutputRows/flushTimes are RUNNING TOTALS and this is
  // called once per drain cycle, so they must OVERRIDE rather than accumulate:
  // addRuntimeStat() sums its argument into RuntimeMetric::sum, which over k
  // cycles would report a sum of prefix sums (~k*total/2) instead of the total.
  setRuntimeStat(prefix + ".inputRows", RuntimeMetric(node.totalInputRows));
  setRuntimeStat(prefix + ".outputRows", RuntimeMetric(node.totalOutputRows));
  setRuntimeStat(prefix + ".flushTimes", RuntimeMetric(node.flushTimes));
  // Per-cycle values: accumulating is correct here, and mirrors upstream
  // HashAggregation's kFlushRowCount.
  addRuntimeStat(prefix + ".flushRowCount", RuntimeCounter(node.numOutputRows));
  if (node.numInputRows > 0) {
    addRuntimeStat(
        prefix + ".aggregationPct",
        RuntimeCounter(100 * node.numOutputRows / node.numInputRows));
  }
  if (node.groupingSet != nullptr) {
    addRuntimeStat(
        prefix + ".hashTableBytes",
        RuntimeCounter(
            node.groupingSet->allocatedBytes(), RuntimeCounter::Unit::kBytes));
  }
}

void MultiGroupingSetAggregation::recordFinalStats() {
  if (finalStatsRecorded_) {
    return;
  }
  finalStatsRecorded_ = true;
  addRuntimeStat("gsagg.bypassRows", RuntimeCounter(bypassRows_));
  addRuntimeStat("gsagg.flushes", RuntimeCounter(numFlushes_));
  addRuntimeStat("gsagg.nestedDrains", RuntimeCounter(numNestedDrains_));
}

void MultiGroupingSetAggregation::close() {
  for (auto& node : nodes_) {
    node.groupingSet.reset();
    node.output.reset();
  }
  outputQueue_.clear();
  drainStack_.clear();
  Operator::close();
}

namespace {
class GroupingSetAggregationTranslator : public Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto gsNode =
            std::dynamic_pointer_cast<const GroupingSetAggregationNode>(node)) {
      return std::make_unique<MultiGroupingSetAggregation>(id, ctx, gsNode);
    }
    return nullptr;
  }
};
} // namespace

void registerMultiGroupingSetAggregation() {
  Operator::registerOperator(
      std::make_unique<GroupingSetAggregationTranslator>());

  // Plan-node serde. core::PlanNode::registerSerDe() cannot register this node
  // because the node lives in exec/ (registering it there would invert the
  // core/ -> exec/ dependency), so the exec-side registration hook does it.
  // Guarded against a double Register(), which the registry treats as a fatal
  // duplicate-key error.
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  if (!registry.Has("GroupingSetAggregationNode")) {
    registry.Register(
        "GroupingSetAggregationNode", GroupingSetAggregationNode::create);
  }
}

} // namespace facebook::velox::exec
