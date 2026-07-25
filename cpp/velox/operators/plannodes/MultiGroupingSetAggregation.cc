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
#include "operators/plannodes/MultiGroupingSetAggregation.h"

#include <algorithm>
#include <limits>

#include <fmt/format.h>

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
      maxPartialAggregationMemoryUsage_{
          static_cast<int64_t>(driverCtx->queryConfig().maxPartialAggregationMemoryUsage())},
      maxExtendedPartialAggregationMemoryUsage_{
          static_cast<int64_t>(driverCtx->queryConfig().maxExtendedPartialAggregationMemoryUsage())},
      abandonPartialAggregationMinRows_{driverCtx->queryConfig().abandonPartialAggregationMinRows()},
      abandonPartialAggregationMinPct_{driverCtx->queryConfig().abandonPartialAggregationMinPct()},
      operatorFlushTargetBytes_{maxExtendedPartialAggregationMemoryUsage_} {
  // Nothing that allocates from pool() may happen here (Operator.h:207-215).
}

void MultiGroupingSetAggregation::initialize() {
  Operator::initialize();
  VELOX_CHECK_GE(maxPartialAggregationMemoryUsage_, 0);
  VELOX_CHECK_GE(maxExtendedPartialAggregationMemoryUsage_, 0);
  VELOX_CHECK_GE(abandonPartialAggregationMinRows_, 0);

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
    node.gid = sets[i].groupingId;
    node.activeKeysMask = sets[i].activeKeysMask;
    for (auto j = 0; j < numKeys_; ++j) {
      if ((node.activeKeysMask & (GroupingSetMask{1} << j)) != 0) {
        node.activeKeys.push_back(static_cast<column_index_t>(j));
      }
    }
    node.naturalType = makeNaturalType(node);
    masks.push_back(node.activeKeysMask);
  }

  plan_ = buildDerivationPlan(masks, node_->childGroupedSet());

  // Keep the ordinary per-set soft threshold: coarse sets are often tiny and
  // should not be forced to flush merely because the operator owns several
  // grouping sets. Their combined allocation is governed separately by
  // operatorFlushTargetBytes_. If the extended limit is configured below the
  // initial limit, clamp the local threshold to the operator target.
  int32_t numBudgetedNodes{0};
  const auto initialNodeBudget = std::min(maxPartialAggregationMemoryUsage_, maxExtendedPartialAggregationMemoryUsage_);
  for (auto i = 0; i < numSets; ++i) {
    if (!plan_.bypass[i]) {
      nodes_[i].maxPartialBytes = initialNodeBudget;
      ++numBudgetedNodes;
    }
  }

  // Deriving a child from a parent drops the keys in P \ S. This is valid only
  // when the parent is a strict superset. buildDerivationPlan establishes that
  // invariant; keep an execution-boundary check so lattice changes cannot
  // silently produce wrong answers.
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
      // The parent's naturalType lists its active keys in ascending key order,
      // so this node's key j is at the position of j within the parent's
      // activeKeys. Both vectors are sorted; walk them together instead of
      // searching the parent from the beginning for every child key.
      const auto& parentKeys = nodes_[parent].activeKeys;
      node.deriveKeyChannels.reserve(node.activeKeys.size());
      size_t parentPosition{0};
      for (auto key : node.activeKeys) {
        while (parentPosition < parentKeys.size() && parentKeys[parentPosition] < key) {
          ++parentPosition;
        }
        VELOX_CHECK(
            parentPosition < parentKeys.size() && parentKeys[parentPosition] == key,
            "Grouping set {} has an active key its parent does not",
            i);
        node.deriveKeyChannels.push_back(static_cast<column_index_t>(parentPosition));
      }
      node.deriveAccBase = static_cast<column_index_t>(parentKeys.size());
    }
  }

  // Tables. The bypassed set gets none: rows arriving from the child partial
  // aggregate are already grouped at that grain, so we tag and forward them.
  for (auto i = 0; i < numSets; ++i) {
    auto& node = nodes_[i];
    if (plan_.bypass[i]) {
      node.plannedBypass = true;
      continue;
    }
    node.aggregates = makeAggregateInfos(i);
    for (const auto& info : node.aggregates) {
      VELOX_CHECK_NOT_NULL(info.function, "Aggregate info has no Aggregate instance");
    }

    makeGroupingSet(i);
  }

  addRuntimeStat("gsagg.numSets", RuntimeCounter(numSets));
  addRuntimeStat("gsagg.flushTargetBytes", RuntimeCounter(operatorFlushTargetBytes_, RuntimeCounter::Unit::kBytes));
  addRuntimeStat("gsagg.numBudgetedNodes", RuntimeCounter(numBudgetedNodes));
  addRuntimeStat("gsagg.abandonMinRows", RuntimeCounter(abandonPartialAggregationMinRows_));
  addRuntimeStat("gsagg.abandonMinPct", RuntimeCounter(abandonPartialAggregationMinPct_));
  addRuntimeStat("gsagg.abandonMinEvidenceCycles", RuntimeCounter(kAbandonMinEvidenceCycles));
  int32_t numRoots{0};
  for (auto i = 0; i < numSets; ++i) {
    if (plan_.parent[i] == kRawInputParent) {
      ++numRoots;
    }
  }
  // numRoots == numSets means the plan degenerated to a flat fan-out and the
  // raw input is scanned once per set.
  addRuntimeStat("gsagg.numLatticeRoots", RuntimeCounter(numRoots));
}

RowTypePtr MultiGroupingSetAggregation::makeNaturalType(const NodeState& node) const {
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
    types.push_back(resolveIntermediateType(node_->aggregates()[i].call->name(), node_->aggregates()[i].rawInputTypes));
  }
  return ROW(std::move(names), std::move(types));
}

std::vector<AggregateInfo> MultiGroupingSetAggregation::makeAggregateInfos(int32_t index) {
  auto& node = nodes_[index];
  // The GroupingSet is constructed over this node's own naturalType, so the
  // accumulator columns sit immediately after this node's active keys, both on
  // the way in and on the way out.
  const auto accBase = node.numActive();

  std::vector<AggregateInfo> infos;
  infos.reserve(numAggregates_);
  for (auto i = 0; i < numAggregates_; ++i) {
    const auto& aggregate = node_->aggregates()[i];
    // Function lookup uses the base name and raw input types. Merge behavior
    // comes from the GroupingSet flags, not a "_merge" companion name.
    const auto& name = aggregate.call->name();

    // Flush, bypass, and lattice derivation require decomposable aggregates.
    VELOX_CHECK_NULL(
        aggregate.mask,
        "GroupingSetAggregation does not support masked "
        "aggregates");
    VELOX_CHECK(aggregate.sortingKeys.empty(), "GroupingSetAggregation does not support order-sensitive aggregates");
    VELOX_CHECK(!aggregate.distinct, "GroupingSetAggregation does not support DISTINCT aggregates");

    AggregateInfo info;
    info.inputs = {static_cast<column_index_t>(accBase + i)};
    info.constantInputs = {nullptr};
    info.mask = std::nullopt;
    info.intermediateType = resolveIntermediateType(name, aggregate.rawInputTypes);
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
        node.naturalType->childAt(accBase + i)->equivalent(*info.intermediateType),
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
      // Use the inherited Operator reclaim guard and spill statistics.
      &nonReclaimableSection_,
      &operatorCtx_->driverCtx()->queryConfig(),
      pool(),
      spillStats_.get());
}

// Shaping

RowVectorPtr MultiGroupingSetAggregation::derive(int32_t index, const RowVectorPtr& source) const {
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

RowVectorPtr MultiGroupingSetAggregation::project(int32_t index, const RowVectorPtr& natural) const {
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
    if ((node.activeKeysMask & (GroupingSetMask{1} << j)) != 0) {
      children.push_back(natural->childAt(active));
      ++active;
    } else {
      children.push_back(BaseVector::createNullConstant(inputType->childAt(j), size, pool()));
    }
  }

  for (auto a = 0; a < numAggregates_; ++a) {
    children.push_back(natural->childAt(active + a));
  }

  children.push_back(BaseVector::createConstant(BIGINT(), Variant(node.gid), size, pool()));

  return std::make_shared<RowVector>(pool(), outputType_, /*nulls=*/nullptr, size, std::move(children));
}

// Input

bool MultiGroupingSetAggregation::needsInput() const {
  return !noMoreInput_ && drainStack_.empty() && outputQueue_.empty();
}

void MultiGroupingSetAggregation::feedChildren(
    int32_t index,
    const RowVectorPtr& natural,
    GroupingSetMask* touchedTableNodes) {
  for (auto child : plan_.children[index]) {
    feedNode(child, derive(child, natural), touchedTableNodes);
  }
}

void MultiGroupingSetAggregation::feedNode(
    int32_t index,
    const RowVectorPtr& natural,
    GroupingSetMask* touchedTableNodes) {
  auto& node = nodes_[index];
  node.numInputRows += natural->size();
  node.totalInputRows += natural->size();

  if (node.groupingSet == nullptr) {
    // Planned-bypass and dynamically abandoned nodes are identity operations
    // over intermediate states. Keep their row counts separate: one describes
    // the chosen plan, while the other measures an adaptive fallback.
    VELOX_DCHECK(node.plannedBypass || node.dynamicallyAbandoned);
    if (node.plannedBypass) {
      node.plannedBypassRows += natural->size();
      plannedBypassRows_ += natural->size();
    } else {
      node.dynamicAbandonRows += natural->size();
      dynamicAbandonRows_ += natural->size();
    }
    node.numOutputRows += natural->size();
    node.totalOutputRows += natural->size();
    outputQueue_.push_back(project(index, natural));
    feedChildren(index, natural, touchedTableNodes);
    return;
  }

  // Abandonment drops the GroupingSet before completeNodeDrain() returns.
  VELOX_DCHECK(!node.dynamicallyAbandoned);

  node.groupingSet->addInput(natural, /*mayPushdown=*/false);
  if (touchedTableNodes != nullptr) {
    VELOX_DCHECK_LT(index, kMaxGroupingSets);
    *touchedTableNodes |= GroupingSetMask{1} << index;
  }
  // A stream prefix does not identify the steady-state reduction ratio.
  // Abandonment is evaluated only after a completed pressure drain.
}

void MultiGroupingSetAggregation::maybeAbandonAfterDrain(int32_t index) {
  auto& node = nodes_[index];
  // A grand-total set holds one group and should never abandon.
  if (node.numActive() == 0 || node.groupingSet == nullptr || node.dynamicallyAbandoned) {
    return;
  }
  // Only pressure-driven drains describe the steady-state cost of keeping the
  // table.
  if (node.reason != FlushReason::kSoftBudget && node.reason != FlushReason::kHardCap &&
      node.reason != FlushReason::kOperatorTarget) {
    return;
  }
  // Pool several completed cycles to smooth threshold oscillation. Empty
  // cycles are neither evidence nor a valid denominator.
  if (node.numInputRows == 0) {
    return;
  }
  node.abandonWindowInRows += node.numInputRows;
  node.abandonWindowOutRows += node.numOutputRows;
  ++node.abandonWindowCycles;
  // Match HashAggregation's row-count policy while retaining the stronger
  // multi-cycle evidence requirement. The threshold applies to the current
  // evidence window, not stale lifetime rows from an earlier healthy window.
  if (node.abandonWindowCycles < kAbandonMinEvidenceCycles ||
      node.abandonWindowInRows <= abandonPartialAggregationMinRows_) {
    return;
  }
  const auto pooledPct = static_cast<int64_t>(
      static_cast<FlushableBytes>(node.abandonWindowOutRows) * 100 /
      static_cast<FlushableBytes>(node.abandonWindowInRows));
  if (pooledPct < abandonPartialAggregationMinPct_) {
    // The table is reducing enough rows; begin a fresh evidence window.
    node.abandonWindowCycles = 0;
    node.abandonWindowInRows = 0;
    node.abandonWindowOutRows = 0;
    return;
  }
  node.dynamicallyAbandoned = true;
  const auto prefix = fmt::format("gsagg.set{}", index);
  addRuntimeStat(prefix + ".abandoned", RuntimeCounter(1));
  addRuntimeStat(prefix + ".abandonInputRows", RuntimeCounter(node.abandonWindowInRows));
  addRuntimeStat(prefix + ".abandonOutputRows", RuntimeCounter(node.abandonWindowOutRows));
  addRuntimeStat(prefix + ".abandonPct", RuntimeCounter(pooledPct));
}

void MultiGroupingSetAggregation::addInput(RowVectorPtr input) {
  VELOX_CHECK(drainStack_.empty());
  VELOX_CHECK(!noMoreInput_);

  // Operators must not emit empty vectors. Keep receivedInput_ false so empty
  // partitions also skip the final grand-total sweep.
  if (input->size() == 0) {
    return;
  }
  receivedInput_ = true;

  // Every root must consume the whole batch before pressure can start a drain.
  // Stopping halfway would leave later roots missing rows, with no input resume
  // cursor from which to recover.
  for (auto i : plan_.order) {
    if (plan_.parent[i] == kRawInputParent) {
      feedNode(i, derive(i, input));
    }
  }

  // Now pressure. The scan first honors node-local hard and soft limits in
  // topological order. If only the shared target is exceeded, it drains the
  // largest eligible table.
  maybeStartPressureDrain();
}

void MultiGroupingSetAggregation::noMoreInput() {
  // Do not restart the final sweep on a repeated notification.
  if (noMoreInput_) {
    return;
  }
  Operator::noMoreInput();

  // A drain in flight owns its iterator, so the final sweep can be initialized
  // now. getOutput() finishes drainStack_ before advancing the sweep.
  //
  // Spark emits no grouping-set rows for empty input. A global Velox
  // GroupingSet would otherwise synthesize an identity row per empty driver.
  if (!receivedInput_) {
    finalSweepPos_ = static_cast<int32_t>(plan_.order.size());
    recordFinalStats();
    return;
  }
  finalSweepPos_ = 0;
}

// Flush policy

int64_t MultiGroupingSetAggregation::clampFlushableBytes(FlushableBytes bytes) {
  const auto max = static_cast<FlushableBytes>(std::numeric_limits<int64_t>::max());
  return bytes > max ? std::numeric_limits<int64_t>::max() : static_cast<int64_t>(bytes);
}

MultiGroupingSetAggregation::FlushableBytes MultiGroupingSetAggregation::flushableNodeBytes() const {
  FlushableBytes total{0};
  for (const auto& node : nodes_) {
    if (node.groupingSet != nullptr) {
      total += static_cast<FlushableBytes>(node.groupingSet->allocatedBytes());
    }
  }
  return total;
}

bool MultiGroupingSetAggregation::overFlushTarget() const {
  return flushableNodeBytes() > static_cast<FlushableBytes>(operatorFlushTargetBytes_);
}

MultiGroupingSetAggregation::FlushReason MultiGroupingSetAggregation::localFlushReason(
    int32_t index,
    FlushableBytes& cachedFlushableBytes) {
  auto& node = nodes_[index];
  if (node.groupingSet == nullptr || node.draining) {
    return FlushReason::kNone;
  }
  if (node.dynamicallyAbandoned) {
    // Abandonment normally drops the table before the next pressure scan.
    return FlushReason::kNone;
  }
  if (exceedsHardCap(index)) {
    return FlushReason::kHardCap;
  }

  if (node.numActive() == 0) {
    // GroupingSet::isPartialFull() only examines hash tables. A global
    // accumulator has no table, but its rows and string allocator may own
    // external aggregate state. It is safe to emit that intermediate state
    // and call resetGlobalAggregation() after the drain.
    return node.groupingSet->allocatedBytes() > static_cast<uint64_t>(node.maxPartialBytes) ? FlushReason::kSoftBudget
                                                                                            : FlushReason::kNone;
  }

  // isPartialFull() may change hash-table mode and therefore allocation size.
  // Keep the one-pass total exact for later nodes without rescanning nodes_.
  const auto bytesBefore = static_cast<FlushableBytes>(node.groupingSet->allocatedBytes());
  const bool partialFull = node.groupingSet->isPartialFull(node.maxPartialBytes);
  const auto bytesAfter = static_cast<FlushableBytes>(node.groupingSet->allocatedBytes());
  if (bytesAfter >= bytesBefore) {
    cachedFlushableBytes += bytesAfter - bytesBefore;
  } else {
    VELOX_DCHECK_GE(cachedFlushableBytes, bytesBefore - bytesAfter);
    cachedFlushableBytes -= bytesBefore - bytesAfter;
  }
  if (partialFull) {
    return FlushReason::kSoftBudget;
  }
  return FlushReason::kNone;
}

bool MultiGroupingSetAggregation::exceedsHardCap(int32_t index) const {
  const auto& node = nodes_[index];
  if (node.groupingSet == nullptr || node.draining || node.dynamicallyAbandoned) {
    return false;
  }
  const auto allocatedBytes = node.groupingSet->allocatedBytes();
  if (node.maxPartialBytes <= 0) {
    return allocatedBytes > 0;
  }
  const auto budget = static_cast<uint64_t>(node.maxPartialBytes);
  if (budget > std::numeric_limits<uint64_t>::max() / kHardCapMultiple) {
    return false;
  }
  return allocatedBytes > kHardCapMultiple * budget;
}

bool MultiGroupingSetAggregation::maybeStartPressureDrain() {
  VELOX_DCHECK(drainStack_.empty());

  auto flushableBytes = flushableNodeBytes();
  const auto targetBytes = static_cast<FlushableBytes>(operatorFlushTargetBytes_);
  const auto recordPressureSample = [&]() {
    peakSampledFlushableBytes_ = std::max(peakSampledFlushableBytes_, clampFlushableBytes(flushableBytes));
    const auto overshoot = flushableBytes > targetBytes ? clampFlushableBytes(flushableBytes - targetBytes) : 0;
    maxTargetOvershootBytes_ = std::max(maxTargetOvershootBytes_, overshoot);
  };

  for (auto i : plan_.order) {
    const auto reason = localFlushReason(i, flushableBytes);
    if (reason != FlushReason::kNone) {
      recordPressureSample();
      pushDrain(i, reason);
      return true;
    }
  }

  recordPressureSample();
  if (flushableBytes <= targetBytes) {
    return false;
  }

  // The target is shared, so draining the first node in topological order can
  // make little progress under skew. Pick the largest grouping-set allocation
  // that actually has state to emit. Ties retain topological order.
  int32_t candidate{-1};
  uint64_t candidateBytes{0};
  for (auto i : plan_.order) {
    const auto& node = nodes_[i];
    if (!nodeHasOutput(i)) {
      continue;
    }
    const auto bytes = node.groupingSet->allocatedBytes();
    if (candidate == -1 || bytes > candidateBytes) {
      candidate = i;
      candidateBytes = bytes;
    }
  }
  if (candidate == -1) {
    // No live state needs materialization. Free retained empty storage now;
    // returning while still over target would let needsInput() accept another
    // batch with unresolved, reclaimable pressure.
    for (auto i : plan_.order) {
      auto& node = nodes_[i];
      if (node.groupingSet == nullptr || node.draining || nodeHasOutput(i) || node.groupingSet->allocatedBytes() == 0) {
        continue;
      }
      if (node.numActive() == 0) {
        node.groupingSet->resetGlobalAggregation();
      } else {
        node.groupingSet->resetTable(/*freeTable=*/true);
      }
    }
    VELOX_CHECK(
        flushableNodeBytes() <= targetBytes, "Grouping-set operator retained empty allocations above its target");
    return false;
  }

  pushDrain(candidate, FlushReason::kOperatorTarget);
  ++numOperatorTargetFlushes_;
  return true;
}

void MultiGroupingSetAggregation::maybeGrowBudget(int32_t index) {
  auto& node = nodes_[index];
  if (node.groupingSet == nullptr || node.dynamicallyAbandoned) {
    return;
  }
  if (node.reason != FlushReason::kSoftBudget && node.reason != FlushReason::kHardCap) {
    return;
  }
  if (overFlushTarget()) {
    return;
  }
  if (node.maxPartialBytes >= maxExtendedPartialAggregationMemoryUsage_) {
    return;
  }

  // Doubling from zero would never make progress, so a zero-budget
  // configuration grows by one byte after its first successful drain.
  int64_t grown;
  if (node.maxPartialBytes <= 0) {
    grown = std::min<int64_t>(1, maxExtendedPartialAggregationMemoryUsage_);
  } else if (node.maxPartialBytes > maxExtendedPartialAggregationMemoryUsage_ / 2) {
    grown = maxExtendedPartialAggregationMemoryUsage_;
  } else {
    grown = node.maxPartialBytes * 2;
  }

  // MemoryPool rounds each maybeReserve() request to an 8 MiB quantum. Reuse
  // unused operator-pool reservation before asking for more; otherwise several
  // small node-local doublings would each pin a full quantum. Reserve only the
  // headroom that can be useful before the shared flush target is reached.
  const auto flushableBytes = flushableNodeBytes();
  const auto nodeBytes = static_cast<int64_t>(std::min<uint64_t>(
      node.groupingSet->allocatedBytes(), static_cast<uint64_t>(std::numeric_limits<int64_t>::max())));
  const auto nodeHeadroom = std::max<int64_t>(grown - std::min(grown, nodeBytes), 0);
  const auto targetBytes = static_cast<FlushableBytes>(operatorFlushTargetBytes_);
  const auto targetHeadroom = flushableBytes < targetBytes ? static_cast<int64_t>(targetBytes - flushableBytes) : 0;
  const auto requiredHeadroom = std::min(nodeHeadroom, targetHeadroom);
  const auto availableReservation = std::max<int64_t>(pool()->availableReservation(), 0);
  const auto toReserve = std::max<int64_t>(requiredHeadroom - availableReservation, 0);
  if (toReserve > 0) {
    const auto reservedBefore = pool()->reservedBytes();
    if (!pool()->maybeReserve(static_cast<uint64_t>(toReserve))) {
      return;
    }
    const auto reservedAfter = pool()->reservedBytes();
    const auto reservationDelta = reservedAfter > reservedBefore
        ? static_cast<int64_t>(std::min<uint64_t>(
              reservedAfter - reservedBefore, static_cast<uint64_t>(std::numeric_limits<int64_t>::max())))
        : 0;
    growthReservationBytes_ = reservationDelta > std::numeric_limits<int64_t>::max() - growthReservationBytes_
        ? std::numeric_limits<int64_t>::max()
        : growthReservationBytes_ + reservationDelta;
    ++numGrowthReservationCalls_;
  }
  node.maxPartialBytes = grown;
}

// Drain

void MultiGroupingSetAggregation::pushDrain(int32_t index, FlushReason reason) {
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
    // A global GroupingSet reports zero table rows even with a live
    // accumulator.
    return node.numInputRows > 0;
  }
  return node.groupingSet->numRows() > 0;
}

RowVectorPtr MultiGroupingSetAggregation::getOutput() {
  // Emit queued bypass/pass-through rows first.
  if (!outputQueue_.empty()) {
    auto result = std::move(outputQueue_.front());
    outputQueue_.pop_front();
    return result;
  }

  // Without an active drain, either keep accumulating or advance the final
  // sweep.
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

  // GroupingSet::getOutput writes into a caller-supplied RowVector.
  const auto maxOutputRows = (node.numActive() == 0) ? 1 : outputBatchRows();
  if (node.output != nullptr) {
    VectorPtr reusable = std::move(node.output);
    BaseVector::prepareForReuse(reusable, maxOutputRows);
    node.output = std::static_pointer_cast<RowVector>(reusable);
  } else {
    node.output = std::static_pointer_cast<RowVector>(BaseVector::create(node.naturalType, maxOutputRows, pool()));
  }
  // project() may still share these children downstream; prepareForReuse
  // reallocates when the vector is not uniquely owned.

  // preferredOutputBatchBytes() is uint64_t but GroupingSet::getOutput's
  // maxOutputBytes parameter is int32_t. Clamp explicitly rather than rely on
  // an implicit narrowing conversion, which would wrap to a negative cap if the
  // config were ever raised past 2 GiB.
  const auto maxOutputBytes = static_cast<int32_t>(std::min<uint64_t>(
      operatorCtx_->driverCtx()->queryConfig().preferredOutputBatchBytes(),
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max())));

  // Pass the member because getOutput may replace the RowVectorPtr.
  const bool hasMore = node.groupingSet->getOutput(maxOutputRows, maxOutputBytes, node.iterator, node.output);
  RowVectorPtr natural = node.output;

  if (!hasMore || natural == nullptr || natural->size() == 0) {
    completeNodeDrain(index);
    return nullptr;
  }

  node.numOutputRows += natural->size();
  node.totalOutputRows += natural->size();

  // Each drained batch is used twice: once as operator output (with this set's
  // gid and null mask), once as input to every child. A child may merge partial
  // installments independently, so parent and child drains may be interleaved.
  auto output = project(index, natural);

  // Parent output is hashed into child tables while getOutput() is running.
  // A direct child can be a pass-through after dynamic abandonment, so collect
  // the live tables reached by the full recursive feed. Checking only the
  // direct child would miss a deeper table and let it grow without a hard-cap
  // check until the parent finished draining.
  GroupingSetMask touchedTableNodes{0};
  GroupingSetMask directChildren{0};
  for (auto child : plan_.children[index]) {
    directChildren |= GroupingSetMask{1} << child;
    feedNode(child, derive(child, natural), &touchedTableNodes);
  }

  // A table fed during an ancestor's drain may overshoot its soft budget until
  // that drain completes. Only the hard cap nests a drain. Scan topological
  // order so every pushed node is strictly later than the current parent and
  // drainStack_ remains rank-ordered even when several branches cross the cap
  // in one derived batch.
  for (auto candidate : plan_.order) {
    const auto candidateBit = GroupingSetMask{1} << candidate;
    if ((touchedTableNodes & candidateBit) != 0 && exceedsHardCap(candidate)) {
      pushDrain(candidate, FlushReason::kHardCap);
      ++numNestedDrains_;
      if ((directChildren & candidateBit) == 0) {
        ++numTransitiveHardCapDrains_;
      }
    }
  }

  return output;
}

void MultiGroupingSetAggregation::completeNodeDrain(int32_t index) {
  auto& node = nodes_[index];
  recordNodeStats(index);
  // Evaluate abandon before resetting cycle counters or growing the budget.
  maybeAbandonAfterDrain(index);
  maybeGrowBudget(index);

  // Pinned Velox frees a hash table when getOutput() reaches exhaustion. Make
  // that contract explicit instead of pretending a slot array can be retained.
  // A global set has no hash table; reset its reusable row and external
  // aggregate state after its intermediate result has been emitted.
  if (node.numActive() == 0) {
    node.groupingSet->resetGlobalAggregation();
  } else {
    node.groupingSet->resetTable(/*freeTable=*/true);
  }
  if (node.dynamicallyAbandoned) {
    // From here on this node is a pure projection; drop the GroupingSet so
    // pressure scans stop reporting it.
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

  // A parent drain can fill several descendants. Keep draining at this safe
  // boundary so needsInput() cannot accept another batch while an actionable
  // table remains above the shared target.
  if (drainStack_.empty() && !noMoreInput_) {
    maybeStartPressureDrain();
  }
}

bool MultiGroupingSetAggregation::isFinished() {
  const bool finished = noMoreInput_ && drainStack_.empty() &&
      finalSweepPos_ >= static_cast<int32_t>(plan_.order.size()) && outputQueue_.empty();
  if (finished) {
    recordFinalStats();
  }
  return finished;
}

// Reclaim

bool MultiGroupingSetAggregation::reclaimableBytes(uint64_t& reclaimableBytes) const {
  // Report only retained allocations from empty, idle grouping sets. Live
  // accumulator state cannot be reclaimed safely.
  reclaimableBytes = 0;
  if (nonReclaimableSection_) {
    return false;
  }
  for (auto i = 0; i < static_cast<int32_t>(nodes_.size()); ++i) {
    const auto& node = nodes_[i];
    if (node.groupingSet == nullptr || node.draining || nodeHasOutput(i)) {
      continue;
    }
    const auto bytes = node.groupingSet->allocatedBytes();
    reclaimableBytes = bytes > std::numeric_limits<uint64_t>::max() - reclaimableBytes
        ? std::numeric_limits<uint64_t>::max()
        : reclaimableBytes + bytes;
  }
  return true;
}

void MultiGroupingSetAggregation::reclaim(uint64_t /*targetBytes*/, memory::MemoryReclaimer::Stats& stats) {
  // Reclaim must not allocate. Free only empty tables; draining live state can
  // allocate output vectors and grow descendant tables on this same pool.
  // MemoryReclaimer::run records reclaimed bytes and execution time.
  if (nonReclaimableSection_) {
    ++stats.numNonReclaimableAttempts;
    return;
  }

  addRuntimeStat("gsagg.reclaim.count", RuntimeCounter(1));
  const auto reservedAtEntry = pool()->reservedBytes();

  // Free retained allocations from completed flushes and empty global cycles.
  for (auto i = 0; i < static_cast<int32_t>(nodes_.size()); ++i) {
    auto& node = nodes_[i];
    if (node.groupingSet == nullptr || node.draining || nodeHasOutput(i)) {
      continue;
    }
    if (node.numActive() == 0) {
      if (node.groupingSet->allocatedBytes() > 0) {
        node.groupingSet->resetGlobalAggregation();
      }
    } else if (node.groupingSet->allocatedBytes() > 0) {
      node.groupingSet->resetTable(/*freeTable=*/true);
    }
  }
  pool()->release();

  const auto reservedAfter = pool()->reservedBytes();
  const auto reclaimed = reservedAtEntry > reservedAfter
      ? static_cast<int64_t>(std::min<uint64_t>(
            reservedAtEntry - reservedAfter, static_cast<uint64_t>(std::numeric_limits<int64_t>::max())))
      : 0;
  addRuntimeStat("gsagg.reclaim.bytes", RuntimeCounter(std::max<int64_t>(reclaimed, 0), RuntimeCounter::Unit::kBytes));
}

// Stats and teardown

void MultiGroupingSetAggregation::recordNodeStats(int32_t index) {
  const auto& node = nodes_[index];
  const auto prefix = fmt::format("gsagg.set{}", index);
  addRuntimeStat(prefix + ".inputRows", RuntimeCounter(node.numInputRows));
  addRuntimeStat(prefix + ".outputRows", RuntimeCounter(node.numOutputRows));
  addRuntimeStat(prefix + ".flushTimes", RuntimeCounter(1));
  addRuntimeStat(prefix + ".flushRowCount", RuntimeCounter(node.numOutputRows));
  if (node.reason == FlushReason::kSoftBudget || node.reason == FlushReason::kHardCap ||
      node.reason == FlushReason::kOperatorTarget) {
    // WholeStageResultIterator already transports this standard aggregation
    // metric to the JVM. Expose pressure-emitted rows under that name so the
    // tagged Expand can surface the canary signal without widening the JNI
    // metrics ABI. End-of-input drains are deliberately excluded.
    addRuntimeStat("flushRowCount", RuntimeCounter(node.numOutputRows));
  }
  if (node.numInputRows > 0) {
    const auto aggregationPct = static_cast<int64_t>(
        static_cast<FlushableBytes>(node.numOutputRows) * 100 / static_cast<FlushableBytes>(node.numInputRows));
    addRuntimeStat(prefix + ".aggregationPct", RuntimeCounter(aggregationPct));
  }
  if (node.groupingSet != nullptr) {
    addRuntimeStat(
        prefix + ".hashTableBytes", RuntimeCounter(node.groupingSet->allocatedBytes(), RuntimeCounter::Unit::kBytes));
  }
}

void MultiGroupingSetAggregation::recordFinalStats() {
  if (finalStatsRecorded_) {
    return;
  }
  finalStatsRecorded_ = true;

  int32_t plannedBypassSets{0};
  int32_t dynamicallyAbandonedSets{0};
  for (auto i = 0; i < static_cast<int32_t>(nodes_.size()); ++i) {
    const auto& node = nodes_[i];
    const auto prefix = fmt::format("gsagg.set{}", i);

    // recordNodeStats() reports table-backed cycles as deltas. Planned bypasses
    // never drain, and dynamically abandoned nodes may forward more rows after
    // their last drain, so close the delta accounting for those identity
    // phases here. The explicit totals provide a single authoritative lifetime
    // value as well.
    if (node.plannedBypass || node.dynamicallyAbandoned) {
      addRuntimeStat(prefix + ".inputRows", RuntimeCounter(node.numInputRows));
      addRuntimeStat(prefix + ".outputRows", RuntimeCounter(node.numOutputRows));
    }
    addRuntimeStat(prefix + ".totalInputRows", RuntimeCounter(node.totalInputRows));
    addRuntimeStat(prefix + ".totalOutputRows", RuntimeCounter(node.totalOutputRows));
    addRuntimeStat(prefix + ".plannedBypassRows", RuntimeCounter(node.plannedBypassRows));
    addRuntimeStat(prefix + ".dynamicAbandonRows", RuntimeCounter(node.dynamicAbandonRows));
    addRuntimeStat(prefix + ".plannedBypass", RuntimeCounter(node.plannedBypass ? 1 : 0));
    addRuntimeStat(prefix + ".dynamicallyAbandoned", RuntimeCounter(node.dynamicallyAbandoned ? 1 : 0));

    plannedBypassSets += node.plannedBypass ? 1 : 0;
    dynamicallyAbandonedSets += node.dynamicallyAbandoned ? 1 : 0;
  }

  // Preserve the original metric as an alias for planned bypass rows.
  addRuntimeStat("gsagg.bypassRows", RuntimeCounter(plannedBypassRows_));
  addRuntimeStat("gsagg.plannedBypassRows", RuntimeCounter(plannedBypassRows_));
  addRuntimeStat("gsagg.dynamicAbandonRows", RuntimeCounter(dynamicAbandonRows_));
  // Same standard metric used by HashAggregation. On this operator it counts
  // rows identity-forwarded after the adaptive per-set table was abandoned.
  addRuntimeStat("abandonedPartialAggregationRows", RuntimeCounter(dynamicAbandonRows_));
  addRuntimeStat("gsagg.plannedBypassSets", RuntimeCounter(plannedBypassSets));
  addRuntimeStat("gsagg.dynamicallyAbandonedSets", RuntimeCounter(dynamicallyAbandonedSets));
  addRuntimeStat("gsagg.flushes", RuntimeCounter(numFlushes_));
  addRuntimeStat("gsagg.nestedDrains", RuntimeCounter(numNestedDrains_));
  addRuntimeStat("gsagg.transitiveHardCapDrains", RuntimeCounter(numTransitiveHardCapDrains_));
  addRuntimeStat("gsagg.operatorTargetFlushes", RuntimeCounter(numOperatorTargetFlushes_));
  addRuntimeStat(
      "gsagg.peakSampledFlushableBytes", RuntimeCounter(peakSampledFlushableBytes_, RuntimeCounter::Unit::kBytes));
  addRuntimeStat(
      "gsagg.maxTargetOvershootBytes", RuntimeCounter(maxTargetOvershootBytes_, RuntimeCounter::Unit::kBytes));
  addRuntimeStat("gsagg.growthReservationCalls", RuntimeCounter(numGrowthReservationCalls_));
  addRuntimeStat("gsagg.growthReservationBytes", RuntimeCounter(growthReservationBytes_, RuntimeCounter::Unit::kBytes));
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
  std::unique_ptr<Operator> toOperator(DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node) override {
    if (auto gsNode = std::dynamic_pointer_cast<const GroupingSetAggregationNode>(node)) {
      return std::make_unique<MultiGroupingSetAggregation>(id, ctx, gsNode);
    }
    return nullptr;
  }
};
} // namespace

void registerMultiGroupingSetAggregation() {
  Operator::registerOperator(std::make_unique<GroupingSetAggregationTranslator>());

  // Plan-node serde. core::PlanNode::registerSerDe() cannot register this node
  // because the node lives in exec/ (registering it there would invert the
  // core/ -> exec/ dependency), so the exec-side registration hook does it.
  // Guarded against a double Register(), which the registry treats as a fatal
  // duplicate-key error.
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  if (!registry.Has("GroupingSetAggregationNode")) {
    registry.Register("GroupingSetAggregationNode", GroupingSetAggregationNode::create);
  }
}

} // namespace facebook::velox::exec
