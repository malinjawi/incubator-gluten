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
#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <vector>

#include "operators/plannodes/GroupingSetAggregationNode.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/exec/GroupingSet.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/// Merges intermediate aggregate states for a grouping-set derivation forest.
///
/// Each set owns a Velox GroupingSet over its active keys. Roots consume the
/// operator input; every other set consumes its parent's emitted states. A
/// prefix ROLLUP is therefore a cascade from the finest grain to the grand
/// total. Each node owns its drain iterator, allowing parent drains to pause
/// safely while a child drains.
class MultiGroupingSetAggregation : public Operator {
 public:
  MultiGroupingSetAggregation(int32_t operatorId, DriverCtx* driverCtx, const GroupingSetAggregationNodePtr& node);

  /// Builds pool-backed per-set state. Pool allocation must not happen in the
  /// constructor because Velox initializes the memory reclaimer here.
  void initialize() override;

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override;

  /// Reclaim only frees retained allocations from empty grouping sets. It never
  /// materializes live state while the memory arbitrator is active.
  bool canReclaim() const override {
    return true;
  }

  /// Reports only bytes reclaim() can release immediately.
  bool reclaimableBytes(uint64_t& reclaimableBytes) const override;

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats) override;

 private:
  using FlushableBytes = unsigned __int128;

  /// Records why a node drains for pressure handling and finalization.
  enum class FlushReason {
    kNone,
    /// Over its own maxPartialBytes.
    kSoftBudget,
    /// Over kHardCapMultiple * maxPartialBytes. The only reason that may nest a
    /// drain inside a parent's drain.
    kHardCap,
    /// Flushable grouping-set allocations exceed the operator-wide target.
    kOperatorTarget,
    /// Final sweep.
    kEndOfInput,
  };

  /// Per grouping-set state.
  struct NodeState {
    /// Copied from the node.
    int64_t gid{0};
    GroupingSetMask activeKeysMask{0};

    /// Channels, into the operator input, of this set's active keys,
    /// ascending. Size == numActiveKeys.
    std::vector<column_index_t> activeKeys;

    /// ROW(<active keys>, acc1..accm), used for both GroupingSet input and
    /// extracted output.
    RowTypePtr naturalType;

    /// Column selection that reshapes a batch of the parent's naturalType (or
    /// of the operator's input type, for a root) into this node's naturalType.
    /// Both are fixed at initialize() time.
    std::vector<column_index_t> deriveKeyChannels;
    column_index_t deriveAccBase{0};

    /// Null for a planned bypass and for a node whose dynamically abandoned
    /// table has been drained and dropped.
    std::unique_ptr<GroupingSet> groupingSet;

    /// Staging only. Aggregate instances are stateful (setAllocator /
    /// setOffsets bind them to one RowContainer), so every node needs its own
    /// set -- m * n live Aggregate instances. The GroupingSet constructor takes
    /// these by rvalue reference and moves them, so this vector is empty after
    /// makeGroupingSet(). Do not read it back.
    std::vector<AggregateInfo> aggregates;

    /// This node's own drain cursor. Per-node ownership is the whole point:
    /// parking a half-consumed iterator while something else drains is free.
    RowContainerIterator iterator;

    /// Caller-allocated destination for GroupingSet::getOutput -- that method
    /// writes into a RowVector the caller supplies, it does not create one.
    /// Shaped as naturalType, reused across drain batches.
    RowVectorPtr output;

    /// Reduction bookkeeping, feeding the abandon valve and the runtime stats.
    /// Reset on every flush.
    int64_t numInputRows{0};
    int64_t numOutputRows{0};

    /// Lifetime row accounting. Unlike the per-flush counters above, these
    /// survive table drains and include identity-forwarded rows.
    int64_t totalInputRows{0};
    int64_t totalOutputRows{0};

    /// Pooled evidence for the abandon decision, accumulated over saturated
    /// pressure cycles and reset whenever the pooled ratio comes back healthy.
    /// See maybeAbandonAfterDrain().
    int64_t abandonWindowInRows{0};
    int64_t abandonWindowOutRows{0};
    int32_t abandonWindowCycles{0};

    /// Node-local soft threshold. Every node starts at the ordinary partial
    /// aggregation limit; the operator-wide target controls their combined
    /// allocation.
    int64_t maxPartialBytes{0};

    /// Planner-selected identity lane. No hash table is ever constructed.
    bool plannedBypass{false};

    /// Once dynamically abandoned this node drops its table and becomes a pure
    /// projection. A merge node has isRawInput = false, so pass-through is the
    /// identity on state. Its natural input and output types are identical.
    bool dynamicallyAbandoned{false};

    /// Identity-forwarded rows, split by why the node has no table.
    int64_t plannedBypassRows{0};
    int64_t dynamicAbandonRows{0};

    /// Whether this node is currently on drainStack_.
    bool draining{false};

    /// Why it was pushed.
    FlushReason reason{FlushReason::kNone};

    int32_t numActive() const {
      return static_cast<int32_t>(activeKeys.size());
    }
  };

  /// ROW(<active keys of set i>, acc1..accm).
  RowTypePtr makeNaturalType(const NodeState& node) const;

  /// Builds AggregateInfos directly over this set's natural input type.
  std::vector<AggregateInfo> makeAggregateInfos(int32_t index);

  /// Constructs nodes_[index].groupingSet over nodes_[index].naturalType.
  void makeGroupingSet(int32_t index);

  /// Reshapes a batch of the parent's naturalType (or the operator's input
  /// type) into node `index`'s naturalType. Pure column selection: one
  /// reference copy per column, no data movement, arbitrary lattice.
  RowVectorPtr derive(int32_t index, const RowVectorPtr& source) const;

  /// Projects a natural batch into the uniform key/state/gid output shape.
  /// Active keys and states are shared; masked keys and gid are constants.
  RowVectorPtr project(int32_t index, const RowVectorPtr& natural) const;

  /// Feeds one naturalType-shaped batch into node `index`, honouring the bypass
  /// and abandon valves, and recursing into its children when the node is a
  /// pass-through. When supplied, touchedTableNodes marks every live table
  /// actually fed through that recursion.
  void feedNode(int32_t index, const RowVectorPtr& natural, GroupingSetMask* touchedTableNodes = nullptr);

  /// derive() + feedNode() for every child of `index`.
  void feedChildren(int32_t index, const RowVectorPtr& natural, GroupingSetMask* touchedTableNodes = nullptr);

  /// Evaluates abandon only after a pressure-driven drain. Completed drain
  /// cycles measure the sustained output/input ratio without the prefix-sample
  /// bias of HashAggregation's early-abandon heuristic. Evidence is pooled
  /// until both the cycle and QueryConfig row minima are satisfied before
  /// making the irreversible decision.
  void maybeAbandonAfterDrain(int32_t index);

  /// Raises one node's local threshold after a useful pressure drain. Any
  /// explicit pool reservation is coordinated against operator-wide headroom,
  /// so several small node-local doublings reuse one reservation quantum.
  void maybeGrowBudget(int32_t index);

  /// Not const: GroupingSet::isPartialFull() may rehash, which is exactly the
  /// self-healing behaviour we want it to have. cachedFlushableBytes is the
  /// one-pass allocation snapshot for this pressure scan; this method adjusts
  /// it if isPartialFull() changes the node's allocation.
  FlushReason localFlushReason(int32_t index, FlushableBytes& cachedFlushableBytes);

  /// Side-effect-free hard-cap test used while a parent drain feeds a child.
  /// A cascade must not run the child's soft or operator-wide pressure policy.
  bool exceedsHardCap(int32_t index) const;

  /// At a safe scheduling boundary, arms one local-pressure drain or, when the
  /// shared target is exceeded, the largest eligible table. Returns true when
  /// a drain was started.
  bool maybeStartPressureDrain();

  /// Exact wide sum of allocatedBytes() over all live grouping sets. The
  /// 128-bit accumulator prevents a multi-table sum from overflowing.
  FlushableBytes flushableNodeBytes() const;

  bool overFlushTarget() const;

  /// Runtime counters are signed 64-bit; clamp only at that reporting boundary.
  static int64_t clampFlushableBytes(FlushableBytes bytes);

  /// Whether this node still holds state worth draining.
  bool nodeHasOutput(int32_t index) const;

  void pushDrain(int32_t index, FlushReason reason);

  /// Pulls one batch out of the node on top of drainStack_, projects it for
  /// output and feeds it to that node's children. Returns nullptr when the node
  /// is exhausted (and pops it).
  RowVectorPtr advanceDrain();

  void completeNodeDrain(int32_t index);

  void recordNodeStats(int32_t index);

  void recordFinalStats();

  /// A child may exceed its soft budget while its parent drains, but crossing
  /// this multiple forces a nested drain.
  static constexpr int64_t kHardCapMultiple = 4;

  /// Pressure cycles required before an abandon decision.
  static constexpr int32_t kAbandonMinEvidenceCycles = 3;

  const GroupingSetAggregationNodePtr node_;
  const int32_t numKeys_;
  const int32_t numAggregates_;

  /// Cached QueryConfig values.
  const int64_t maxPartialAggregationMemoryUsage_;
  const int64_t maxExtendedPartialAggregationMemoryUsage_;
  const int32_t abandonPartialAggregationMinRows_;
  const int32_t abandonPartialAggregationMinPct_;

  /// Operator-wide target for all flushable grouping-set allocations,
  /// including global aggregate state. A full input batch, hash-table capacity
  /// growth, and a parent-to-child drain batch can
  /// temporarily overshoot it; maybeStartPressureDrain() restores the
  /// boundary invariant before needsInput() becomes true.
  const int64_t operatorFlushTargetBytes_;

  /// The derivation forest. Built once in initialize().
  DerivationPlan plan_;

  /// One entry per grouping set. plan_.order carries topological order.
  std::vector<NodeState> nodes_;

  // makeGroupingSet() uses Operator's inherited nonReclaimableSection_ and
  // spillStats_ members.

  /// Bypass-lane rows, pass-through rows and parked flush output. Bounded by
  /// needsInput() returning false while non-empty.
  std::deque<RowVectorPtr> outputQueue_;

  /// Drains in flight, innermost last. Strictly increasing in topological rank,
  /// hence bounded by the number of grouping sets. Empty == accumulating.
  std::vector<int32_t> drainStack_;

  /// Index into plan_.order. Negative while accumulating; >= order.size() means
  /// the final sweep is complete.
  int32_t finalSweepPos_{-1};

  /// Whether this driver received any non-empty input. Prevents a global
  /// GroupingSet from synthesizing an identity row for an empty partition.
  bool receivedInput_{false};

  /// Node-row operations on identity lanes. A row forwarded through two
  /// dynamically abandoned levels is intentionally counted twice.
  int64_t plannedBypassRows_{0};
  int64_t dynamicAbandonRows_{0};
  int32_t numFlushes_{0};
  int32_t numNestedDrains_{0};
  int32_t numTransitiveHardCapDrains_{0};
  int32_t numOperatorTargetFlushes_{0};
  int32_t numGrowthReservationCalls_{0};
  int64_t growthReservationBytes_{0};
  int64_t peakSampledFlushableBytes_{0};
  int64_t maxTargetOvershootBytes_{0};
  bool finalStatsRecorded_{false};
};

/// Registers the grouping-set aggregation translator with the Velox driver.
void registerMultiGroupingSetAggregation();

} // namespace facebook::velox::exec
