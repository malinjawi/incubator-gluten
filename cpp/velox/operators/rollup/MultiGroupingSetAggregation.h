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

#include <deque>

#include "velox/exec/AggregateInfo.h"
#include "velox/exec/GroupingSet.h"
#include "operators/rollup/GroupingSetAggregationNode.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/// Fused grouping-set aggregation operator. See HYBRID_DESIGN.md.
///
/// ---------------------------------------------------------------------------
/// Shape
/// ---------------------------------------------------------------------------
/// One `GroupingSet` per grouping set (NOT one shared table namespaced by gid),
/// arranged into a derivation forest over the grouping-set lattice. Each set is
/// computed from its smallest already-computed superset rather than from raw
/// input. Per-set tables are what make three things expressible that a single
/// shared table forecloses (HYBRID_DESIGN.md §2.5):
///
///   * the BYPASS lane -- one set running with no table at all;
///   * PER-SET ABANDON -- the finest sets are exactly the ones that should give
///     up when the reduction ratio is poor;
///   * NARROW KEYS -- a set hashes and stores only its active keys, not all n
///     plus a gid.
///
/// ---------------------------------------------------------------------------
/// Why GroupingSet-direct rather than HashTable + RowContainer
/// ---------------------------------------------------------------------------
///  * Its constructor takes only plain values -- no AggregationNode, no
///    OperatorCtx -- so it can be instantiated once per grouping set with
///    per-set key channels and per-set input types. In-tree precedent:
///    ColumnStatsCollector constructs the public GroupingSet constructor
///    directly with hand-built AggregateInfos and a null spillConfig.
///  * {isPartial = true, isRawInput = false} gives exactly merge semantics: the
///    input path dispatches to Aggregate::addIntermediateResults and the output
///    path to Aggregate::extractAccumulators -- states in, states out.
///  * Empty hashers means global aggregation, which is the grand-total set for
///    free.
///  * The flush machinery already exists on it: isPartialFull(), numDistinct(),
///    resetTable(freeTable), allocatedBytes().
///
/// ---------------------------------------------------------------------------
/// The type pipe
/// ---------------------------------------------------------------------------
/// EVERY node's GroupingSet is constructed over that node's own `naturalType`,
///   ROW(<active keys of this set>, acc1..accm)
/// which is also exactly what GroupingSet::extractGroups emits for it. Batches
/// arriving from a parent (or from the raw input, for a root) are reshaped into
/// that type by `derive()`, a pure column *selection* -- one reference copy per
/// column, no data movement. Three consequences:
///
///   * a set's table hashes only its own active keys;
///   * an ABANDONED node becomes a literal identity pass-through, because its
///     input type and its output type are the same. The chain-only
///     `dropDeadKeyColumn` of the prefix-rollup prototype disappears;
///   * the derivation forest works on an arbitrary lattice, not just a chain.
///
/// ---------------------------------------------------------------------------
/// Flush: per-node iterators plus an explicit drain stack
/// ---------------------------------------------------------------------------
/// There is no shared drain cursor. Every node owns its `RowContainerIterator`
/// and its output vector, and `drainStack_` records which drains are in flight,
/// innermost last. This replaces a four-state machine plus a single
/// `drainLevel_` scalar, and it turns three hard-won invariants of the
/// prototype into structural properties (HYBRID_DESIGN.md §4.3):
///
///   * TERMINATION IS STRUCTURAL. A node is pushed only by its own parent's
///     drain or by the final sweep, and `plan_.order` places parents strictly
///     before children, so the stack is strictly increasing in topological rank
///     and bounded by the chain depth. There is no `next > drainLevel_` bound
///     to get right, and no way to livelock the driver.
///   * NO STRANDED ITERATOR. Parking a parent while a child drains costs
///     nothing and loses nothing, so "at most one drain in flight" -- which the
///     correctness argument never required (§6.2 explicitly permits partial,
///     interleaved installments of a parent's state) -- is not a rule anyone
///     has to enforce.
///   * RECLAIM CANNOT RETARGET, because there is nothing to retarget: it pushes
///     onto the same stack.
class MultiGroupingSetAggregation : public Operator {
 public:
  MultiGroupingSetAggregation(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const GroupingSetAggregationNodePtr& node);

  /// Pool-allocating setup lives here, not in the constructor
  /// (Operator.h:207-215: constructing from the memory pool in the ctor risks
  /// an arbitration deadlock). Builds the derivation plan, the per-node
  /// AggregateInfos and the per-node GroupingSets.
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

  /// Output is partial state, so there is nothing to spill -- but there is
  /// always the RETAINED-BUT-EMPTY table slack a steady-state flush leaves
  /// behind, and freeing that is provably net non-allocating. reclaim() frees
  /// exactly that and nothing else; see the long comment on reclaim() for why
  /// it deliberately does NOT drain live state under arbitration.
  bool canReclaim() const override {
    return true;
  }

  /// Reports only the retained-but-empty table bytes reclaim() can actually
  /// free, not the whole reservation the Operator default would advertise.
  bool reclaimableBytes(uint64_t& reclaimableBytes) const override;

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

 private:
  /// Why a node is being drained. Replaces the prototype's three interacting
  /// clauses on one boolean, and drives whether the table is freed rather than
  /// retained (HYBRID_DESIGN.md §4.4).
  enum class FlushReason {
    kNone,
    /// Over its own maxPartialBytes.
    kSoftBudget,
    /// Over kHardCapMultiple * maxPartialBytes. The only reason that may nest a
    /// drain inside a parent's drain.
    kHardCap,
    /// The sum over all live tables is over the operator-wide ceiling.
    kOperatorCeiling,
    /// The node gave up on aggregating but still holds accumulated state.
    kAbandoned,
    /// The memory arbitrator asked.
    kReclaim,
    /// Final sweep.
    kEndOfInput,
  };

  /// Per grouping-set state.
  struct NodeState {
    /// Copied from the node.
    int64_t gid{0};
    std::vector<bool> keyIsActive;

    /// Channels, into the operator's INPUT type, of this set's active keys,
    /// ascending. Size == numActiveKeys.
    std::vector<column_index_t> activeKeys;

    /// ROW(<active keys>, acc1..accm). Both the type this node's GroupingSet is
    /// constructed over AND the type its extract emits -- see "The type pipe".
    RowTypePtr naturalType;

    /// Column selection that reshapes a batch of the PARENT's naturalType (or
    /// of the operator's input type, for a root) into this node's naturalType.
    /// Both are fixed at initialize() time.
    std::vector<column_index_t> deriveKeyChannels;
    column_index_t deriveAccBase{0};

    /// Null for the bypass lane and for a node whose abandoned table has been
    /// drained and dropped.
    std::unique_ptr<GroupingSet> groupingSet;

    /// Staging only. Aggregate instances are stateful (setAllocator /
    /// setOffsets bind them to one RowContainer), so every node needs its own
    /// set -- m * n live Aggregate instances. The GroupingSet constructor takes
    /// these by rvalue reference and moves them, so this vector is EMPTY after
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

    /// Cumulative, for stats only.
    int64_t totalInputRows{0};
    int64_t totalOutputRows{0};
    int32_t flushTimes{0};

    /// Pooled evidence for the abandon decision, accumulated over saturated
    /// pressure cycles and reset whenever the pooled ratio comes back healthy.
    /// See maybeAbandonAfterDrain().
    int64_t abandonWindowInRows{0};
    int64_t abandonWindowOutRows{0};
    int32_t abandonWindowCycles{0};

    /// Per-node budget; doubles up to maxExtendedPartialAggregationMemoryUsage_
    /// exactly as HashAggregation::maybeIncreasePartialAggregationMemoryUsage
    /// does.
    int64_t maxPartialBytes{0};

    /// No hash table by planner proof (HYBRID_DESIGN.md §3): rows are tagged
    /// with this set's gid and forwarded verbatim. Note a bypassed set is still
    /// a node in the forest -- its children are fed from the stream that passes
    /// through it, so they see R rows rather than G(set) rows.
    bool bypass{false};

    /// Once abandoned this node drops its table and becomes a pure projection.
    /// Correct because a merge node has isRawInput = false, so pass-through IS
    /// the identity on state -- and because its input and output types are the
    /// same (see "The type pipe"), pass-through needs no reshaping at all.
    bool abandoned{false};

    /// Whether this node is currently on drainStack_.
    bool draining{false};

    /// Why it was pushed; decides whether the table is freed on completion.
    FlushReason reason{FlushReason::kNone};

    int32_t numActive() const {
      return static_cast<int32_t>(activeKeys.size());
    }
  };

  /// ROW(<active keys of set i>, acc1..accm).
  RowTypePtr makeNaturalType(const NodeState& node) const;

  /// Builds this node's AggregateInfos directly, following
  /// ColumnStatsCollector::createAggregates -- the in-tree precedent for
  /// driving a GroupingSet without an AggregationNode.
  ///
  /// toAggregateInfo is NOT usable here: its first statement is
  /// `aggregationNode.sources()[0]->outputType()`, so it requires a real source
  /// plan node whose output type is this node's input type, which we do not
  /// have and would have to fabricate.
  std::vector<AggregateInfo> makeAggregateInfos(int32_t index);

  /// Constructs nodes_[index].groupingSet over nodes_[index].naturalType.
  void makeGroupingSet(int32_t index);

  /// Reshapes a batch of the parent's naturalType (or the operator's input
  /// type) into node `index`'s naturalType. Pure column selection: one
  /// reference copy per column, no data movement, arbitrary lattice.
  RowVectorPtr derive(int32_t index, const RowVectorPtr& source) const;

  /// Projects a naturalType batch into the uniform output shape: active keys
  /// pass through by reference, inactive keys become constant NULL,
  /// accumulators pass through by reference, and a constant gid column is
  /// appended. Zero-copy: only the masked keys and gid are newly allocated, and
  /// both are constants.
  RowVectorPtr project(int32_t index, const RowVectorPtr& natural) const;

  /// Feeds one naturalType-shaped batch into node `index`, honouring the bypass
  /// and abandon valves, and recursing into its children when the node is a
  /// pass-through.
  void feedNode(int32_t index, const RowVectorPtr& natural);

  /// derive() + feedNode() for every child of `index`.
  void feedChildren(int32_t index, const RowVectorPtr& natural);

  /// Abandon valve, evaluated at the END of a PRESSURE drain cycle (soft
  /// budget / hard cap / operator ceiling) and nowhere else.
  ///
  /// HashAggregation.cpp:183-187 samples `distinct/rows` on a prefix of the
  /// stream and gives up if the ratio is high. That test is UNSOUND for a
  /// lattice node, and measurably so. Distinct-count on a prefix follows the
  /// occupancy curve E[d] = G(1 - e^(-n/G)), so d/n is an estimate of
  /// (1 - e^(-n/G)) * G/n -- the SATURATION of the table -- not of the node's
  /// reduction G/N. Whenever the node's true group count G is large relative
  /// to the sample size n, d/n is near 1 no matter how well the node will
  /// eventually collapse. On TPC-DS q67 the 7-key node sampled 100,352 rows,
  /// saw 90,555 distinct (90.2%, model predicts 90.2%) and abandoned -- while
  /// its true reduction over the full stream is 5,342,042 -> 640,797, i.e.
  /// 12.0%. It was judged 6.8x worse than it is. The 6-key node then abandoned
  /// only because the first one had, having been handed a 5.34M-row stream
  /// instead of a 640K-row one. Together they emitted 9.8M rows of avoidable
  /// duplicate partial state into the pre-shuffle output.
  ///
  /// The bias is not fixable by rescaling the threshold, because the quantity
  /// is not identifiable from a prefix: "G is huge" and "there is no reduction
  /// at all" produce the same d/n ~ 1 signature, and no amount of prefix tells
  /// them apart. What DOES identify the steady state is a completed pressure
  /// drain: over one full budget-sized cycle the node absorbed numInputRows
  /// and emitted numOutputRows groups, and that ratio is the marginal cost it
  /// will keep paying on every subsequent cycle. Measuring there is unbiased,
  /// costs nothing extra, and fires precisely when abandoning actually wins --
  /// when the node is cycling its table without collapsing anything.
  ///
  /// A node that never reaches memory pressure never abandons. That is the
  /// intended behaviour: abandoning costs N rows forever and is irreversible
  /// (the table is dropped in completeNodeDrain), whereas flushing costs G
  /// rows per cycle with bounded memory, so abandon can only win once flush
  /// cycles are actually recurring.
  ///
  /// WHY THERE IS NO abandonPartialAggregationMinRows_ FLOOR HERE. The obvious
  /// guard -- "ignore cycles that absorbed fewer than N rows" -- inverts this
  /// valve under exactly the memory pressure it exists to handle, and measured
  /// so on q67. That config defaults to 100,000 rows, but the rows a cycle can
  /// absorb are set by the BUDGET, not by the config: at a 4 MB partial budget
  /// the 7-key node's table fills at ~6,100 groups and the cycle ends after
  /// ~19,000 rows. The floor was therefore never cleared, on any of the 282
  /// cycles it ran, every one of which measured 97.5% -- so the node ground a
  /// table that collapsed nothing for the entire query. The tighter the budget,
  /// the shorter the cycle, the more certainly the valve is silenced; the valve
  /// was unreachable precisely when it was most needed.
  ///
  /// A row floor is the wrong instrument because it is guarding against the
  /// wrong thing. Its job is to stop the ratio being read off an unsaturated
  /// table -- but that bias afflicts a PREFIX of a single growing table, which
  /// is not what a completed flush cycle is. A cycle starts from an empty table
  /// and ends when something forces a drain, and the ratio it measures is the
  /// node's SUSTAINED emission rate under whatever regime is actually forcing
  /// those drains. That rate -- not G -- is what the decision turns on, and a
  /// cycle measures it directly however few rows it saw.
  ///
  /// This is why kOperatorCeiling cycles count as evidence too, which is not
  /// obvious: such a cycle can be cut short by a SIBLING's table filling the
  /// operator-wide ceiling, leaving this node drained at low occupancy and
  /// reading high. An earlier version of this fix therefore discarded them, and
  /// that was measurably wrong. On q67 at a 64 MB budget the 7-key node takes
  /// 21 of its 23 drains on the ceiling; discarding them left one usable cycle,
  /// never enough to decide, and the node ground a table at a pooled 78% ratio
  /// for the whole query (1.30x vs the 1.45x it gets by abandoning). The
  /// resolution is that chronic truncation is not a measurement artefact -- it
  /// IS the operating regime. A node that keeps being cut short keeps paying
  /// that rate, so the rate is real.
  ///
  /// What replaces the floor is kAbandonMinEvidenceCycles cycles of POOLED
  /// evidence. Pooling is what makes an individual short cycle harmless: it
  /// guards against a non-stationary key stream, where one early burst of
  /// distinct keys could otherwise latch an irreversible decision, and it reads
  /// a node sitting near the threshold correctly where a consecutive-run rule
  /// is defeated by oscillation across it.
  void maybeAbandonAfterDrain(int32_t index);

  /// Post-flush budget policy, mirroring
  /// HashAggregation::maybeIncreasePartialAggregationMemoryUsage.
  void maybeGrowBudget(NodeState& node);

  /// Not const: GroupingSet::isPartialFull() may rehash, which is exactly the
  /// self-healing behaviour we want it to have.
  FlushReason flushReason(int32_t index);

  bool shouldFreeTable(int32_t index, FlushReason reason);

  /// Sum of allocatedBytes() over every live table.
  int64_t totalNodeBytes() const;

  bool overOperatorCeiling() const;

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

  /// A child fed during a parent's drain is allowed to overshoot its SOFT
  /// budget until the parent's drain completes -- it cannot exceed G(child)
  /// entries, which is by definition what it would hold at end of input anyway.
  /// It nests a drain only on crossing this multiple of the soft budget, which
  /// is the escape hatch for adversarial lattices where G(child) is itself
  /// huge. Measured: on a 3-key rollup under a small budget the nested path
  /// DOES fire (flushCorrectnessSmallBudget records 41 hard-cap drains), so it
  /// is a normal path under pressure, not an adversarial-only one.
  static constexpr int64_t kHardCapMultiple = 4;

  /// Saturated pressure cycles that must be pooled before the abandon decision
  /// is allowed to fire. See maybeAbandonAfterDrain().
  static constexpr int32_t kAbandonMinEvidenceCycles = 3;

  const GroupingSetAggregationNodePtr node_;
  const int32_t numKeys_;
  const int32_t numAggregates_;

  /// Cached QueryConfig knobs, read once in the ctor as HashAggregation does.
  /// abandon_partial_aggregation_min_rows is deliberately NOT among them --
  /// see maybeAbandonAfterDrain() for why a row floor is the wrong instrument
  /// for a flush-cycle measurement.
  const int64_t maxPartialAggregationMemoryUsage_;
  const int64_t maxExtendedPartialAggregationMemoryUsage_;
  const int32_t abandonPartialAggregationMinPct_;

  /// ADVISORY target for the SUM of all live tables -- NOT an enforced bound.
  /// It is only sampled in flushReason(), addInput() arms at most one drain per
  /// batch, and HashTable slot-array growth is power-of-two granular, so the
  /// sum can and does overshoot: measured 1.18x over at a 4MB ceiling and ~158x
  /// at 64KB on a 4-set rollup. At the Velox default (16MB) the sum stayed
  /// inside the target on that workload. Note also that the global (grand
  /// total) node's allocation is counted here but can never be freed, so a
  /// ceiling below ~256KB leaves overOperatorCeiling() permanently true.
  ///
  /// OPEN: either enforce it (test overOperatorCeiling() in the cascade loop of
  /// advanceDrain(), not just kHardCap) or exclude global nodes from
  /// totalNodeBytes() and document a floor. Left advisory for now because
  /// enforcing it changes flush behaviour and needs its own measurement.
  ///
  /// Derived, not a new config key: this
  /// operator replaces exactly one partial aggregation, and
  /// max_extended_partial_aggregation_memory is already the sanctioned answer
  /// to "how much may one partial aggregation's tables occupy at most". The
  /// rejected alternative -- splitting the budget n ways -- starves the one or
  /// two nodes actually holding data, and the lattice makes that worse, not
  /// better: coarse nodes are tiny.
  const int64_t operatorBudgetCeilingBytes_;

  /// The derivation forest. Built once in initialize().
  DerivationPlan plan_;

  /// One entry per grouping set, in node order (NOT topological order --
  /// plan_.order carries that).
  std::vector<NodeState> nodes_;

  // This class deliberately declares NEITHER `nonReclaimableSection_` NOR
  // `spillStats_`. Both exist on Operator (Operator.h:677), and shadowing them
  // means the arbitrator tests a flag nothing sets while GroupingSet's own
  // reclaim guard is invisible to it. makeGroupingSet() passes the INHERITED
  // members.

  /// Bypass-lane rows, pass-through rows and parked flush output. Bounded by
  /// needsInput() returning false while non-empty.
  std::deque<RowVectorPtr> outputQueue_;

  /// Drains in flight, innermost last. Strictly increasing in topological rank,
  /// hence bounded by the chain depth. Empty == accumulating.
  std::vector<int32_t> drainStack_;

  /// Index into plan_.order. Negative while accumulating; >= order.size() means
  /// the final sweep is complete.
  int32_t finalSweepPos_{-1};

  /// Whether any input ROW has ever reached this operator (per driver). Guards
  /// the grand-total set: its GroupingSet is global, and GroupingSet::getOutput
  /// synthesises one row on a fresh iterator whether or not input arrived.
  /// Spark emits ZERO rows for a grouping-sets query over empty input, so an
  /// unguarded drain would emit one spurious identity row PER DRIVER.
  bool receivedInput_{false};

  int64_t bypassRows_{0};
  int32_t numFlushes_{0};
  int32_t numNestedDrains_{0};
  int32_t numReclaims_{0};
  bool finalStatsRecorded_{false};
};

/// Registers the grouping-set aggregation translator with the Velox driver.
void registerMultiGroupingSetAggregation();

} // namespace facebook::velox::exec
