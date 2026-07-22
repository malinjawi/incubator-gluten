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
#include "velox/exec/Operator.h"

#include "RollupNode.h"

namespace gluten {

/// Fused rollup operator (draft). See DESIGN.md for the algorithm.
///
/// ---------------------------------------------------------------------------
/// Why GroupingSet-direct rather than HashTable + RowContainer
/// ---------------------------------------------------------------------------
/// Both are reachable from an externally registered operator, but GroupingSet
/// is strictly the better fit and it is what this draft uses:
///
///  * Its constructor (GroupingSet.h:34-49) takes only plain values — no
///    AggregationNode, no OperatorCtx — so it can be instantiated once per
///    rollup level with per-level key channels and per-level input types. There
///    is in-tree precedent for non-HashAggregation use: GroupingSet::createForDistinct
///    (GroupingSet.h:62-68, used by MarkDistinct and EnforceDistinct), and — much
///    closer to what we need — ColumnStatsCollector, which constructs the public
///    GroupingSet constructor directly with hand-built AggregateInfos and a null
///    spillConfig (ColumnStatsCollector.cpp:123-152). That is the template this
///    operator follows.
///  * Passing {isPartial = true, isRawInput = false} gives us exactly the
///    semantics a merge level needs: the input path dispatches to
///    Aggregate::addIntermediateResults (GroupingSet.cpp:351-355) and the output
///    path to Aggregate::extractAccumulators (GroupingSet.cpp:871-876), i.e.
///    states in, states out.
///  * Empty hashers means global aggregation (GroupingSet.cpp:62), which is the
///    grand-total level for free, including the addSingleGroupIntermediateResults
///    path (GroupingSet.cpp:660-661).
///  * The flush machinery we need already exists on it: isPartialFull(),
///    numDistinct(), resetTable(freeTable=false), allocatedBytes().
///
/// Dropping to HashTable::createForAggregation + RowContainer (HashTable.h:568-582)
/// would mean re-implementing initializeAggregates (the GroupingSet.cpp:395-418
/// setAllocator/setOffsets dance), the group-probe loop, and the extract loop by
/// hand, for no capability we actually need. The one thing it would buy is
/// finer control over the row layout, which is not on the critical path.
///
/// The cost of this choice is a dependency on internal Velox execution headers.
/// RESOLVED (was OPEN QUESTION 4): cpp/velox/CMakeLists.txt:283-289 adds
/// ${VELOX_HOME} itself to the include path, i.e. Gluten compiles against the
/// whole Velox source tree, and it already includes velox/exec/HashTable.h at
/// the same depth (cpp/velox/operators/hashjoin/HashTableBuilder.h:23,
/// cpp/velox/jni/JniHashTable.h:25). GroupingSet.h and AggregateInfo.h are
/// therefore available. They remain unstable internal APIs, which is still an
/// argument for upstreaming, but it is not a blocker.
///
/// ---------------------------------------------------------------------------
/// Level indexing
/// ---------------------------------------------------------------------------
/// Levels are finest-first, matching RollupNode::levels():
///   levels_[0]      : all n keys live — the BYPASS lane, no hash table.
///   levels_[1..n-1] : n-i keys live — a GroupingSet keyed on the prefix.
///   levels_[n]      : grand total — a GroupingSet with no hashers.
///
/// Level i's GroupingSet is constructed over the *natural* output type of level
/// i-1, ROW(k1..k[n-i+1], acc1..accm), which is precisely the shape
/// GroupingSet::extractGroups emits for level i-1. That is what makes the
/// cascade a straight pipe: level i-1's drain output is fed verbatim into level
/// i's addInput.
class RollupOperator : public facebook::velox::exec::Operator {
 public:
  RollupOperator(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      const RollupNodePtr& node);

  /// Pool-allocating setup lives here, not in the constructor
  /// (Operator.h:207-215: constructing from the memory pool in the ctor risks an
  /// arbitration deadlock). Builds the per-level AggregateInfos and GroupingSets.
  void initialize() override;

  bool needsInput() const override;

  void addInput(facebook::velox::RowVectorPtr input) override;

  void noMoreInput() override;

  facebook::velox::RowVectorPtr getOutput() override;

  facebook::velox::exec::BlockingReason isBlocked(
      facebook::velox::ContinueFuture* /*future*/) override {
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override;

  /// Output is partial state, so there is nothing to spill — but there is
  /// always something to free: cascading a flush is semantically free at any
  /// point. See DESIGN.md §4.
  ///
  /// The arbitration contract is now known (Operator.cpp:745-804): reclaim() may
  /// park output vectors, but must be NET NON-ALLOCATING on this pool or the
  /// VELOX_CHECK_GE(reclaimedBytes, 0) at Operator.cpp:790 fires. See the long
  /// comment on RollupOperator::reclaim. If a level drain cannot be made
  /// reliably non-growing, revert both of these to the base-class defaults and
  /// accept arbitration-time query failure under pressure.
  bool canReclaim() const override {
    return true;
  }

  void reclaim(
      uint64_t targetBytes,
      facebook::velox::memory::MemoryReclaimer::Stats& stats) override;

 private:
  /// Per-level state.
  struct LevelState {
    /// Number of live keys; levels_[i].numKeys == n - i.
    int32_t numKeys{0};

    /// Spark-supplied grouping id and null mask, copied from RollupNode::Level.
    int64_t gid{0};
    std::vector<bool> keyIsNull;

    /// ROW(k1..k[numKeys], acc1..accm) — what this level's table emits, and what
    /// the next (coarser) level consumes. For the bypass lane this is the
    /// operator's input type.
    facebook::velox::RowTypePtr naturalType;

    /// Null for the bypass lane (index 0) and for any abandoned level.
    std::unique_ptr<facebook::velox::exec::GroupingSet> groupingSet;

    /// Staging only. Aggregate instances are stateful (setAllocator /
    /// setOffsets bind them to one RowContainer), so every level needs its own —
    /// m * n live Aggregate instances for an n-key rollup. The GroupingSet
    /// constructor takes these by rvalue reference and moves them, so this
    /// vector is EMPTY after makeGroupingSet(); the live instances belong to the
    /// GroupingSet. Do not read it back.
    std::vector<facebook::velox::exec::AggregateInfo> aggregates;

    /// Cursor for GroupingSet::getOutput while this level is draining.
    facebook::velox::exec::RowContainerIterator iterator;

    /// Caller-allocated destination for GroupingSet::getOutput — that method
    /// writes into a RowVector the caller supplies, it does not create one (see
    /// GroupingSet::extractGroups, GroupingSet.cpp:847-888). Shaped as
    /// naturalType. Reused across drain batches via BaseVector::prepareForReuse,
    /// which itself falls back to a fresh allocation when the previous vector or
    /// any of its children is still referenced downstream.
    facebook::velox::RowVectorPtr output;

    /// Reduction-ratio bookkeeping, feeding the abandon valve and the runtime
    /// stats. Reset on every flush, like HashAggregation's counters.
    int64_t numInputRows{0};
    int64_t numOutputRows{0};

    /// Cumulative, for stats only.
    int64_t totalInputRows{0};
    int64_t totalOutputRows{0};
    int32_t flushTimes{0};

    /// Per-level budget; doubles up to maxExtendedPartialAggregationMemoryUsage_
    /// exactly as HashAggregation::maybeIncreasePartialAggregationMemoryUsage does.
    int64_t maxPartialBytes{0};

    /// Once abandoned this level drops its table and becomes a pure projection:
    /// incoming states are re-projected with this level's null mask and gid and
    /// forwarded to the next level unchanged. Correct because a merge level has
    /// isRawInput = false, so pass-through *is* the identity on state.
    bool abandoned{false};
  };

  enum class State {
    /// Accepting input.
    kAccumulating,
    /// A pressure-triggered drain of a single level is in flight. On completion
    /// we return to kAccumulating.
    kPressureDrain,
    /// noMoreInput() seen; walking levels finest -> grand total.
    kFinalCascade,
    kDone,
  };

  /// Builds ROW(k1..kL, acc1..accm) for a level with L live keys.
  facebook::velox::RowTypePtr makeNaturalType(int32_t numKeys) const;

  /// Builds this level's AggregateInfos directly, following
  /// ColumnStatsCollector::createAggregates (ColumnStatsCollector.cpp:78-121) —
  /// the in-tree precedent for driving a GroupingSet without an AggregationNode.
  ///
  /// exec::toAggregateInfo (AggregateInfo.h:77-82) is NOT usable here: its first
  /// statement is `aggregationNode.sources()[0]->outputType()`
  /// (AggregateInfo.cpp:49), so it requires a real source plan node whose output
  /// type is this level's input type, which we do not have and would have to
  /// fabricate. Building the infos by hand is both shorter and removes the
  /// expressionEvaluator out-parameter question entirely.
  ///
  /// exec::resolveIntermediateType is public
  /// (velox/exec/AggregateFunctionRegistry.h:58).
  std::vector<facebook::velox::exec::AggregateInfo> makeAggregateInfos(int32_t index);

  /// Reshapes a batch of levels_[index - 1].naturalType into
  /// levels_[index].naturalType by dropping the key column this level masks out.
  /// Used only on the abandoned-level pass-through path. Column reshuffling
  /// only — no data is copied.
  facebook::velox::RowVectorPtr dropDeadKeyColumn(
      int32_t index,
      const facebook::velox::RowVectorPtr& natural) const;

  /// Constructs levels_[index].groupingSet over levels_[index - 1].naturalType.
  void makeGroupingSet(int32_t index);

  /// Projects a level-`index` natural row into the uniform output shape:
  /// keys 0..numKeys-1 pass through by reference, keys numKeys..n-1 become
  /// constant NULL per keyIsNull, accumulators pass through by reference, and a
  /// constant gid column is appended.
  ///
  /// Implemented with BaseVector::createNullConstant / createConstant
  /// (BaseVector.h:601-610). Zero-copy: no accumulator data is touched.
  facebook::velox::RowVectorPtr project(
      int32_t index,
      const facebook::velox::RowVectorPtr& natural) const;

  /// Feeds one natural-shaped batch into level `index`, honouring the abandon
  /// valve. Returns the batch that should be forwarded onward if the level is
  /// abandoned (a pass-through), or nullptr if it was absorbed into the table.
  facebook::velox::RowVectorPtr feedLevel(
      int32_t index,
      const facebook::velox::RowVectorPtr& natural);

  /// Pulls one batch out of the draining level, projects it for output, and
  /// pushes it into the next coarser level. Returns nullptr when the level is
  /// exhausted (and advances/finishes the cascade).
  facebook::velox::RowVectorPtr advanceDrain();

  /// HashAggregation.cpp:183-187, applied per level.
  bool shouldAbandon(const LevelState& level) const;

  /// Post-flush budget policy, mirroring
  /// HashAggregation::maybeIncreasePartialAggregationMemoryUsage.
  void maybeGrowBudget(LevelState& level);

  /// Finest level index (largest numKeys) whose table is over budget, or -1.
  /// Also enforces the operator-wide ceiling (DESIGN.md §3.4 option (c)) and
  /// picks up levels that were abandoned while still holding state.
  int32_t findFullLevel() const;

  /// Sum of allocatedBytes() over every live level table. The quantity the
  /// operator-wide ceiling is tested against.
  int64_t totalLevelBytes() const;

  void recordLevelStats(const LevelState& level, int32_t index);

  const RollupNodePtr node_;
  const int32_t numKeys_;
  const int32_t numAggregates_;

  /// Cached QueryConfig knobs, read once in the ctor as HashAggregation does
  /// (HashAggregation.cpp:55-62).
  const int64_t maxPartialAggregationMemoryUsage_;
  const int64_t maxExtendedPartialAggregationMemoryUsage_;
  const int32_t abandonPartialAggregationMinRows_;
  const int32_t abandonPartialAggregationMinPct_;

  /// Operator-wide ceiling on the SUM of all live level tables — DESIGN.md §3.4
  /// option (c). Derived, not a new config key; see the derivation comment on
  /// findFullLevel().
  const int64_t operatorBudgetCeilingBytes_;

  /// Size n + 1, finest-first.
  std::vector<LevelState> levels_;

  // FIXED (adversarial review): this class previously declared its own
  //   tsan_atomic<bool> nonReclaimableSection_{false};
  // and
  //   exec::SpillStats spillStats_;
  // Both SHADOW protected members of the base class:
  //   * Operator::nonReclaimableSection_ (velox/exec/Operator.h:677) is the flag
  //     Operator::MemoryReclaimer::reclaim actually tests
  //     (Operator.cpp:772-787). Wiring GroupingSet to a *different* flag of the
  //     same name meant the arbitrator was reading a variable nothing ever set,
  //     while GroupingSet's own reclaim guard was invisible to it. We now pass
  //     &nonReclaimableSection_ resolving to the INHERITED member.
  //   * Operator::spillStats_ (velox/exec/Operator.h) is a
  //     std::shared_ptr<exec::SpillStats>, so the shadow also changed the type
  //     out from under Operator::recordSpillStats(). GroupingSet wants an
  //     exec::SpillStats*, which is exactly `spillStats_.get()` on the base
  //     member — always non-null (it is default-constructed in Operator.h).
  // No members are declared here for either; makeGroupingSet() uses the
  // inherited ones.

  /// Bypass-lane rows and, if reclaim() turns out to need it, parked flush
  /// output. Bounded by needsInput() returning false while non-empty.
  std::deque<facebook::velox::RowVectorPtr> outputQueue_;

  State state_{State::kAccumulating};

  /// Level currently draining; valid in kPressureDrain and kFinalCascade.
  int32_t drainLevel_{-1};

  /// Set when noMoreInput() arrives while a pressure drain is still in flight.
  /// The in-flight drain finishes first, then the full cascade starts at level 1
  /// — see the comment in noMoreInput().
  bool pendingFinalCascade_{false};

  /// Whether any input row has ever reached this operator (per driver).
  /// Guards the grand-total level: its GroupingSet is global, and
  /// GroupingSet::getOutput synthesises one row on a fresh iterator whether or
  /// not input arrived (GroupingSet.cpp:671-707). Spark emits ZERO rows for a
  /// grouping-sets query over empty input — verified against the golden file
  /// gluten-ut/spark40/.../postgreSQL/groupingsets.sql.out:190-212, where
  /// `group by grouping sets ((a,b),())` over an empty table produces no rows —
  /// so an unguarded drain would emit one spurious identity row per driver.
  bool receivedInput_{false};

  /// Whether reclaim() may run its Phase-2 cascade drain. False when any level
  /// would extract out-of-line bytes — a variable-width GROUPING KEY or
  /// aggregate intermediate type, or an accumulator that reports
  /// !isFixedSize()/accumulatorUsesExternalMemory(). Those are the shapes in
  /// which the parked output can approach the size of the container being
  /// freed, which risks tripping the VELOX_CHECK_GE(reclaimedBytes, 0) at
  /// Operator.cpp:788-795 and failing the whole QUERY. Phase 1 is
  /// non-allocating by construction and always runs. Computed in initialize();
  /// see the derivation there, including why the accumulator flags alone are
  /// not sufficient (min(varchar) reports isFixedSize() == true).
  bool phase2ReclaimSafe_{true};

  int64_t bypassRows_{0};
  int32_t cascadeFlushes_{0};

  /// True only for the duration of reclaim(). Two behaviours change while it is
  /// set, both required by the net-non-allocating contract described on
  /// RollupOperator::reclaim:
  ///   * maybeGrowBudget() becomes a no-op — it calls pool()->maybeReserve(),
  ///     which RAISES this pool's reservation, and reclaimedBytes is measured as
  ///     a delta on reservedBytes() (ScopedReclaimedBytesRecorder,
  ///     MemoryArbitrator.cpp:527-557). Growing the budget while the arbitrator
  ///     is asking for memory back is also simply wrong.
  ///   * a fully drained level is reset with freeTable = true rather than false,
  ///     so the RowContainer allocation is actually returned instead of retained
  ///     for reuse.
  bool reclaiming_{false};

  int32_t numReclaims_{0};
};

} // namespace gluten
