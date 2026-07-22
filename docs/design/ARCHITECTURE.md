# Fused Rollup Operator — code architecture

`DESIGN.md` explains *why* the operator exists and why the algorithm is shaped the
way it is. This document is the reviewer's map of *how the draft code is
organised*: what each file owns, where a batch physically goes, what the state
machine guarantees, and where the seams are.

Everything below cites the draft files in this directory by line number. Where a
draft line cites Velox or Gluten, that citation has been checked against the real
trees (`velox/` and `cpp/velox/` respectively).

---

## 1. File inventory

Four draft files, plus the two design documents. None of them are in the build
yet — they live in `docs/design/` so the design can be reviewed as code before
any of it lands.

| Draft file | Lines | Owns | Real home |
| --- | ---: | --- | --- |
| `RollupNode.h` | 312 | The plan node: the immutable description of a rollup — key hierarchy, per-level gid/null mask, aggregate list, output type derivation. | `cpp/velox/operators/plannodes/RollupNode.h` |
| `RollupOperator.h` | 336 | Operator declaration: `LevelState`, the `State` enum, the private helper surface. | `cpp/velox/operators/plannodes/RollupOperator.h` |
| `RollupOperator.cpp` | 756 | The whole runtime: level construction, bypass lane, cascade flush, abandon valve, budget policy, stats. | `cpp/velox/operators/plannodes/RollupOperator.cpp` |
| `RollupTranslator.cpp` | 489 | Part A: the `Operator::PlanNodeTranslator` and its registration. Part B: `SubstraitToVeloxPlanConverter::toRollupPlan`, i.e. Substrait → `RollupNode`. | Part A → `cpp/velox/operators/plannodes/RollupOperator.cpp` (bottom) or a small `RollupRegistration.cc`; Part B → **merged into** `cpp/velox/substrait/SubstraitToVeloxPlan.cc` |

### Where these actually land

`cpp/velox/operators/` already has `reader/`, `serializer/`, `hashjoin/`,
`writer/`, `functions/`, `plannodes/`. The custom-plan-node precedent lives in
`plannodes/` — `CudfVectorStream.{h,cc}` and `RowVectorStream.{h,cc}` are both a
node + operator + translator triple in that directory. **Do not create
`operators/rollup/`**; follow the existing convention and use
`cpp/velox/operators/plannodes/`. New sources go in the source list at
`cpp/velox/CMakeLists.txt:177-188`, next to `operators/plannodes/RowVectorStream.cc`
(:180).

Part B is not a new file. It is a diff against the existing
`SubstraitToVeloxPlanConverter`: a branch at the top of
`toVeloxPlan(const ::substrait::ExpandRel&)` (`SubstraitToVeloxPlan.cc:899`) and a
new private method `toRollupPlan` declared in `SubstraitToVeloxPlan.h` beside the
other `toVeloxPlan` overloads. It needs two includes that file does not have
today — `velox/exec/AggregateFunctionRegistry.h` and `RollupNode.h`
(`RollupTranslator.cpp:139-146`).

Registration is a single call added next to the existing
`Operator::registerOperator(std::make_unique<CudfVectorStreamOperatorTranslator>())`
at `cpp/velox/compute/VeloxBackend.cc:183`. Unlike the cudf one it is
unconditional: the native side always knows how to run the node, and whether the
fused plan is *emitted* is a JVM-side config decision (`RollupTranslator.cpp:66-81`).

### What is deliberately missing

- `RollupOperator::reclaim` is `VELOX_NYI` (`RollupOperator.cpp:685`). The
  contract is now understood — see §5 — but the implementation is unwritten and
  is the highest-risk open item.
- `RollupNode::serialize` throws `VELOX_UNSUPPORTED` (`RollupNode.h:179-181`),
  following the `CudfVectorStream.h:153-155` precedent. Cost: no plan
  tracing/replay on rollup stages.
- There are no tests. `DESIGN.md §8` has the plan.

---

## 2. Data flow

### 2.1 Setup — `initialize()` (`RollupOperator.cpp:61-103`)

Runs outside the constructor because constructing from `pool()` in an operator
constructor risks arbitration deadlock (`Operator.h:207-215`, cited at
`RollupOperator.h:92-95`).

1. **Precondition check** (:70-81). The source must emit
   `ROW(k1..kn, acc1..accm)` with the keys at channels `0..n-1` in hierarchy
   order. Name-matched, not just arity-matched, because a silent key permutation
   would produce wrong results rather than a crash.
2. **Level table** (:83-92). `levels_` is sized `n+1`, finest-first.
   `levels_[i].naturalType` is the operator input type for `i == 0`, and
   `makeNaturalType(n - i)` otherwise (:105-127).
3. **Grouping sets for levels 1..n** (:97-100). Level 0 gets nothing — it is the
   bypass lane, the single biggest hash table we never build.
   `makeAggregateInfos(i)` (:129) builds `AggregateInfo` by hand;
   `makeGroupingSet(i)` (:217) constructs the `GroupingSet` over
   `levels_[i-1].naturalType` with `isPartial=true, isRawInput=false`
   (:241-243) and `spillConfig=nullptr` (:247).

### 2.2 One input batch — `addInput()` (`RollupOperator.cpp:258-290`)

```
input : ROW(k1..kn, acc1..accm)
  |
  |-- (a) project(0, input)  -------------------> outputQueue_        [:264]
  |        keys pass by reference, gid_n appended, zero copy
  |
  `-- (b) carry = input
          for i in 1..n:  feedLevel(i, carry)                         [:269-279]
             absorbed  -> returns nullptr, loop stops
             abandoned -> returns dropDeadKeyColumn(i, carry),
                          which is ALSO projected to outputQueue_ and
                          carried to level i+1
  |
  `-- (c) full = findFullLevel(); if >= 0 -> kPressureDrain           [:284-289]
```

Two things worth pausing on:

- **(a) is the whole point.** The finest grain is already grouped by the child
  partial aggregate, so it needs no table — just a tag. `project`
  (`RollupOperator.cpp:715`) shares the surviving key columns and *all*
  accumulator columns by reference and creates only two things: constant-NULL
  vectors for masked keys and a constant gid column
  (`BaseVector::createNullConstant` / `createConstant`, `BaseVector.h:601-610`).
- **The loop stops at the first level that absorbs.** A non-abandoned level
  returns `nullptr` from `feedLevel` because the states are now inside its hash
  table; they will reach level `i+1` later, during that level's drain. That is
  the "exactly once" invariant (§3).

### 2.3 One drained batch — `advanceDrain()` (`RollupOperator.cpp:488-586`)

Called from `getOutput()` (:470-486) only after `outputQueue_` is empty, and only
in `kPressureDrain` or `kFinalCascade`.

```
prepareForReuse(levels_[d].output, maxOutputRows)                     [:500-508]
  (GroupingSet::getOutput writes into a CALLER-supplied RowVector —
   GroupingSet.cpp:847-888 hash path, :671-707 global path)
  |
groupingSet->getOutput(maxOutputRows, byteCap, level.iterator, natural)  [:511-517]
  |
  +-- hasMore && natural->size() > 0:
  |     output = project(d, natural)          --> RETURNED to caller   [:526]
  |     carry  = natural                                               [:529]
  |     for i in d+1..n: feedLevel(i, carry)  --> next level           [:530-538]
  |     (this is the CASCADE: one drained batch used twice)
  |
  `-- exhausted:
        recordLevelStats / maybeGrowBudget                             [:544-546]
        resetTable(freeTable = abandoned || reclaiming_)               [:547-560]
        level.iterator = RowContainerIterator{}                        [:561]
        advance the state machine                                      [:565-584]
```

`natural` is level `d`'s natural type, which *is* level `d+1`'s declared input
type. That identity is what makes the cascade a straight pipe with no
re-projection (§4).

For the grand-total level `maxOutputRows` is forced to 1 (:500) — a global
aggregation holds exactly one group.

### 2.4 End of input — `noMoreInput()` (`RollupOperator.cpp:622-640`)

```
if state == kPressureDrain:  pendingFinalCascade_ = true; return      [:631-634]
else:                        state = kFinalCascade; drainLevel_ = 1   [:638-639]
```

Then `getOutput()` calls `advanceDrain()` repeatedly. Each level, on exhaustion,
increments `drainLevel_` (:566); when it passes `levels_.size()` the operator
records its summary stats and goes to `kDone` (:567-572).

Level 0 is skipped — everything it ever held was emitted by the bypass lane as it
arrived.

---

## 3. The state machine

Four states, declared at `RollupOperator.h:190-199`.

### 3.1 Transition table

| State | `addInput` | `getOutput` | `noMoreInput` | `reclaim` | `isFinished` |
| --- | --- | --- | --- | --- | --- |
| **kAccumulating** | Accepts. Bypass-projects, fans out, then `findFullLevel()` may move to **kPressureDrain** (`:284-289`) | Drains `outputQueue_` only; returns `nullptr` when empty (`:470-486`) | → **kFinalCascade**, `drainLevel_ = 1` (`:638-639`) | Legal quiescent point; would drain a level (unimplemented) | `false` |
| **kPressureDrain** | **Never** — `needsInput()` is `false` (`:254-256`) and `addInput` `VELOX_CHECK`s `kAccumulating` (`:259`) | `advanceDrain()` on `drainLevel_`. On exhaustion → **kAccumulating** (`:582-583`), or → **kFinalCascade** if `pendingFinalCascade_` (`:576-578`) | Sets `pendingFinalCascade_`, **stays** (`:631-634`) | Same, but `reclaiming_` forces `freeTable=true` (`:555`) and blocks `maybeGrowBudget` (`:600-602`) | `false` |
| **kFinalCascade** | Never | `advanceDrain()`; on exhaustion `++drainLevel_`, → **kDone** past the last level (`:565-572`) | Not called again | Same | `false` |
| **kDone** | Never | `nullptr` (both guards fail) | Not called again | Nothing to free | `true` **iff** `outputQueue_` is also empty (`:642-644`) |

`isBlocked` is unconditionally `kNotBlocked` (`RollupOperator.h:105-108`) — the
operator never waits on anything external.

### 3.2 Invariants each transition must preserve

**I1 — every state flows down the hierarchy exactly once.**
A batch of accumulators enters level `i` either from `addInput` or from level
`i-1`'s drain, and is then either absorbed (`feedLevel` returns `nullptr`, and the
fan-out loop *stops*) or passed through by an abandoned level (and continues). It
is never both. The corresponding output row is emitted once, by `project(i, ...)`,
at the moment level `i` releases it. Double-counting would be a correctness bug
that the post-shuffle final aggregation cannot detect, because duplicate group
states are legal by design (`DESIGN.md §4.0`) — nothing downstream would complain.

The subtle case is an abandoned level: `feedLevel` returns the pass-through
(`:306`), `addInput`/`advanceDrain` project *that* to `outputQueue_` and carry the
same vector onward. Correct because a merge level has `isRawInput=false`, so
pass-through is the identity on state.

**I2 — a half-consumed `RowContainerIterator` is never abandoned.**
`LevelState::iterator` (`RollupOperator.h:158-159`) is the cursor into a
`RowContainer` that is still being listed. Abandoning it mid-scan and re-listing
later against a container that has since taken new input silently loses or
duplicates rows.

This is what `pendingFinalCascade_` exists for. `noMoreInput()` may legitimately
arrive during `kPressureDrain` — `needsInput()` is false, but the Driver may still
deliver no-more-input. The naive `state_ = kFinalCascade; drainLevel_ = 1`
would clobber `drainLevel_` and strand the iterator. Instead the in-flight drain
completes, and only then does the cascade start (`:573-578`, rationale at
`:624-630`).

The same reasoning is why the abandon valve does **not** force an inline drain
(`:324-334`): `feedLevel` is called from the middle of two different fan-out
loops, so an inline drain would re-enter the drain path while another level's
iterator is half-consumed. The valve just marks the level, and
`findFullLevel()` clause (2) (`:448-453`) picks it up on the next pressure check.
Net effect: **at most one drain is in flight at any time**, which is the invariant
the entire state machine rests on.

**I3 — `drainLevel_` is valid exactly in the two drain states.**
`-1` in `kAccumulating` and `kDone` (`RollupOperator.h:310`); `advanceDrain`
opens with `VELOX_CHECK_GE(drainLevel_, 1)` (`:489`). Level 0 is never drained.

**I4 — `kDone` does not mean finished.**
`isFinished()` also requires `outputQueue_` to be empty (`:643`). The final
cascade's last steps push pass-through projections onto the queue, so the two can
diverge.

**I5 — reclaim must be net non-allocating.**
`ScopedReclaimedBytesRecorder` + `VELOX_CHECK_GE(reclaimedBytes, 0)` at
`Operator.cpp:788-795`. This is why `maybeGrowBudget` early-returns when
`reclaiming_` (`:600-602`): a single successful `pool()->maybeReserve()` raises
the reservation and can make the delta negative on its own.

---

## 4. Type contracts

### 4.1 The `naturalType(L)` chain

`makeNaturalType(L)` (`RollupOperator.cpp:105-127`) builds

```
ROW( k1..kL , acc1..accm )
```

taking the key names/types verbatim from the operator input type (:112-115) and
resolving each accumulator type from the **registry**, not from the call's
declared return type:

```cpp
resolveIntermediateType(aggregates()[i].call->name(), aggregates()[i].rawInputTypes)
```

(`:123-124`; `exec::resolveIntermediateType` is public,
`velox/exec/AggregateFunctionRegistry.h:57-60`). `makeAggregateInfos` uses the
identical expression at `:187`, and `RollupNode::makeOutputType` uses it again.
Using one source of truth in all three places is what guarantees the pipe joints
line up.

Level 0 is special-cased to the operator input type itself (`:90-91`) — it has all
`n` keys live, so `makeNaturalType(n)` would produce the same thing, but taking
the input type directly avoids depending on that coincidence.

### 4.2 Why level *i*'s output type IS level *i+1*'s input type

Level `i` has `n-i` live keys, so `naturalType(i) = ROW(k1..k[n-i], accs)`. Level
`i+1`'s `GroupingSet` is constructed over `levels_[i].naturalType`
(`makeGroupingSet`, `:219`) with key channels `0..(n-i-1)` (`:222-225`) — i.e. it
takes the level below's shape and simply groups on one fewer key. Its own
extraction then emits `ROW(k1..k[n-i-1], accs)`, which is `naturalType(i+1)`.

So the cascade needs **no re-projection at all**: `advanceDrain` hands `natural`
straight to `feedLevel(d+1, natural)` (`:529-531`). The only place a reshape
happens is `dropDeadKeyColumn` (`:341-363`), on the abandoned-level pass-through
path, where the level has no table to do the narrowing for it. That is pure
column reshuffling — the same child `VectorPtr`s, reordered.

### 4.3 Operator input precondition and output type

**Input** (checked at `:70-81`, and again on the planner side at
`RollupTranslator.cpp:195-209`):

```
ROW( k1, k2, ..., kn ,  acc1, ..., accm )
     ^ hierarchy order, k1 coarsest      ^ intermediate types
```

**Output**, uniform across every level (`RollupNode::makeOutputType`,
`RollupNode.h:238`):

```
ROW( k1..kn all NULLABLE , acc1..accm , gid BIGINT )
```

Key columns must be nullable even when the source column is not, because a level
with `L` live keys emits NULL for `k(L+1)..kn`. The gid column must stay last —
`RollupNode::gidChannel()` is defined as `size() - 1` (`RollupNode.h:165-167`).

### 4.4 Type flow, n = 3

```
                       input : ROW(k1,k2,k3, a1..am)
                                    |
        (a) project(0) -------------+------------- output ROW(k1,k2,k3, a1..am, gid=0)
                                    |
                                    v
                    L1  GroupingSet over ROW(k1,k2,k3, a1..am)
                        keys {0,1}                       naturalType(2)
                                    |
                                    v  drained batch: ROW(k1,k2, a1..am)
        (b) project(1) -------------+------------- output ROW(k1,k2,NULL, a1..am, gid=1)
                                    |
                                    v
                    L2  GroupingSet over ROW(k1,k2, a1..am)
                        keys {0}                         naturalType(1)
                                    |
                                    v  drained batch: ROW(k1, a1..am)
        (c) project(2) -------------+------------- output ROW(k1,NULL,NULL, a1..am, gid=3)
                                    |
                                    v
                    L3  GroupingSet over ROW(k1, a1..am)
                        keys {}  (global)                naturalType(0)
                                    |
                                    v  drained batch: ROW(a1..am)   [1 row]
        (d) project(3) -------------+------------- output ROW(NULL,NULL,NULL, a1..am, gid=7)

        Every arrow marked v is a TYPE IDENTITY, not a conversion:
        naturalType(L) is literally the RowType the level below was
        constructed over.  gid values are Spark's, read verbatim from
        the Expand literals — never recomputed in C++.
```

---

## 5. Ownership and lifetime

**`Aggregate` instances are stateful and single-table-bound.**
`setAllocator`/`setOffsets` bind an `Aggregate` to one `RowContainer`, so every
level needs its *own* instances: **m × n live instances** for an n-key rollup with
m aggregates (`RollupOperator.h:150-156`). They are created in
`makeAggregateInfos` via `exec::Aggregate::create(name, kPartial, rawInputTypes,
intermediateType, config)` (`RollupOperator.cpp:198-203`).

**`LevelState::aggregates` is empty after `makeGroupingSet`.**
The `GroupingSet` constructor takes the vector by rvalue reference and moves it
(`RollupOperator.cpp:226`). The staging vector is a construction-time courier
only; reading it back afterwards yields nothing. `initialize()` therefore does
`levels_[i].aggregates = makeAggregateInfos(i); makeGroupingSet(i);` in that
order and never touches it again (`:97-100`).

**Per-level output vector reuse.**
`GroupingSet::getOutput` does *not* allocate its result — both the hash path
(`GroupingSet.cpp:847-888`) and the global path (`:671-707`) write into a
caller-supplied `RowVector`. `advanceDrain` keeps one per level in
`LevelState::output` and recycles it with `BaseVector::prepareForReuse`
(`RollupOperator.cpp:501-508`), which itself falls back to a fresh allocation
when the previous vector or any child is still referenced downstream — which is
exactly what happens when `project()` has shared those children into a batch the
consumer still holds. Correct by construction, but it means reuse is *best
effort*, not guaranteed.

**Table lifetime.**
On exhaustion, `resetTable(freeTable)` (`:556`) where
`freeTable = level.abandoned || reclaiming_` — keep the allocation for a normal
flush (mirroring `HashAggregation::resetPartialOutputIfNeed`,
`HashAggregation.cpp:286-311`), give the memory back when the level is dead or the
arbitrator is asking. An abandoned level additionally drops its `GroupingSet`
entirely (`:557-559`), after which it is a pure projection forever.

**Budget.**
Each level carries its own `maxPartialBytes`, doubling up to
`max_extended_partial_aggregation_memory` (`maybeGrowBudget`, `:588-620`), and the
operator enforces a ceiling on the **sum** at
`operatorBudgetCeilingBytes_ = maxExtendedPartialAggregationMemoryUsage_`
(`RollupOperator.h:285`, derivation at `RollupOperator.cpp:391-423`). No new
config key: the fused operator replaces one partial aggregation, so all its level
tables together live inside the envelope one partial aggregation is allowed to
reach.

**`close()`** (`:706`) drops every `GroupingSet` and output vector and clears the
queue before calling the base.

---

## 6. Extension points

### 6.1 CUBE / GROUPING SETS

The node format is already general. `RollupNode::Level` carries an explicit
`keyIsNull` bitmask rather than deriving it from `numKeys`
(`RollupNode.h:62-75`), precisely so a lattice of arbitrary grouping sets needs no
format change.

What is rollup-specific and would have to change:

1. **`RollupNode`'s constructor `VELOX_CHECK`** that `levels_[i].numKeys == n - i`
   (`RollupNode.h:118-127`) — a lattice is not a chain.
2. **The translator's prefix validation** (`RollupTranslator.cpp:308-329`), which
   sorts levels by `numKeys` and rejects anything that is not a strict descending
   prefix. This is the deliberate v1 gate: a non-prefix shape returns `nullptr`
   and the caller builds the ordinary `ExpandNode`.
3. **`levels_` becomes a DAG, not a vector.** Every place that says
   "level `i+1`" — the fan-out loop in `addInput` (`:269-279`), the cascade loop
   in `advanceDrain` (`:528-539`), `dropDeadKeyColumn`'s
   `levels_[index-1]` (`:344-355`) — needs a per-level *parent* pointer instead of
   `index - 1`, and each level must know which key columns to keep rather than
   assuming a prefix. `naturalType` becomes per-level rather than
   per-`numKeys`.
4. **Drain order** must become a topological order over the lattice, so that a
   level is only drained after every level that feeds it.

The state machine and the type-identity trick survive all of that unchanged, as
long as each edge in the lattice still satisfies "parent's natural type is the
child's declared input type" — which requires choosing, for each set, a parent
whose live keys are a superset. That choice is the interesting design problem;
everything else is bookkeeping.

### 6.2 Spill

Deliberately absent: `RollupNode::canSpill` returns `false`
(`RollupNode.h:172-174`), the `Operator` base is constructed with `std::nullopt`
for the spill config (`RollupOperator.cpp:44`), and every `GroupingSet` gets
`spillConfig = nullptr` (`:247`). The justification is `DESIGN.md §4`: output is
pre-shuffle partial state, so duplicate group states are legal and a level can
always *flush* instead of spilling.

If spill were ever needed, the seam is narrow and already shaped for it:

- `spillStats_` (`RollupOperator.h:290`) exists solely so a future spill path has
  somewhere to land; today it is never written (`nullptr` `spillConfig`, and
  `ColumnStatsCollector.cpp:151` establishes that even a null `SpillStats*` is
  accepted).
- `nonReclaimableSection_` (`RollupOperator.h:282`) is already threaded into every
  `GroupingSet` (`:248`).
- The natural implementation is per-level: give level `i` a real `SpillConfig` and
  let `GroupingSet` do the work it already knows how to do.

**Do not confuse spill with `reclaim`.** `reclaim` (`:646-685`) is unimplemented
and is the design's highest-risk item. `canReclaim()` returns `true`
(`RollupOperator.h:124-126`), and `maybeSetReclaimer` installs the reclaimer
regardless of `spillConfig` (`Operator.cpp:113-121`), so the operator *is* in the
arbitrator's reach today. The intended strategy — drain the finest non-empty
level fully, park projected batches on `outputQueue_`, feed naturals onward, then
`resetTable(freeTable=true)` — is only legal if the freed table bytes exceed the
parked output bytes (invariant I5). Common but not guaranteed. If it cannot be
made reliably non-growing, `canReclaim()` must revert to the base default.

### 6.3 Upstreaming to `facebookincubator/velox`

The seam is clean, because the split is already Gluten-specific-vs-generic:

**Would go upstream** — `RollupNode.h`, `RollupOperator.{h,cpp}`, and the
`PlanNodeTranslator` in `RollupTranslator.cpp:45-64`. None of it references
Substrait, Spark, or Gluten; the Spark-specific facts (gid values, null
placement) enter as plain data on `RollupNode::Level` rather than as logic. Once
upstream, the node becomes a first-class `core::PlanNode`, `LocalPlanner`'s
`dynamic_pointer_cast` chain handles it directly, and the registration at
`VeloxBackend.cc:183` disappears.

**Would stay in Gluten** — Part B of `RollupTranslator.cpp` (the `isRollup=`
marker detection and `toRollupPlan`), and the whole JVM side (`PLANNER.md`).

The strongest argument *for* upstreaming is dependency hygiene. The operator
includes `velox/exec/GroupingSet.h` and `velox/exec/AggregateInfo.h` — internal
execution headers with no stability guarantee. This is legal today
(`cpp/velox/CMakeLists.txt:283-289` puts `${VELOX_HOME}` itself on the include
path, and `cpp/velox/operators/hashjoin/HashTableBuilder.h:23` already includes
`velox/exec/HashTable.h` at the same depth), but it means every Velox bump can
break the build. Upstreaming converts that from a liability into a maintained
interface.

Two things must be settled before upstreaming: `serialize()` (upstream nodes are
expected to round-trip; `VELOX_UNSUPPORTED` would not be accepted), and
`reclaim()`.

---

## 7. Cross-file consistency: the aggregate-function contract

This is the one contract that spans all three code files and is easy to break
silently, so it is worth stating in one place.

`RollupNode::Aggregate::call->name()` is the **base, unsuffixed** Velox aggregate
function name (`sum`, not `sum_partial` or `sum_merge`), and `rawInputTypes` are
the **true raw** argument types.

- `RollupTranslator.cpp:428-433` recovers the base name from the Substrait
  measure via `SubstraitParser::findVeloxFunction`, rather than reusing the child
  `AggregationNode`'s call names — Gluten has already rewritten those into
  companion names (`toAggregationFunctionStep` →
  `toAggregationFunctionName`, `SubstraitToVeloxPlan.cc:288-351`), so a
  finest-grain partial child carries `"<base>_partial"`.
- `RollupOperator.cpp:187` calls `resolveIntermediateType(name, rawInputTypes)`
  and `:198-203` calls `Aggregate::create(name, kPartial, rawInputTypes, ...)`.
  This is byte-for-byte what Velox's own `toAggregateInfo` does for a
  kIntermediate `AggregationNode` (`AggregateInfo.cpp:83-105`).

The merge semantics come from the **caller**, not from the name:
`isRawInput=false` routes input to `Aggregate::addIntermediateResults`
(`GroupingSet.cpp:351-355`) and `isPartial=true` routes output to
`extractAccumulators` (`GroupingSet.cpp:871-876`). `Aggregate.h:343-352` is
explicit that `argTypes` are always raw types and that "partial and intermediate
aggregations create functions using kPartial".

Using the `_merge` companion instead would break `resolveIntermediateType`, whose
argument types would then have to be the *intermediate* type — `avg` is the
immediate counterexample (raw `DOUBLE`, intermediate `ROW(DOUBLE, BIGINT)`, and
`avg_merge`'s declared argument is the ROW, per
`AggregateCompanionSignatures.cpp:125-138`). The full argument is at
`RollupTranslator.cpp:337-386`.
