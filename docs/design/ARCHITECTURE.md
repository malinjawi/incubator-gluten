# Fused Rollup Operator — historical prototype architecture

> **Historical prototype, not production source.** This document explains the
> archived `docs/design/Rollup*` proof of concept and preserves its design
> rationale and audit trail. Those files are not compiled. The production
> Gluten-local implementation is
> `cpp/velox/operators/plannodes/GroupingSetAggregationNode.{h,cc}`,
> `GroupingSetLattice.{h,cc}`, and `MultiGroupingSetAggregation.{h,cc}`, with tests in
> `cpp/velox/tests/MultiGroupingSetAggregationTest.cc`. Where this document's
> names or line references disagree with those files, the production files are
> authoritative.

`DESIGN.md` explains *why* the operator exists and why the prototype algorithm
was shaped the way it is. This document is the reviewer's map of *how that
prototype code is organised*: what each file owns, where a batch physically
goes, what the state machine guarantees, and where the seams are.

Everything below cites the archived files in this directory by their historical
line numbers. These references are retained for auditability; they are not
current production-code navigation.

---

## 1. Historical prototype inventory

Five prototype source files live in `docs/design/`. None is in the build, and
none should be copied into a Velox checkout. Their production successors are
already Gluten-local:

| Historical prototype file | Owns | Production successor |
| --- | --- | --- |
| `docs/design/RollupNode.h` | Prototype plan-node contract. | `cpp/velox/operators/plannodes/GroupingSetAggregationNode.{h,cc}` |
| `docs/design/RollupOperator.h` | Prototype runtime declaration and state machine. | `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.h` |
| `docs/design/RollupOperator.cpp` | Prototype runtime algorithm. | `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.cc` |
| `docs/design/RollupTranslator.cpp` | Prototype translator, registration, and Substrait conversion. | Registration is in `MultiGroupingSetAggregation.cc`; conversion is in `cpp/velox/substrait/SubstraitToVeloxPlan.{h,cc}`. |
| `docs/design/RollupOperatorTest.cpp` | Non-building prototype test sketch. | `cpp/velox/tests/MultiGroupingSetAggregationTest.cc` |

### Production implementation

`cpp/velox/operators/` already has `reader/`, `serializer/`, `hashjoin/`,
`writer/`, `functions/`, `plannodes/`. The custom-plan-node precedent lives in
`plannodes/` — `CudfVectorStream.{h,cc}` and `RowVectorStream.{h,cc}` are both a
node + operator + translator triple in that directory. The production
`GroupingSetAggregationNode`, `GroupingSetLattice`, and
`MultiGroupingSetAggregation` implementation follows that convention in
`cpp/velox/operators/plannodes/`. All three `.cc` files are listed in
`cpp/velox/CMakeLists.txt`, next to `operators/plannodes/RowVectorStream.cc`.

The production Substrait conversion is not a standalone translator file.
`SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::ExpandRel&)`
recognizes the marker and calls `toGroupingSetAggregation()` in
`cpp/velox/substrait/SubstraitToVeloxPlan.{h,cc}`. It includes the Gluten-local
`operators/plannodes/GroupingSetAggregationNode.h`.

Registration is Gluten-local:
`cpp/velox/compute/VeloxBackend.cc` calls
`registerMultiGroupingSetAggregation()`, whose definition and translator live
with `MultiGroupingSetAggregation.cc`. Whether the fused plan is emitted remains
a JVM-side config decision.

### Prototype gaps (historical)

The archived prototype left reclaim, serde, and executable tests incomplete.
Those statements apply only to `docs/design/Rollup*`; the production
`MultiGroupingSetAggregation` has reclaim handling, the production node has
serde implemented in `GroupingSetAggregationNode.cc`, and the production test
lives under `cpp/velox/tests/`. Their
remaining verification caveats are recorded in `DEFENSIBILITY.md` and
`DEPLOYMENT_RUNBOOK.md`.

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
Pinned Velox's `GroupingSet::getOutput()` already clears a non-global hash table
with `freeTable=true` when the iterator reaches exhaustion. The operator makes
that lifetime explicit with `resetTable(true)` at completion rather than
describing an allocation-retention optimization that does not exist. A global
set has no table; after emitting its intermediate row the operator calls
`resetGlobalAggregation()` so its reusable row, string allocator, and external
aggregate state can begin a new pressure cycle. An abandoned set additionally
drops its `GroupingSet` entirely, after which it is a pure projection forever.

**Budget.**
Each non-bypassed grouping set starts with the ordinary
`max_partial_aggregation_memory` soft threshold and may double it after a useful
pressure drain, up to `max_extended_partial_aggregation_memory`. Before making
an explicit pool reservation, the operator subtracts existing unused
reservation and requests only the headroom useful before the shared target.
This lets several node-local doublings reuse Velox's 8 MiB reservation quantum.

That extended limit is also `operatorFlushTargetBytes_`, the target for the sum
of all flushable grouping-set allocations, including global aggregate state.
It is not a strict allocation cap: target
checks occur at input and top-level-drain boundaries, while hash tables grow in
capacity steps and descendant hard caps are checked during a cascade. Before
accepting another input batch, the operator repeatedly drains local hard/soft
violations or the largest shared-target candidate. A pressure-drained global
set emits one mergeable intermediate row and is then reset with Velox's
`resetGlobalAggregation()` API. No new config key is required.

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
