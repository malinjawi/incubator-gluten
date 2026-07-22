# Fused grouping-set aggregation — defensibility document

**Status:** not mergeable as a single change. Splits into three PRs, one of which is ready today.
**Audience:** Apache Gluten / Velox reviewers, and engineering management.
**Date of record:** 2026-07-22. Every measurement below was taken on the tree at that date.

This document is written to be read adversarially. Section 5 lists everything known to be
wrong with the work, ranked, before any reviewer has to find it. If you find a defect that
is not in section 5, that is a reporting failure and the rest of this document should be
treated with proportionally more suspicion.

---

## 1. WHAT THIS IS

**Changeset A (Gluten, JVM + C++ bridge).** Spark plans `GROUP BY ROLLUP/CUBE/GROUPING SETS`
as a partial aggregate above an `Expand` that duplicates each input row once per grouping
set. Changeset A contains two independent things. The first is `LazyAggregateExpandRule`,
which rewrites that shape into three stages: a partial aggregate at the *finest* grain
below the Expand, the Expand duplicating already-aggregated buffers, and a partial-merge
aggregate above it before the shuffle. This is a pure JVM plan rewrite, it degrades by
being a no-op, and it is the piece that is close to mergeable. The second is a fused
branch: when the shape qualifies, the rewrite instead emits a marker on the Expand
(`isRollup=1`) which the Substrait→Velox converter (`SubstraitToVeloxPlan.cc`,
`toGroupingSetAggregation`) collapses into a single Velox `GroupingSetAggregationNode`,
deleting the Expand and the merge stage entirely. The fused branch is off by default and
is *not* ready.

**Changeset B (Velox, upstream).** A new plan node `GroupingSetAggregationNode` and
operator `MultiGroupingSetAggregation` that computes every grouping set in one pass, with
a derivation lattice: each grouping set is aggregated from the smallest already-aggregated
superset rather than from raw input, so `{a}` is derived from `{a,b}` instead of re-probing
the raw stream. Also in Changeset B, and separable from it, is a two-line correctness fix
to the existing upstream `HashAggregation` operator (`needsInput()` must return false while
`input_` is held, plus a `VELOX_CHECK_NULL(input_)` in `addInput()`); without it the
existing operator silently drops rows in the post-abandon and distinct-with-new-groups
buffering states.

---

## 2. THE CLAIM SET

### 2.1 What is claimed

| # | Claim | Exact scope | Evidence |
|---|---|---|---|
| C1 | The fused operator computes a 4-set prefix rollup **pre-shuffle stage** 3.0–3.5x faster than the vanilla Expand+partial-aggregate pipeline. | One stage, one query shape, one dataset. Hand-constructed C++ plan in `Q67RollupBenchmarkTest.cpp`. TPC-DS q67 SF10 input columns. **With the bypass lane enabled.** | `Q67RollupBenchmarkTest.cpp`, run by hand. Vanilla 6,768–7,909ms; fused 2,235–2,886ms. |
| C2 | The bypass lane (forwarding the finest-grain set's rows without re-aggregating them) is worth 2.29x on its own. | Same harness, same query. | `NO_BYPASS` control arm 5,312ms vs 2,318ms with bypass. |
| C3 | The derivation lattice reduces hash probes below the vanilla flat fan-out. | Structural, from the plan: a rollup of n sets probes n tables of geometrically shrinking cardinality instead of n tables each fed the full raw stream. | Probe counts from the same harness. |
| C4 | The three-stage rewrite (non-fused) is semantically equivalent to the original plan. | All shapes admitted by `LazyAggregateExpandRule`'s guards. | Result-comparison tests in `LazyAggregateExpandSuite.scala`; argument in section 4. |
| C5 | The `HashAggregation::needsInput()` fix repairs a live row-loss bug in upstream Velox. | Upstream `HashAggregation`, independent of everything else here. | Revert it and `flushCorrectnessSmallBudget` drops rows. Two regression tests added to `AggregationTest.cpp`. |
| C6 | The exactly-once invariant of the fused operator holds: no grouping set's table is fed while its own output iterator is live, and no node is drained twice. | The operator's drain state machine. | Audited path-by-path (section 4.2). Not broken by any of the 34 tests. |

### 2.2 What is explicitly NOT claimed

Read this list before quoting any number.

- **Nothing has ever executed inside real Gluten.** Not one query. The Gluten C++ side has
  never been compiled on the machine where this work was done. Every Gluten-side test is
  planning-only; `FusedGroupingSetAggregateSuite.scala` says so in its own header comment.
  The first real Gluten plan the fused path meets will fail (see D1).
- **C1 is not an end-to-end q67 number.** It is one pipeline stage in a hand-fed C++
  harness that never goes through Gluten's `getAggRel`. End-to-end q67 speedup is unmeasured.
  There is no claim about it.
- **C1's 3.0–3.5x is the *bypass* arm, and the bypass lane is dead through Gluten.** The
  JVM hardcodes `childGrouped=0` (`ExpandExecTransformer.scala`), and
  `SubstraitParser::configSetInOptimization` (`SubstraitParser.cc:285`) only returns true
  when the character after the key is `1`. So `childGroupedSet` is always `nullopt` in the
  integrated path. **The number that corresponds to Changeset A as written is the
  `NO_BYPASS` arm: 5,312ms vs 6,768–7,909ms vanilla, i.e. ~1.27–1.49x on that stage.** Any
  presentation that quotes 3.0–3.5x for the integrated path is wrong. This was found by
  review, not by measurement, and it is disclosed here rather than left to a reviewer.
- **No performance number in this document was re-verified during the verification pass.**
  Gluten C++ cannot be compiled on this machine. C1, C2 and C3 stand or fall on
  reproduction by a third party (section 7).
- **No claim of production readiness for the fused branch.** It is off by default,
  experimental, and has a known hard-crash class (D1).
- **No claim that memory is bounded.** The operator's documented "ceiling" is advisory and
  is measurably overshot (D8).
- **No claim that `reclaim()` works.** It has never executed (D10).

---

## 3. ARCHITECTURE

### 3.1 The problem

Spark's rollup plan re-reads the same row once per grouping set. For a 4-set rollup over an
N-row input, the partial aggregate probes 4N times, on a stream that the Expand has already
inflated 4x. The aggregation is done at the coarsest grains against raw input, which is
exactly where it is least selective.

### 3.2 The design

Two independent ideas, deliberately kept separable:

**(a) Aggregate first, expand second (JVM only).** Put a partial aggregate at the finest
grain *below* the Expand. The Expand then duplicates aggregate *buffers* rather than rows.
A partial-merge aggregate above the Expand reduces per set before the shuffle. This is
plan-level only, needs no native change, and is the piece with real test coverage.

The merge stage above the Expand is not optional. Deleting it reintroduces the
high-cardinality shuffle regression documented as GLUTEN-7986 in the ClickHouse backend: on
q67 SF10, shuffle rows go from ~4.8M merged to ~5.75M un-merged *per set*. Correct results,
worse shuffle.

**(b) Fuse the whole thing into one operator (Velox).** Replace partial-agg → Expand →
partial-merge with a single operator holding one hash table per grouping set, wired into a
derivation forest. Each set is fed from the smallest already-aggregated superset. On a
prefix rollup this is a chain: raw → `{a,b,c}` → `{a,b}` → `{a}` → `{}`, so each table
sees only the *output cardinality* of its parent rather than the raw row count. The
`gid` column is carried verbatim as planner-supplied opaque data and is never recomputed.

### 3.3 Alternatives considered and rejected

- **zhouyuan's flat fan-out (gluten#12052, stale-closed).** One operator, n independent
  tables, each fed the raw stream. Rejected on measurement: **the probe count is identical
  to vanilla.** It saves the Expand's row materialisation and nothing else. Which leads to:
- **"Avoid materialising the expanded rows" as a rationale.** Rejected as a *stated*
  rationale because it is false. Velox's `Expand` is already effectively zero-copy — it
  shares child vectors and reprojects. There is no materialisation to avoid. The win has to
  come from the probe count, which is why the lattice, not the fusion, is the load-bearing
  idea. Anyone selling this work on "fewer copies" is selling it wrong.
- **Sort-based streaming aggregation over the grouping-set prefix.** A prefix rollup is a
  sequence of prefixes, so one sort on the finest key order streams every level. Rejected:
  it forces a full sort where the vanilla plan needs none, and it does not generalise to
  CUBE or arbitrary GROUPING SETS, which are an antichain in the general case. Would be a
  strictly better plan for the narrow prefix-rollup-of-a-sorted-input case; that case is not
  common enough to justify a second operator.
- **Extending `HashAggregation` with a set-of-tables mode.** Rejected on blast radius.
  `HashAggregation` is one of the most load-bearing operators in Velox; adding a multi-table
  drain state machine, a per-node flush policy and a cascade to it would put every existing
  aggregation query behind this change's review. A separate operator is reviewable in
  isolation and inert when no `GroupingSetAggregationNode` appears in the plan
  (the translator returns `nullptr`).

### 3.4 The one architectural consequence that must be declared

Substituting the fused operator for partial-agg + Expand + partial-merge **silently removes
barrier support and spill support from that pipeline.** `GroupingSetAggregationNode` does
not override `supportsBarrier()` (defaults false) where `AggregationNode::supportsBarrier()`
returns true, and `canSpill()` returns false. The node is internally consistent — the
operator genuinely has no `startDrain`/`finishDrain`/`isDraining` — and the spill rationale
(this holds partial state; the answer is to flush, not spill) is sound. But it is a
capability regression relative to what it replaces and it belongs in the PR description, not
in a footnote.

---

## 4. CORRECTNESS ARGUMENT

### 4.1 The central invariant

> The rewritten subtree produces the same multiset of rows, with the same schema and the
> same column order, as the aggregate it replaces.

Two supporting facts carry it:

**Duplicate partial states are legal pre-shuffle.** The Expand duplicates aggregate
*buffers*, so the same partial state reaches the Final aggregate once per grouping set it
belongs to. Final-mode merging is associative and commutative over these buffers, so
merging duplicated buffers at different grains yields the same result as aggregating the
raw rows at each grain. This is why an *ignored* marker (a build without the operator) is a
**performance cliff, not a wrong answer** — the plan degrades to un-merged partial states
which the Final stage still merges correctly. State this explicitly in the PR; it is the
first question a reviewer asks.

**gid is never recomputed.** Spark's `GROUPING_ID` bit order is version-dependent, and a
masked NULL is indistinguishable from a data NULL once written. So the gid literal from the
Expand's projection is carried through verbatim as opaque planner data. Verified against
`ExpandRelNode.java:61-69` for the wire layout (level-major,
`fields(level).switching_field().duplicates(column)`) and against `NullLiteralNode.java:31`
for the null encoding.

### 4.2 Trap-by-trap

| Trap | Handling | Tested |
|---|---|---|
| **NULL from a rolled-up key vs. NULL in the data** | Never distinguished by the operator. gid carries the distinction and is passed through untouched; masked slots are literal nulls on the wire (`literal_type_case() == kNull`). | Result-comparison tests in `LazyAggregateExpandSuite`. |
| **gid / mask / level indexing** | Independently re-verified. Scala emits `[k1..k_{N-1}, gid, buf...]`; `shapeFusible` pins the literal slot at index `N-1`; C++ derives `gidSlot = numKeys`. Consistent, no off-by-one. `set.keyIsActive` is indexed by child channel; the output ProjectNode reorders by `keySlotToChannel[slot]`; consistent under an arbitrary permutation, not just identity. | Structural review + `MultiGroupingSetAggregationTest`. |
| **Empty input** | `receivedInput_` is set *below* the zero-row early return, so it means "at least one row seen". This is what prevents the global (grand-total) node emitting a spurious identity row for an empty input. JVM side has a `bottomGroupingKeys.isEmpty` guard. | `LazyAggregateExpandSuite` "degenerate grouping sets". |
| **Duplicate grouping sets** | Rejected. Enforced in the node constructor **and now at the `buildDerivationPlan` boundary** (a `VELOX_CHECK` was added, since the function is an inline free function in an installed public header whose precondition previously lived in a different file). JVM mirrors the check. | `latticeRejectsDuplicateMasks` (new); "duplicate grouping sets keep duplicate result rows". |
| **Aggregate filter clauses (`FILTER (WHERE …)`)** | Excluded. `aggExpr.filter.isDefined` bails the rule; C++ `VELOX_CHECK`s that no measure has a filter. | "aggregate filter clause" test. |
| **DISTINCT aggregates** | Excluded by `isDistinct` guard. The `RewriteDistinctAggregates` look-alike Expand is separately rejected because its aggregate functions reference expand-created attributes. | "single count distinct" / "multiple count distinct" — but see D14, these tests are weak. |
| **Float non-associativity** | Under strict mode, float/double `sum` is excluded, mirroring `FlushableHashAggregateRule` policy. | "floating point sum … strict mode". |
| **AQE re-invocation / idempotency** | `initialInputBufferOffset == 0` guard. Every emitted merge stage carries offset ≥ 1; the fused path emits no Regular agg at all. Reasoning verified correct. | **Not tested.** See D15. |
| **Slot instability across projections** | `buildReplaceAttributeMap` used `collectFirst`, silently ignoring a second, different attribute in the same slot. **Fixed:** all non-literal entries at a slot must now be `semanticEquals`; a disagreeing slot is dropped rather than guessed. Latent, not live — no query was found that reaches it. | Not directly tested (no reaching query exists). |
| **Exactly-once drain** | `plan_.order` is popcount-descending and a parent is a strict superset, so a parent's rank is strictly greater and `drainStack_` is a strict root-to-leaf path. `flushReason` returns `kNone` for a draining node and `pushDrain` re-checks `!draining`. No node is fed while its iterator is live; no node is pushed twice. The final sweep visits each node once, after every ancestor. | Audited exhaustively; not breakable by any of the 34 tests. |
| **Use-after-move / dangling vectors** | Zero-copy `project()`/`derive()` sharing is protected by `use_count`-based reallocation in `prepareForReuse`; `extractGroups` copies out-of-line strings into vector-owned buffers, so `close()` resetting the grouping sets cannot dangle a queued batch. | Audited. |

### 4.3 What the correctness audit found *sound* and worth stating affirmatively

- No empty vector can be emitted (`Driver.cpp:1382` would abort); every queued batch derives
  from a non-empty source.
- Returning `nullptr` while not finished, not blocked, and not needing input is normal Velox
  behaviour, not a stalled driver — the Driver falls through to the upstream operator and
  re-enters `getOutput`. Costs a lap, hangs nothing.
- All mask arithmetic is correct at the 64-key boundary (`uint64_t`, `1ull << j` with
  `j ≤ 63`, `VELOX_CHECK_LE(numKeys, 64)`).
- Every division is guarded; the int64 overflow margin on `100 * abandonWindowOutRows` is
  ~9.2e16 rows.
- The Substrait symbol surface was audited exhaustively: **no fabricated, misspelled,
  wrong-arity or wrong-order symbol was found.** Every symbol was checked against its
  declaration.
- Malformed Substrait is checked, not UB: projection width, key-slot form, slot→channel
  stability, permutation completeness, gid integrality, aggregate pass-through, absence of
  masked aggregates. No unchecked `duplicates[i]`, no unchecked `*channel`.
- An unrecognised marker falls through cleanly to the untouched vanilla Expand path.

---

## 5. KNOWN DEFECTS AND GAPS

Ranked. "FIXED" means fixed and verified in the current tree. "OPEN" means a comment was
left in the code at the cited site and nothing else was done.

### Blockers

**D1 — The fused path hard-fails on any aggregate with a multi-column partial buffer. FIXED (by exclusion).**
`HashAggregateExecTransformer.scala:91-95` (`extractStructNeeded`) returns true whenever a
Partial-mode function has `aggBufferAttributes.size > 1`, and `:415-418` then wraps the
AggregateRel in a **ProjectRel**. The tagged Expand's Substrait input is therefore not an
AggregateRel, and `SubstraitToVeloxPlan.cc:1163-1167`'s
`VELOX_CHECK(expandRel.input().has_aggregate(), ...)` fires — as a `VeloxRuntimeError` that
kills the task, not a fallback, because the marker is withheld during validation.
This hits `avg` (buffer `(sum, count)`) and `sum` over `DecimalType` (buffer
`(sum, isEmpty)`) — **always**, not occasionally.

**TPC-DS q67 aggregates `sum(ss_sales_price)` over `decimal(7,2)`. The flagship benchmark
query is in the broken class.** This was structurally invisible to every measurement taken,
because the C++ harness hand-builds its plan and never goes through `getAggRel`.

*Fix applied:* `shapeFusible` now additionally requires
`aggregateExpressions.forall(_.aggregateFunction.aggBufferAttributes.length == 1)`. Two
regression tests added to `FusedGroupingSetAggregateSuite` (avg; decimal sum) asserting no
marker is emitted and the merge stage is kept. **This excludes q67 and most of the value
from the fused path.** The OPEN comment records that consequence. The real fix — teaching
the C++ side to see through the extract-struct ProjectRel and re-pack flattened columns — is
substantial work and interacts with the unverified `resolveIntermediateType` agreement.

*Note on a related report:* a secondary arithmetic error at `.cc:945` (`numAggregates =
measures().size()` counts aggregate expressions M, while the child emits flattened buffer
columns B) is real but **not independently reachable** — `extractStructNeeded()` and `B > M`
are the same predicate, so the `has_aggregate()` check always fires first. It is one defect,
not two. Do not describe it as "a second silent catastrophic bug".

**D2 — Validation/transform divergence removes Gluten's fallback net. OPEN.**
`ExpandExecTransformer.scala` emits the marker only when `validation = false`, deliberately
(the comment says so). `toGroupingSetAggregation` contains **13 `VELOX_CHECK`s**
(`SubstraitToVeloxPlan.cc:947, 961, 971, 986, 994, 1002, 1012, 1021, 1032, 1040, 1062, 1106,
1165`), every one of which is therefore an unrecoverable task failure with no plan-level
recovery. The rule compensates by re-implementing three native invariants in Scala
(duplicate-mask check, permutation check, masked-aggregate check) — three hand-maintained
mirrors of a native contract, none covered by an executing test. Any drift is a production
query failure. D1 is the first instance; it will not be the last.

*Not fixed, deliberately.* The cheaper of the two fixes — have the native side degrade to a
plain `ExpandNode` instead of `VELOX_CHECK` when the shape is not fusible, which also
removes the need for all three Scala mirrors — lives in Gluten C++, which cannot be compiled
on this machine. Shipping an unverified change there would be worse than shipping none.
A precise OPEN at `LazyAggregateExpandRule.scala:335` names both options.
**This is the single most defensible objection a reviewer will raise and it cannot be
argued away.**

**D3 — `reclaim()` has never executed, by any test, ever. OPEN.**
~200 lines (`MultiGroupingSetAggregation.cpp:988-1183`) plus `phase2ReclaimSafe_`,
`reclaiming_`, `startSize` resumption and `folly::makeGuard` unwinding. `FlushReason`
instrumentation across every test individually: `kEndOfInput` 21, `kHardCap` 4,
`kSoftBudget` 1, `kOperatorCeiling` 1, **`kReclaim` 0**, **`kAbandoned` 0**. Meanwhile
`canReclaim()` returns `true` unconditionally, so the arbitrator **will** call this in
production, and getting it wrong trips `VELOX_CHECK_GE(reclaimedBytes, 0)` — which fails the
*query*, not the reclaim.

Worse, Phase 1 is provably vacuous: `GroupingSet::getOutput` calls
`table_->clear(freeTable=true)` before returning false, so `allocatedBytes()` is already 0 at
every drain completion. Phase 1 hunts for `numRows() == 0 && allocatedBytes() > 0`, which is
unsatisfiable; measured `reclaimPhase1Hits = 0` in every run. And `phase2ReclaimSafe_`
inspects only *extraction* types — it says nothing about cascade-driven descendant-table
growth, so its "exposure is at most one output batch" bound is unaccounted-for.

*Next step:* an arbitration test using `memory::testingRunArbitration()` and the
`SharedArbitrator` fixtures, as `AggregationTest` already does. This is real work and belongs
in its own change. **The fused branch must not go default-on until this exists.**

**D4 — Changeset A is a hard fork-dependency on unmerged Velox.**
`VeloxBackend.cc:190` calls `registerMultiGroupingSetAggregation()` unconditionally and
`SubstraitToVeloxPlan.cc:26` includes `GroupingSetAggregationNode.h` unconditionally. Gluten
C++ **will not compile against upstream Velox at all**. An ASF reviewer will block on this
alone. It is the structural reason the PR must be split.

*Related, FIXED:* `VeloxConfig.scala` documented "without it the marker is ignored and the
plan degrades" — describing a build that cannot exist, because it does not link. The doc has
been rewritten to say so, and to state the multi-column-buffer exclusion from D1.

**D5 — The plan node is in the wrong namespace and the wrong file, for a reason. (From the API review; not re-verified.)**
`GroupingSetAggregationNode` is `facebook::velox::exec`, declared in `exec/`, deriving from
`core::PlanNode`. Every production Velox node is `core::`-namespaced in `core/PlanNode.h`;
the only other out-of-`core` nodes are internal scaffolding and a doc example. It cannot move
because its constructor calls `resolveIntermediateType(...)` from the member-initializer
list, which requires a populated aggregate function registry and forces
`#include "velox/exec/AggregateFunctionRegistry.h"` into a node header — an `exec/ → core/`
layering inversion. `AggregationNode`'s ctor touches no registry.

The header pre-concedes this objection. Pre-conceding is not retiring. The correct answer is
for the **translator** to build the `CallTypedExpr` with the right intermediate type, as
`AggregationNode` at `kIntermediate` already does, and for the node to trust it. Same class:
`outputType_` also reads `source->outputType()`, which no other Velox node does and which
makes the node un-rebuildable; use `groupingKeys[i]->type()` instead (the nullability essay
in the header is correct, and it means the reason for reading the source type is gone).

Knock-on gaps in the same family, all real, all must-fix before an upstream PR: no `Builder`;
no `accept(PlanNodeVisitor&, …)` override; no `addSummaryDetails` override (the default
truncates, and `addDetails` emits an unbounded per-set bitstring loop, so a wide CUBE
summary is garbage); **no serde** — `serialize()` throws, which is a permanent hole in
`PlanNodeSerdeTest` and **breaks distributed execution**, since Prestissimo ships plan
fragments as serialized JSON; and `VELOX_CHECK` used where the codebase uses
`VELOX_USER_CHECK` for planner-supplied invariants.

### Confirmed, non-blocking

**D6 — The bypass lane is dead through Gluten. Not a code defect. OPEN comment added.**
Covered in section 2.2. The C++ is correct; the *claim* built on it was not. Recovering it
requires making the bottom aggregate a *non*-flushable Regular aggregate (which cannot
abandon), trading the abandon safety valve for the bypass lane. That is a real design fork
worth choosing deliberately rather than defaulting into. The OPEN at the marker site records
the measured 2.29x and the trade.

**D7 — Runtime stats were numerically meaningless. FIXED.**
`Operator::addRuntimeStat` accumulates into `RuntimeMetric::sum`, and `recordNodeStats` runs
per drain cycle passing **running totals**. Over k cycles the reported sum is ≈ k·total/2 —
measured 155 drains on one node in `noMoreInputDuringDrain`, inflating `gsagg.set1.inputRows`
~78x. `.inputRows`, `.outputRows`, `.flushTimes` and `gsagg.reclaim.count` switched to
`setRuntimeStat`. `.flushRowCount` / `.aggregationPct` / `.hashTableBytes` left on
`addRuntimeStat` — those are genuine per-cycle samples and match upstream `HashAggregation`.

**D8 — `operatorBudgetCeilingBytes_` is not a ceiling. Comment FIXED; mechanism OPEN.**
Documented as "Ceiling on the SUM of all live tables"; enforced nowhere.
`overOperatorCeiling()` is only *sampled* in `flushReason()`, `addInput` arms at most one
drain per batch, and the cascade checks only `kHardCap`. Measured peak total node bytes
against the ceiling on `flushCorrectnessSmallBudget`:

| ceiling | peak bytes | overshoot |
|---|---|---|
| 16 MB | 4,952,288 | 0.30x |
| 4 MB | 4,952,288 | 1.18x |
| 1 MB | 5,103,264 | 4.87x |
| 256 KB | 5,065,408 | 19.3x |
| 64 KB | 10,353,824 | 158x |
| 4 KB | 13,257,088 | 3237x |

Other tests reach 5248x (`noMoreInputDuringDrain`), 2774x
(`aggregateVariableWidthUnderFlush`), 1345x (`forcedFlush`). Honest framing: **at the Velox
default `max_extended_partial_aggregation_memory` of 16 MB the operator stays inside budget
on this workload; it degrades without bound below ~4 MB.** The `kHardCapMultiple = 4` escape
hatch cannot bound anything sub-MB because `HashTable` slot-array growth is power-of-two
granular — a single node was measured holding 2,424,832 bytes against a 4,096-byte soft
budget. The docstring is now "advisory" and records the measurement and the ~256 KB global
node floor. Not enforcing it now, because enforcement changes flush behaviour and needs its
own measurement.

**D9 — The global node's memory is permanently unreclaimable and counts toward the ceiling. OPEN.**
Measured 262,144 bytes that no path frees: `resetTable()` is a no-op on it,
`resetGlobalAggregation()` is deliberately never called, reclaim Phase 1 skips it — but
`totalNodeBytes()` includes it. So whenever the budget is below ~256 KB,
`overOperatorCeiling()` is permanently true for the operator's life, which permanently
disables `maybeGrowBudget`, forces `shouldFreeTable` true, and returns `kOperatorCeiling` for
every non-empty node on every check. That is exactly the flush-every-batch cliff the
`shouldFreeTable` comment claims to have fixed. Either exclude global nodes from
`totalNodeBytes()` or state the floor. Currently: floor stated.

**D10 — `shouldFreeTable()` is dead code and its 24 lines of comment describe impossible states.**
Same chain as D3: `clear(freeTable=true)` already ran, so by the time `completeNodeDrain`
reaches `resetTable(freeTable)` the table is gone and the argument cannot matter. Runtime
proof: across `forcedFlush` (41 resets), `noMoreInputDuringDrain` (159),
`flushCorrectnessSmallBudget` (83), **every reset of a node with `numActive() > 0` found
`allocatedBytes() == 0`**. The "CEILING SELF-PERPETUATION" and "ZERO-ROW-BUT-STILL-FULL"
comments describe states that cannot arise on the drain path.

**D11 — Dead abandoned-state branch. FIXED.**
`abandoned = true` is written only at `.cpp:597` (`maybeAbandonAfterDrain`, called from
`completeNodeDrain:951`), which drops `groupingSet` eight lines later at `:964`. So
`{abandoned && groupingSet != nullptr}` never survives, and `feedNode`'s 12-line branch was
unreachable behind the `groupingSet == nullptr` check at `:489`. Its comment described a
`flushReason() == kAbandoned` pickup mechanism **that does not exist** — a reviewer reading
it concludes the author does not know their own state machine. Branch replaced with
`VELOX_DCHECK(!node.abandoned)` and an accurate comment. `kAbandoned` annotated as currently
unreachable but retained as a fail-safe, because `maybeGrowBudget` genuinely reads
`abandoned` inside the window.

**D12 — Lattice preconditions were unenforced at the function boundary. FIXED.**
`buildDerivationPlan` is an inline free function in an installed public header;
duplicate-mask rejection lived only in the node ctor, and `bypass` was never required to be
a lattice root. A bypassed non-root would forward its parent's already-aggregated rows
verbatim tagged with its own gid — correct but silently degenerate, and one planner change
away from a large silent row blow-up. `VELOX_CHECK`s added for both. New tests:
`latticeRejectsDuplicateMasks`, `latticeRejectsBypassOfANonRoot`, `latticeRejectsEmptyMasks`,
`latticeNonAdjacentParent` (a branch `latticeCubePicksSmallestParent` structurally cannot
reach), `latticeWideMasks` (bits 61–63). All 5 pass.

**D13 — `kOperatorCeiling` drains the topologically-first node, not the largest.**
The predicate is global but attached to an arbitrary node. On a prefix rollup this is benign
(popcount-descending order puts the biggest table first); it breaks on an antichain or on
skewed cardinalities, where draining a small node cascades *more* rows into the descendants
that triggered the ceiling. `kHardCap` eventually rescues it, so it is not a livelock — the
ceiling mechanism just does no useful work in that regime.

**D14 — `maybeGrowBudget` reserves per-node and never releases.**
Calls `pool()->maybeReserve(grown - node.maxPartialBytes)` **per node**, where upstream
`HashAggregation` reserves once for one grouping set. The doublings sum to
≈ `maxExtended - maxPartial` per node, held simultaneously across n nodes, and
`pool()->release()` is only called inside `reclaim()`, which never runs (D3). With 9 sets and
a 16 MB extended budget that is ~140 MB of reservation against a documented 16 MB ceiling.
Directly contradicts the header's own rationale.

**D15 — Wasted work and a hidden side effect in the cascade.**
`if (flushReason(child) == FlushReason::kHardCap)` — `flushReason` is neither const nor
cheap: it calls `overOperatorCeiling()` → `totalNodeBytes()` → `allocatedBytes()` over every
node, for every child, on every drain batch; and `isPartialFull()` can trigger a **rehash**
as a side effect. The result is discarded unless it is exactly `kHardCap`. Test the hard cap
directly. Same O(n²)-per-batch pattern in `addInput`'s pressure scan.

**D16 — `prepareForReuse` never reuses anything.** The RowVector is always uniquely owned so
the reuse branch is taken, but every child is shared downstream so every child is
reallocated. Net: full reallocation plus the cost of the checks, every drain batch. Safe —
and worth stating in the PR that this refcount-driven replacement is *exactly* what makes the
zero-copy `project()` and the bypass lane memory-safe. That argument is currently only
implied.

**D17 — Test CMakeLists bin-packing shifted. FIXED.**
`MultiGroupingSetAggregationTest.cpp` was inserted at position 7 of group2, making it 11
files and shifting every subsequent file. Moved to the end with a comment explaining the
positional invariant. Three `velox/exec/CMakeLists.txt` entries also moved to their correct
alphabetical slots.

**D18 — `Q67RollupBenchmarkTest.cpp` is in no `CMakeLists.txt`. OPEN.**
It has only ever been compiled by hand. **A file no CI can build is not evidence** — this is
the file behind claims C1, C2 and C3. Wiring it as a test would put a 1.4 GB external
dependency in a hermetic test target; the right home is `velox/benchmarks/` with
`velox_add_benchmark`. Documented in the file header rather than guessed at.

**D19 — Debug scaffolding must go before merge.**
`getenv("GSAGG_DEBUG")` / `getenv("GSAGG_FLATTEN")` in `initialize()`, the `dbgFlatten_`
deep-copy in `project()`, `dbgDump()` and its `fprintf(stderr)` block, and four
`steady_clock::now()` pairs on the hot path (`project()` runs per output batch — 8 clock
reads per drain batch). `mutable int64_t dbgProjectNanos_` mutated from a `const` method is a
further smell. *Worth keeping:* the `dbg*Rows` four-lane ledger — it is what localised the
silent row loss behind the `HashAggregation::needsInput()` fix, increments are per-batch, and
the cost is unmeasurable. Promote it to real `addRuntimeStat` counters (with D7 fixed).

**D20 — Lattice doc claims the wrong `n`.**
`"O(n^2) mask comparisons; n <= 64 in any realistic plan"` — `n` is `masks.size()`, the
number of **grouping sets**, not keys. The 64 bound is on keys. `CUBE(10)` is 1024 sets, ~1M
comparisons. The conclusion (acceptable, runs once in `initialize()`) survives; the stated
reasoning is false, and a hostile reviewer will use it to question the rest of the header.

**D21 — Naming.** Node is `GroupingSetAggregationNode`; operator is
`MultiGroupingSetAggregation`. Velox pairs `ExpandNode`/`Expand`, `GroupIdNode`/`GroupId`.
Pick one stem. And `GroupingSet` is already a heavily-loaded name in `exec/` — the node reads
as "a node wrapping a `GroupingSet`", which is not what it is.

### Over-restrictive guards (lost performance, all defensible, all should be stated in the PR)

- `apply` matches only `Expand`, `Project(Expand)`, `Filter(Expand)` — never
  `Project(Filter(Expand))` or `Filter(Project(Expand))`. Silent no-op on those shapes.
- `resultExpressions.forall(_.isInstanceOf[Attribute])` rejects any partial aggregate
  carrying a trivial cast in its result list.
- `isSupportedAggregateExpression` is a five-function allowlist. `first`, `last`, `bit_and`,
  `collect_set` all merge associatively and are excluded. Fine as a v1 posture — say so
  explicitly rather than leaving it to be asked.
- `copyTagsFrom` propagates validation/fallback tags onto structurally different nodes. Not
  currently a wrong-answer bug (`doValidate()` does not cache), but a tag saying "this node
  validated" on a node that never validated is a latent trap. Narrow it to the tags actually
  intended.
- `FlushableHashAggregateRule` no longer stops at the aggregate in the fused path (there is
  no Regular merge aggregate to stop at), so the walk keeps descending. In practice a deeper
  Partial aggregate always has an intervening `ShuffleExchangeLike` and no misconversion was
  constructible — but the fused path changes that rule's traversal invariant without saying so.

### Behaviour note, not a defect

Under ANSI `failOnError`, reordering integer sum merges can move *where* an overflow is
detected. This is pre-existing in vanilla partial/final aggregation and in flushable
aggregation, so it is not a new liability — but have the answer ready.

### 5.1 Test state, with attribution

**Measured on the current tree, each test in its own process:**

| | before this verification pass | after |
|---|---|---|
| `MultiGroupingSetAggregationTest` | 29 total, **23 pass, 6 fail** | 34 total, **28 pass, 6 fail** |

All 5 new lattice tests pass. Nothing was weakened or deleted.

**The 6 failures are pre-existing upstream `Expand + kIntermediate` reference-plan crashes,
not fused-operator defects.** They abort with `Type mismatch: BIGINT vs. ARRAY<BIGINT>` at
`VectorHasher.h:188`, in the context `Operator: Aggregation[4]` — a plain `Aggregation`,
i.e. the *comparison* plan, not `GroupingSetAggregation`. That attribution is confirmed;
they are still 6 red tests and a reviewer will ask.

**Two earlier counts circulating about this tree are wrong. Do not quote them.**
"22 pass / 6 fail" (from an older facts sheet) and "27 tests, 26 pass, 1 fail" (from the
operator review, taken against a partially-reverted tree) are both wrong. The numbers above
are the measured ones.

**Untested paths, ranked by risk:**
1. `reclaim()` — zero coverage (D3).
2. Any *executing* fused-path test. `FusedGroupingSetAggregateSuite` is planning-only by its
   own admission. An executing test would have caught D1 on the first run.
3. AQE-enabled re-invocation / idempotency. Every plan-shape test that matters disables AQE.
4. Explicit assertion that the rewritten subtree's `output` equals the original aggregate's
   `output` — the rule's central invariant is asserted only in a comment.
5. `Project(Filter(Expand))` shape (expected no-op).

**Tests that do not test their name (D14-class, JVM side):**
- `LazyAggregateExpandSuite:186` "single count distinct rewrites the dedup aggregate" —
  asserts only that *some* lazy expand fired. With two Expands in the plan it never checks
  which one. Would pass if the wrong Expand were rewritten.
- `:196` "multiple count distinct stays correct" — body is `{ _ => }`. Zero plan assertions.
- `:243` "high cardinality keys with early abandon" — asserts plan shape, asserts nothing
  about abandon engaging or about shuffle volume. Since avoiding the GLUTEN-7986 shuffle
  regression is the stated justification for the entire merge stage, this is the one test
  that should measure bytes and does not.

**Tests verified to genuinely test their name** (checked guard-by-guard): "degenerate
grouping sets", "non-atomic grouping key", "aggregate filter clause", "floating point sum …
strict mode", "disabled by default when flushable aggregation is off", "duplicate grouping
sets keep duplicate result rows", "avg buffers are merged, not averaged".

### 5.2 Claims made in review that were REFUTED

Recorded so nobody re-litigates them:

- *"`<algorithm>` is included but unused in `GroupingSetLattice.h`"* — false; `std::stable_sort`
  is used at `:100-105`.
- *"`velox/exec/CMakeLists.txt` alphabetical ordering is broken"* — the list is already
  non-alphabetical upstream (`ExchangeSource.cpp`, `SerializedPage.cpp`, `Expand.cpp`). The
  local placement was still wrong and was fixed, but "violates a house invariant" overstates it.
- *"group2 holds 11 files, which is the CI regression"* — the insertion is real, but group6
  already held 11 files pre-change, so the timing budgets had already drifted. Correct
  finding, inflated severity.
- *"The M-vs-B off-by-one is a second, silent, catastrophic bug"* — real arithmetic,
  structurally unreachable. One defect (D1), not two.
- The test counts in 5.1.

---

## 6. PUNCH LIST TO MERGEABLE

### PR 1 — `HashAggregation::needsInput()` fix. **Ready to submit today.**

The strongest single artifact in either changeset. Land it first, independent of everything
else. It is a genuine upstream bug: without it the existing operator silently drops rows.

| item | effort | state |
|---|---|---|
| Split into a standalone Velox PR | 1h | not done |
| Title/body must describe **both** changes — `needsInput()` `&& input_ == nullptr` *and* `VELOX_CHECK_NULL(input_)` in `addInput()`. It is not a one-liner. | — | — |
| Two `AggregationTest.cpp` regression tests | done | verified load-bearing: revert the fix and `flushCorrectnessSmallBudget` drops rows |

Verified: both buffering states (post-abandon pass-through; distinct-with-new-groups) are the
only two writes to `input_` and both are now covered; no stall or deadlock on any path where
`input_` is non-null and `getOutput()` could return null; the Driver contract holds
(`addInput` is only reachable inside the `if (needsInput)` block), so the `VELOX_CHECK_NULL`
is safe; no throughput regression. There is merged in-tree precedent.

### PR 2 — Gluten three-stage rewrite (`enableVeloxLazyAggregateExpand` only). **Close.**

Stands on its own merits, has a real test suite, degrades safely (no-op), needs no native
change. Must **not** carry the fused branch, the marker, `shapeFusible`,
`FusedGroupingSetAggregateSuite`, or any of the C++ delta — shipping them together means one
reviewer objection blocks both.

**Must-fix before opening:**

| item | effort |
|---|---|
| Strip the fused branch, the `groupingSetAggregation` field and the marker from `ExpandExecTransformer` | 3h |
| Strip the unconditional `registerMultiGroupingSetAggregation()` / header include from Gluten C++ (D4) | 30m |
| Narrow `copyTagsFrom` to the tags actually intended | 1h |
| Fix the three tests that do not test their name (5.1) | 3h |
| Add the missing `output`-equality assertion for the rule's central invariant | 1h |
| Add an AQE-enabled idempotency test | 2h |
| PR body must state: correctness argument (4.1), the over-restrictive guards (§5), the ANSI overflow-detection note | 1h |

**Nice-to-have:** shuffle-bytes assertion on the high-cardinality test; `Project(Filter(Expand))`
no-op test.

### PR 3 — Velox `GroupingSetAggregationNode` + `MultiGroupingSetAggregation`. **Not close.**

**Must-fix before opening upstream:**

| item | effort |
|---|---|
| Move the intermediate-type resolution into the translator; stop calling `resolveIntermediateType` from the node ctor (D5) | 1–2d |
| Derive `outputType_` from `groupingKeys[i]->type()`, not `source->outputType()` (D5) | 4h |
| Move node to `core::` / `core/PlanNode.h` (unblocked by the two above) (D5) | 4h |
| Add `Builder`, `accept(PlanNodeVisitor&)`, `addSummaryDetails` (D5) | 1d |
| Implement serde + register in `PlanNodeSerdeTest` — **blocks Prestissimo** (D5) | 2d |
| `VELOX_CHECK` → `VELOX_USER_CHECK` for planner-supplied invariants (D5) | 1h |
| Remove all debug scaffolding; promote the `dbg*Rows` ledger to runtime stats (D19) | 4h |
| Fix the lattice `n` documentation (D20) | 15m |
| Delete `shouldFreeTable`'s impossible-state comments; either delete the function or document that it is a no-op on the drain path (D10) | 2h |
| Wire `Q67RollupBenchmarkTest.cpp` into `velox/benchmarks/` with `velox_add_benchmark`, or delete it (D18) | 4h |
| Resolve or attribute the 6 failing tests in the PR body | 2h |
| Pick one naming stem (D21) | 1h |
| Replace `Set{keyIsActive, gid}` with `Set{mask, gid}`, deriving `activeKeys` once in the operator — removes three encodings of one fact and makes the 64-key bound structural (B9) | 1d |
| Declare the barrier/spill capability regression in the PR body (§3.4) | 30m |

*Do not* let a reviewer talk you out of keeping `gid` as opaque planner data. That decision
is correct and well-argued.

**Must-fix before default-on (not before PR):**

| item | effort |
|---|---|
| An arbitration test for `reclaim()` using `memory::testingRunArbitration()` (D3) | 3–5d |
| Make Phase 1 non-vacuous or delete it; account for cascade-driven descendant-table growth in `phase2ReclaimSafe_` (D3) | 2–3d |
| Either enforce `operatorBudgetCeilingBytes_` in the cascade or keep it advisory and document the floor — currently advisory (D8) | 2d if enforcing |
| Exclude global nodes from `totalNodeBytes()` or state the ~256 KB floor (D9) | 1d |
| Release per-node reservations outside `reclaim()` (D14) | 1d |
| Drain the largest node, not the topologically first, under `kOperatorCeiling` (D13) | 1d |
| Test the hard cap directly instead of calling `flushReason` in the cascade (D15) | 2h |

### PR 4 — the fused branch. **Blocked on PR 3 landing.**

Do not open until: PR 3 is upstream; D1 is properly fixed (see through the extract-struct
ProjectRel, not just excluded — the exclusion removes q67 and most of the value); D2 is
resolved in favour of **native soft-degrade to a plain `ExpandNode`**, which also deletes all
three Scala mirrors; the bypass-lane fork (D6) is decided deliberately; and **something has
executed inside real Gluten.**

---

## 7. HOW TO REPRODUCE EVERY NUMBER

### 7.1 Velox build and test

```bash
cd "/Users/linjaboy/Documents/velox OSS/velox"
make debug            # or: cmake --preset debug && ninja -C _build/debug velox
ninja -C _build/debug velox                       # exit 0 on the recorded tree

# Test counts in 5.1 — each test in its own process. Running them in one
# process changes the result; the per-process form is the one that was measured.
BIN=_build/debug/velox/exec/tests/velox_exec_test
$BIN --gtest_list_tests --gtest_filter='MultiGroupingSetAggregation*' \
  | awk '/^[^ ]/{s=$1} /^  /{print s $1}' \
  | while read t; do
      $BIN --gtest_filter="$t" >/dev/null 2>&1 \
        && echo "PASS $t" || echo "FAIL $t"
    done | sort | uniq -c
# expected: 28 PASS, 6 FAIL, 34 total
```

### 7.2 The performance claims C1, C2, C3

**These are not reproducible by `ninja` alone — `Q67RollupBenchmarkTest.cpp` is in no
`CMakeLists.txt` (D18) and requires a TPC-DS SF10 dataset (~1.4 GB).** It has only ever been
compiled by hand. To reproduce:

1. Generate or obtain TPC-DS SF10 `store_sales`, and point the harness at it.
2. Compile `velox/exec/tests/Q67RollupBenchmarkTest.cpp` by hand against the built
   `velox_exec_test` object set, or wire it into `velox/benchmarks/` first (recommended —
   that is the punch-list item).
3. Arms to run, and what each corresponds to:
   - `VANILLA` — Expand + partial aggregate. Recorded: 6,768–7,909ms.
   - `FUSED` — fused operator, bypass lane on. Recorded: 2,235–2,886ms. **This is the
     3.0–3.5x arm and it is NOT what Gluten produces.**
   - `NO_BYPASS` — fused operator, bypass lane off. Recorded: 5,312ms. **This is the arm that
     corresponds to Changeset A as written** (~1.27–1.49x vs vanilla).
   - The 2.29x bypass figure is `NO_BYPASS` 5,312ms ÷ 2,318ms.

Report which arm any quoted number came from. Every one of these figures is single-machine,
single-shape, one stage, and none was re-verified in the verification pass.

### 7.3 Gluten build and checks

```bash
cd "/Users/linjaboy/Documents/velox OSS/incubator-gluten/.claude/worktrees/gluten-velox-aggregate-opt-e1be35"
# JDK 17 required — the default Zulu 8 breaks scalac -release.
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

mvn -Pspark-4.0 -Pscala-2.13 -Pbackends-velox test-compile   # exit 0; scalastyle "Found 0 errors"
mvn -Pspark-4.0 -Pscala-2.13 -pl backends-velox,gluten-substrait spotless:check   # exit 0
```

Gluten C++ **cannot** be built on the machine of record; that is why D4 exists and why no
integrated number appears in this document.

### 7.4 The memory measurements in D8, D9, D10

These required instrumentation that is **not in the tree**: sampling `totalNodeBytes()` at
every table insert in `feedNode`, and logging `allocatedBytes()` immediately before
`resetTable`. Re-adding it is ~30 lines. The `FlushReason` census in D3 came from a counter
on each `flushReason()` return, run over every test individually. The reset census in D10 was
a one-line log before `resetTable`. Anyone re-checking these should expect to re-instrument;
none of it is reproducible from the shipped tree.

---

## Files changed

Gluten worktree
(`/Users/linjaboy/Documents/velox OSS/incubator-gluten/.claude/worktrees/gluten-velox-aggregate-opt-e1be35/`):

- `backends-velox/src/main/scala/org/apache/gluten/extension/LazyAggregateExpandRule.scala`
- `backends-velox/src/main/scala/org/apache/gluten/config/VeloxConfig.scala`
- `backends-velox/src/test/scala/org/apache/gluten/execution/FusedGroupingSetAggregateSuite.scala`
- `gluten-substrait/src/main/scala/org/apache/gluten/execution/ExpandExecTransformer.scala`
- `cpp/velox/substrait/SubstraitToVeloxPlan.{h,cc}`
- `cpp/velox/compute/VeloxBackend.cc`
- `backends-clickhouse/src/main/scala/org/apache/gluten/extension/LazyAggregateExpandRule.scala`

Velox (`/Users/linjaboy/Documents/velox OSS/velox/velox/exec/`):

- `GroupingSetLattice.h`
- `GroupingSetAggregationNode.h`
- `MultiGroupingSetAggregation.{h,cpp}`
- `HashAggregation.{h,cpp}` — the standalone fix
- `CMakeLists.txt`
- `tests/CMakeLists.txt`
- `tests/MultiGroupingSetAggregationTest.cpp`
- `tests/AggregationTest.cpp` — the two regression tests for the standalone fix
- `tests/Q67RollupBenchmarkTest.cpp` — **built by no CMakeLists**
