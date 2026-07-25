# Fused grouping-set aggregation — defensibility document

**Status:** historical readiness audit from 2026-07-22, with a current disposition updated
2026-07-25. Historical measurements remain labeled as such; current production files,
tests, and `DEPLOYMENT_RUNBOOK.md` are authoritative.
**Audience:** Apache Gluten / Velox reviewers, and engineering management.
**Date of record:** 2026-07-22. Every measurement below was taken on the tree at that date.

This document is written to be read adversarially. Section 5 lists everything known to be
wrong with the work, ranked, before any reviewer has to find it. If you find a defect that
is not in section 5, that is a reporting failure and the rest of this document should be
treated with proportionally more suspicion.

**Current disposition (2026-07-25).** The former D1 routing defect is fixed: Spark
multi-column partial buffers, including decimal sum's `(sum, isEmpty)` state, pass through
the extraction `ProjectRel`, are reconstructed as packed Velox accumulator state, and are
restored above the fused node. The q67-shaped native differential passes with bypass off
and on; standard native tests are **54/54** and the pressure selection is **61/61**.
Finest-set bypass is independently selectable and defaults off. This is enough to package
a controlled Linux q67 canary, **not** to claim production readiness. Linux must still
prove end-to-end q67 identity, pressure behavior, AQE behavior, and performance. Fallback
plan conversion is locally covered; fallback execution and metric ownership remain gates
before broad rollout. The pinned macOS Spark/JNI runtime aborts in Folly F14 on the same
query shape even with both features off, so that platform cannot clear the execution gate.

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
is ready only for the controlled Linux canary described in the deployment runbook.

**Changeset B (Gluten-local custom operator + one Velox patch).** A new plan node
`GroupingSetAggregationNode` and operator `MultiGroupingSetAggregation` that computes every
grouping set in one pass, with a derivation lattice: each grouping set is aggregated from
the smallest already-aggregated superset rather than from raw input, so `{a}` is derived
from `{a,b}` instead of re-probing the raw stream. **The operator now lives inside the
Gluten tree** (`cpp/velox/operators/plannodes/`, compiled into Gluten's `libvelox` and
registered Gluten-locally via `registerMultiGroupingSetAggregation()`), mirroring the
existing cudf custom-operator precedent — so it needs **no Velox fork**. It keeps namespace
`facebook::velox::exec` so the node type and call sites stay valid. Also in Changeset B, and
separable from it, is a two-line correctness fix
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
| C6 | The exactly-once invariant of the fused operator holds: no grouping set's table is fed while its own output iterator is live, and no node is drained twice. | The operator's drain state machine. | Audited path-by-path (section 4.2) and exercised by the native standard and pressure suites. |

### 2.2 What is explicitly NOT claimed

Read this list before quoting any number.

- **Nothing had executed inside real Gluten at the date of record.** That historical fact
  explains the original D1 miss. Since then Gluten C++ has compiled and linked locally,
  q67-shaped planning reaches native-plan construction, and native differential coverage
  is green. A valid Linux Spark/JNI q67 execution is still outstanding.
- **C1 is not an end-to-end q67 number.** It is one pipeline stage in a hand-fed C++
  harness that never goes through Gluten's `getAggRel`. End-to-end q67 speedup is unmeasured.
  There is no claim about it.
- **C1's 3.0–3.5x is the historical *bypass* arm, not an integrated q67 claim.** At the
  date of record the JVM hardcoded `childGrouped=0`, so the only comparable integrated
  design arm was `NO_BYPASS`: 5,312ms vs 6,768–7,909ms vanilla, or ~1.27–1.49x on that
  standalone stage. The implementation now emits `finestSetBypass=0|1` from an independent
  default-off setting, but that does not retroactively turn the old harness number into an
  end-to-end Gluten result. Remeasure both settings on Linux.
- **No historical performance number in this document was re-verified during the
  verification pass.** Gluten C++ now compiles locally, but C1, C2 and C3 still stand or
  fall on a controlled cluster reproduction (section 7).
- **No claim of production readiness for the fused branch.** It is off by default and
  experimental. D1 is fixed, while q67 execution and D2 cluster-verification remain gates.
- **No claim of a strict hard memory bound.** The operator enforces an
  operator-wide flush target at safe scheduling boundaries, but a full input
  batch, hash-table capacity jump, or parent drain batch can temporarily
  overshoot it (D8).
- **No claim that reclaim makes live state spillable.** It only releases
  retained allocations from empty tables. The arbitration tests prove callback
  safety and result identity, but the latest runs reclaimed zero bytes (D3).

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
raw rows at each grain. This is why rejecting the marker and taking the native fallback is
a **performance cliff, not a wrong answer** — the plan degrades to un-merged partial states
which the Final stage still merges correctly. The current deployment always compiles the
operator from Gluten's own tree; Velox does not supply it. State the fallback property
explicitly in the PR; it is the first question a reviewer asks.

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
| **gid / mask / level indexing** | Independently re-verified. Scala emits `[k1..k_{N-1}, gid, buf...]`; `shapeFusible` pins the literal slot at index `N-1`; C++ derives `gidSlot = numKeys`. Consistent, no off-by-one. `GroupingSetSpec::activeKeysMask` is indexed by child channel; the output ProjectNode reorders by `keySlotToChannel[slot]`; consistent under an arbitrary permutation, not just identity. | Structural review + `MultiGroupingSetAggregationTest`. |
| **Empty input** | `receivedInput_` is set *below* the zero-row early return, so it means "at least one row seen". This is what prevents the global (grand-total) node emitting a spurious identity row for an empty input. JVM side has a `bottomGroupingKeys.isEmpty` guard. | `LazyAggregateExpandSuite` "degenerate grouping sets". |
| **Duplicate grouping sets** | Rejected. Enforced in the node constructor and at the `buildDerivationPlan` boundary. The lattice helper is declared in `GroupingSetLattice.h` and implemented in `GroupingSetLattice.cc`; JVM mirrors the check. | `latticeRejectsDuplicateMasks`; "duplicate grouping sets keep duplicate result rows". |
| **Aggregate filter clauses (`FILTER (WHERE …)`)** | Excluded. `aggExpr.filter.isDefined` bails the rule; C++ `VELOX_CHECK`s that no measure has a filter. | "aggregate filter clause" test. |
| **DISTINCT aggregates** | Excluded by `isDistinct` guard. The `RewriteDistinctAggregates` look-alike Expand is separately rejected because its aggregate functions reference expand-created attributes. | "single count distinct" / "multiple count distinct" — but see D14, these tests are weak. |
| **Float non-associativity** | Under strict mode, float/double `sum` and every `avg` with a float/double sum buffer are excluded, mirroring `FlushableHashAggregateRule` policy. This includes `avg` over integral input because Spark accumulates it in DOUBLE. | Strict floating-point planner tests for both sum and integral average. |
| **Measure-less grouping sets** | Kept on the ordinary Expand + merge path. Velox treats a zero-aggregate `GroupingSet` as distinct aggregation, whose extraction contract this merge-only operator does not implement. Scala refuses the marker, while the converter and native node reject zero measures defensively. | Planner fallback test + `zeroAggregatesRejected`. |
| **AQE re-invocation / idempotency** | `initialInputBufferOffset == 0` guard. Every emitted merge stage carries offset ≥ 1; the fused path emits no Regular agg at all. Reasoning verified correct. | **Not tested.** See D15. |
| **Slot instability across projections** | `buildReplaceAttributeMap` used `collectFirst`, silently ignoring a second, different attribute in the same slot. **Fixed:** all non-literal entries at a slot must now be `semanticEquals`; a disagreeing slot is dropped rather than guessed. Latent, not live — no query was found that reaches it. | Not directly tested (no reaching query exists). |
| **Exactly-once drain** | `plan_.order` is popcount-descending and a parent is a strict superset, so descendants have strictly greater rank. Nested drains are pushed in that order; siblings may be parked together, but the stack remains rank-ordered and bounded by the set count. `pushDrain` rejects an already-draining node. No node is fed while its iterator is live; the final sweep visits each node once, after every ancestor. | Structural audit + native pressure tests. |
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

**D1 — Multi-column partial buffers did not reach the fused path. FIXED in source and
verified through native-plan construction plus native differential tests.**
The original audit correctly found that Spark wraps a partial aggregate in a buffer-
extraction **ProjectRel** whenever an aggregate has multiple buffer attributes. That
excluded `avg` and decimal `sum`, including q67's `sum(ss_sales_price)`, from a converter
that accepted only a direct `AggregateRel`.

The current rule no longer applies the single-column exclusion. Native conversion accepts
either a direct `AggregateRel` or the standard extraction `ProjectRel` over one. It
reconstructs each aggregate's packed Velox intermediate type, runs
`GroupingSetAggregation`, then replays the extraction project above the fused node to
restore Spark's flattened buffer schema and expected gid position. Native checks pin the
projection form, column mapping, and exact types; an unsupported tagged shape takes D2's
soft fallback.

Evidence now covers the original gap directly:

- focused Spark planning/native-plan inspection routes an eight-key, nine-level,
  nullable-decimal q67 shape to `GroupingSetAggregation` with no `Expand`;
- the native q67 differential uses Spark's decimal `(sum, isEmpty)` intermediate across
  multiple input batches and passes against Expand+kIntermediate with bypass off and on;
- the full native standard and pressure selections pass 54/54 and 61/61;
- the no-bypass q67 shape at a 4 KiB grouping-set budget matches its 1 GiB baseline and
  records repeated pressure drains across the nine-level chain;
- a focused regression abandons an internal rollup level, crosses a live
  descendant's hard cap through that pass-through level, and proves result identity.

The remaining q67 gate is a matched Linux Spark/JNI execution and result comparison. The
local macOS runtime's Folly F14 abort is not a D1 failure because the same query shape aborts
with lazy and fused both off.

**D2 — Native soft fallback. IMPLEMENTED; cluster execution remains open.**
`ExpandExecTransformer.scala` emits the marker only when `validation = false`, deliberately
(the comment says so). Native conversion now catches `VeloxException` from the fused-shape
checks and falls through to a schema-compatible plain `ExpandNode`. The downstream final
aggregation merges the same partial states, so rejection costs pre-shuffle reduction rather
than correctness. Scala keeps cheap eligibility mirrors to avoid tagging shapes native code
will reject. Metric ownership was also audited for both layouts: direct-child fallback
inserts its identity slot, while extraction-Project fallback retains the ordinary project
and Expand slots; the JVM parent does not double-own extraction metrics. Focused coverage
now retains a tagged Spark plan, lowers only the native grouping-set limit, and observes
plain Expand conversion for both direct and extraction-Project layouts. The remaining gate
executes that fixture on Linux and confirms the warning, result, and metric association.

**D3 — Reclaim callback safety. FIXED; effective reclamation is not demonstrated.**
The earlier drain-under-arbitration design was removed. `reclaimableBytes()` and `reclaim()`
now consider only retained allocations on empty, idle grouping sets, including
an empty global cycle. Reclaim never extracts live state or feeds descendants,
so it is non-allocating by construction.
`reclaimFixedWidth`, `reclaimStructIntermediate`, and
`reclaimVariableWidthExternalMemory` pause the task, invoke the operator pool reclaimer, and
compare finalized output with a non-reclaimed run. The latest standard and pressure runs
recorded callback invocations but zero reclaimed bytes in every case. Therefore these tests
prove callback safety and result identity, not that the production arbitrator can recover
memory. Proactive flushes and descendant hard caps are the implemented memory-control
mechanism; effective live-state reclaim would require a separate design.

**D4 — Fork-dependency on unmerged Velox. RESOLVED by relocating the operator into Gluten.**
Previously `VeloxBackend.cc` included `velox/exec/MultiGroupingSetAggregation.h` and
`SubstraitToVeloxPlan.cc` included `velox/exec/GroupingSetAggregationNode.h` — headers that
existed only in a Velox fork, so Gluten C++ would not compile against upstream Velox at all.
The operator now lives **inside Gluten** (`cpp/velox/operators/plannodes/`), compiled into
Gluten's `libvelox` and registered Gluten-locally, exactly like the cudf custom operator.
The two include sites now point at `operators/plannodes/…` (in-tree), and
`registerMultiGroupingSetAggregation()` is defined in the relocated Gluten sources. Gluten
C++ therefore compiles against the pinned Velox revision with only the single needsInput
patch (`ep/build-velox/src/modify_hash_aggregation_input_buffer.patch`) — no operator fork.
The retired operator patch (`0002`) is gone; see
`docs/design/velox-patches/README.md`. The unconditional registration remains (the
translator is inert unless a `GroupingSetAggregationNode` appears in the plan), so the
split-PR argument still holds for *review* scope, but the hard compile-blocker is removed.

*Related, FIXED:* `VeloxConfig.scala` documented "without it the marker is ignored and the
plan degrades," implying an optional operator supplied by Velox. The operator is now
Gluten-local, marker rejection is handled by the native fallback, and the config describes
restoration of multi-column Spark buffers rather than excluding them.

**D5 — The plan node retains an upstream namespace/construction issue.**
`GroupingSetAggregationNode` is now declared in Gluten's
`cpp/velox/operators/plannodes/GroupingSetAggregationNode.h`, but it remains in
`facebook::velox::exec` while deriving from `core::PlanNode`. That is acceptable for the
Gluten-local custom-node precedent; it would still be an objection to moving the node
upstream, where production plan nodes are `core::`-namespaced in `core/PlanNode.h`.

The header itself is now lightweight: registry-dependent implementation moved to
`GroupingSetAggregationNode.cc`, and `outputType_` is derived from the grouping expressions
and aggregate definitions rather than the source schema. The constructor still calls
`resolveIntermediateType(...)`, so deserialization and construction require a populated
aggregate registry. An upstream design should move that resolution into the translator.
The Gluten-local node already has serde and source-contract validation. A separate upstream
version would still need normal core integration such as a Builder, visitor support,
summary details, core serde registration, and user-facing checks for planner invariants.

### Confirmed, non-blocking

**D6 — The bypass lane was disabled through Gluten. FIXED as an independent,
default-off policy.**
`VeloxConfig` now exposes
`fusedGroupingSetAggregate.finestSetBypass.enabled`; the transformer emits
`finestSetBypass=0|1`, and native conversion selects the full-key grouping set when enabled.
The bottom aggregate remains flushable and may emit duplicate partial states or abandon.
That does not break correctness: the final aggregate can merge those states, and native
coverage explicitly exercises flushed and abandoned upstream input. It can change work and
memory distribution, so the first Linux canary keeps bypass off and treats bypass on as a
separate performance arm.

**D7 — Runtime stats were numerically meaningless. FIXED.**
`Operator::addRuntimeStat` accumulates into `RuntimeMetric::sum`, and `recordNodeStats` runs
per drain cycle passing **running totals**. Over k cycles the reported sum is ≈ k·total/2 —
measured 155 drains on one node in `noMoreInputDuringDrain`, inflating `gsagg.set1.inputRows`
~78x. The pinned Velox API has `addRuntimeStat`, not `setRuntimeStat`, so `.inputRows` and
`.outputRows` now add per-cycle deltas, while `.flushTimes` and `gsagg.reclaim.count` add one
per event. `.flushRowCount` / `.aggregationPct` / `.hashTableBytes` remain genuine per-cycle
samples and match upstream `HashAggregation`.

**D8 — `operatorBudgetCeilingBytes_` was not a ceiling. FIXED as an explicit
flush-target contract.** The old implementation sampled a global predicate in
`flushReason()`, armed at most one drain per input batch, and accepted another
batch immediately after that drain. Historical measurements against that
implementation were:

| ceiling | peak bytes | overshoot |
|---|---|---|
| 16 MB | 4,952,288 | 0.30x |
| 4 MB | 4,952,288 | 1.18x |
| 1 MB | 5,103,264 | 4.87x |
| 256 KB | 5,065,408 | 19.3x |
| 64 KB | 10,353,824 | 158x |
| 4 KB | 13,257,088 | 3237x |

Other tests reached 5248x (`noMoreInputDuringDrain`), 2774x
(`aggregateVariableWidthUnderFlush`), and 1345x (`forcedFlush`). The replacement
is deliberately named `operatorFlushTargetBytes_`, and
`maybeStartPressureDrain()` is re-run after every top-level pressure drain before
`needsInput()` can become true. It first honors local hard/soft limits; if only
the shared target is exceeded it drains the largest eligible table. The
remaining approximation is explicit: the shared target is sampled after input
batches and top-level drains, while every live table reached through a recursive
derivation batch is checked against its descendant hard cap. This includes a live
grandchild behind a dynamically abandoned pass-through level. Hash-table capacity
changes remain granular.
`gsagg.peakSampledFlushableBytes` and `gsagg.maxTargetOvershootBytes` expose
safe-boundary samples; they are not total operator-memory peaks and can miss a
transient cascade peak.

**D9 — The global node was outside operator pressure. FIXED.**
`flushableNodeBytes()` now includes every live grouping set. A global set is
pressure-drainable because its output is an intermediate aggregate state: emit
one row, then call Velox's `resetGlobalAggregation()` before the next input
cycle. This bounds variable-width grand-total states such as `array_agg` under
the same shared target instead of allowing them to grow until end of input.

**D10 — `shouldFreeTable()` was dead code. FIXED.**
Same chain as D3: `clear(freeTable=true)` already ran, so by the time `completeNodeDrain`
reached `resetTable(freeTable)` the table was gone and the argument could not matter. Runtime
proof: across `forcedFlush` (41 resets), `noMoreInputDuringDrain` (159),
`flushCorrectnessSmallBudget` (83), **every reset of a node with `numActive() > 0` found
`allocatedBytes() == 0`**. The "CEILING SELF-PERPETUATION" and "ZERO-ROW-BUT-STILL-FULL"
comments described states that cannot arise on the drain path. The helper and
its unreachable enum reasons are deleted; completion now explicitly frees
non-global tables and resets global state.

**D11 — Dead abandoned-state branch. FIXED.**
`abandoned = true` is written by `maybeAbandonAfterDrain`, called from
`completeNodeDrain`, which drops `groupingSet` before returning. So
`{abandoned && groupingSet != nullptr}` never survives, and `feedNode`'s 12-line branch was
unreachable behind the `groupingSet == nullptr` check. Its comment described a
`flushReason() == kAbandoned` pickup mechanism **that does not exist** — a reviewer reading
it concludes the author does not know their own state machine. Branch replaced with
`VELOX_DCHECK(!node.abandoned)` and an accurate comment. The unreachable
`kAbandoned` reason was removed; `maybeGrowBudget` still reads the live
`abandoned` flag during completion.

**D12 — Lattice preconditions were unenforced at the function boundary. FIXED.**
`buildDerivationPlan` is a public helper implemented in `GroupingSetLattice.cc`;
duplicate-mask rejection once lived only in the node constructor, and `bypass` was not
required to be a lattice root. A bypassed non-root would forward its parent's aggregated rows
verbatim tagged with its own gid — correct but silently degenerate, and one planner change
away from a large silent row blow-up. `VELOX_CHECK`s added for both. New tests:
`latticeRejectsDuplicateMasks`, `latticeRejectsBypassOfANonRoot`, `latticeRejectsEmptyMasks`,
`latticeNonAdjacentParent` (a branch `latticeCubePicksSmallestParent` structurally cannot
reach), `latticeWideMasks` (bits 61–63). All 5 pass.

**D13 — The shared-target branch drained the topologically first node, not
the largest. FIXED.** Local hard/soft pressure remains parent-first because
draining a parent also supplies its descendants. Shared-target pressure is now
a separate O(S) selection that chooses the largest eligible live grouping-set
allocation, with topological order as the deterministic tie-break.

**D14 — `maybeGrowBudget` reserved independently per node. FIXED.** Node-local
soft thresholds remain independent so tiny coarse sets are not starved, but
explicit pool reservation is now coordinated at operator scope. Before calling
`maybeReserve`, the operator subtracts the pool's existing unused reservation
and caps useful headroom at `operatorFlushTargetBytes_`. This matters because
Velox rounds every `maybeReserve` request to an 8 MiB quantum: later small
doublings now reuse that first quantum instead of pinning one quantum per node.
`gsagg.growthReservationCalls` and `gsagg.growthReservationBytes` report the
actual calls and rounded reservation deltas.

**D15 — Wasted work and a hidden side effect in the cascade. FIXED.**
The cascade now calls the side-effect-free `exceedsHardCap(child)` predicate directly, and
the top-level pressure scan carries one allocation snapshot through the pass. If
`isPartialFull()` rehashes a node, the snapshot is adjusted by that node's allocation delta.

**D16 — `prepareForReuse` never reuses anything.** The RowVector is always uniquely owned so
the reuse branch is taken, but every child is shared downstream so every child is
reallocated. Net: full reallocation plus the cost of the checks, every drain batch. Safe —
and worth stating in the PR that this refcount-driven replacement is *exactly* what makes the
zero-copy `project()` and the bypass lane memory-safe. That argument is currently only
implied.

**D17 — Test CMakeLists bin-packing shifted. FIXED.**
`MultiGroupingSetAggregationTest.cc` was inserted at position 7 of group2, making it 11
files and shifting every subsequent file. Moved to the end with a comment explaining the
positional invariant. Three `velox/exec/CMakeLists.txt` entries also moved to their correct
alphabetical slots.

**D18 — Benchmark hygiene. PARTIALLY FIXED.**
The historical external `Q67RollupBenchmarkTest.cpp` behind claims C1–C3 was compiled by
hand and remains an audit artifact, not CI evidence. The Gluten test file retains a smaller
q67-profile microbenchmark as `DISABLED_q67ProfileBenchmark`, so it no longer runs in the
default unit suite. A publishable performance claim still needs a real benchmark target
and an end-to-end Gluten measurement.

**D19 — Debug scaffolding. FIXED in the Gluten-local source.**
The external prototype contained environment-controlled deep copies, stderr dumps, and
hot-path clocks. None remains in the Gluten-local operator. Supported diagnostics are
runtime statistics; environment hooks are confined to the test file.

**D20 — Lattice cardinality documentation. FIXED by the concise split header.**
The old header confused the number of grouping sets with the 64-key mask bound. The split
header no longer makes that claim. Parent lookup now ranks candidates once and intersects
per-key 64-bit candidate bitmaps, so construction is `O(S * K + S log S)` with no pairwise
set scan.

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

**Historical measurement before this cleanup, each test in its own process:**

| | before this verification pass | after |
|---|---|---|
| `MultiGroupingSetAggregationTest` | 29 total, **23 pass, 6 fail** | 34 total, **28 pass, 6 fail** |

Those five lattice tests all passed at that point. This table is audit history, not the
current gate.

**Current authoritative result:** the isolated native standard run passes **54/54**, and
the pressure selection passes **61/61**, excluding only
`DISABLED_q67ProfileBenchmark`. That pressure run includes the formerly disabled
regressions against the patched pinned build. The q67-shaped differential covers
multi-column decimal buffers and both bypass settings. A second enabled q67-shaped
regression compares the nine-level, nullable/string-key, Spark-decimal, no-bypass path at a
4 KiB budget with its 1 GiB baseline and asserts real pressure drains plus lifetime metrics.
The suite also includes direct rejection of zero-aggregate native nodes and the
internal-abandonment/transitive-hard-cap regression. Do not carry the six historical failures
forward as present blockers.

**Two other historical counts circulating about the old tree are also wrong. Do not quote
them.**
"22 pass / 6 fail" (from an older facts sheet) and "27 tests, 26 pass, 1 fail" (from the
operator review, taken against a partially-reverted tree) are both wrong. Use 54/54 and
61/61 for the current tree.

**Remaining test gaps, ranked by risk:**
1. A matched Linux end-to-end q67 execution that proves the fused route is selected and
   byte-identical across baseline, Stage 1-only, and Stage 2. Compare the complete inner
   rollup as well as the checked-in query's limited top-100 result. Local native-plan
   inspection proves routing, but the pinned macOS Spark/JNI baseline aborts in Folly F14
   even with both flags off.
2. Executing B2 fallback and metric ownership on Linux. Local coverage now retains a tagged
   plan, lowers only the native grouping-set limit, and proves that direct and
   buffer-extraction-Project layouts both convert to plain Expand. It does not yet execute
   the rejected plan or verify runtime metric association.
3. AQE-enabled re-invocation / idempotency. Every plan-shape test that matters disables AQE.
4. `Project(Filter(Expand))` shape (expected no-op).

The output-contract test now compares each physical plan with its own analyzed output across
name, type, nullability, expression id, qualifier, metadata, and column order. It does not
compare expression ids across two separately analyzed queries, which would be invalid because
Spark allocates fresh ids per analysis.

**Historical JVM test weaknesses recorded on 2026-07-22 (re-audit before claiming they are
fixed):**
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
| ~~Add the missing `output`-equality assertion for the rule's central invariant~~ | done — exact name/type/nullability/exprId/qualifier/metadata/order coverage |
| Add an AQE-enabled idempotency test | 2h |
| PR body must state: correctness argument (4.1), the over-restrictive guards (§5), the ANSI overflow-detection note | 1h |

**Nice-to-have:** shuffle-bytes assertion on the high-cardinality test; `Project(Filter(Expand))`
no-op test.

### PR 3 — `GroupingSetAggregationNode` + `MultiGroupingSetAggregation`. **Not close (as an *upstream* Velox PR).**

**Scope note after the relocation:** the operator now ships **Gluten-local**
(`cpp/velox/operators/plannodes/`, compiled into Gluten's `libvelox`, registered via
`registerMultiGroupingSetAggregation()`), so **none of this checklist blocks the Gluten
trial** — the Gluten trial needs only the canonical HashAggregation patch on Velox plus the
in-tree operator. The list below is the *separate, later* effort of upstreaming the
node/operator into Velox proper (which would let Gluten delete its local copy). It remains
"not close" for that upstream goal; it is not on the critical path for shipping the feature
in Gluten.

**Must-fix before opening upstream (Velox PR only):**

| item | effort |
|---|---|
| Move the intermediate-type resolution into the translator; stop calling `resolveIntermediateType` from the node ctor (D5) | 1–2d |
| Move node to `core::` / `core/PlanNode.h` after removing registry-dependent construction (D5) | 4h |
| Add `Builder`, `accept(PlanNodeVisitor&)`, `addSummaryDetails` (D5) | 1d |
| Move the existing custom serde registration into core and add `PlanNodeSerdeTest` coverage (D5) | 1d |
| `VELOX_CHECK` → `VELOX_USER_CHECK` for planner-supplied invariants (D5) | 1h |
| ~~Delete `shouldFreeTable` and its impossible-state comments (D10)~~ | done |
| Move the disabled q67-profile microbenchmark into a real benchmark target or delete it (D18) | 4h |
| Document the seven disabled pressure regressions and their upstream blockers in the PR body | 2h |
| Pick one naming stem (D21) | 1h |
| ~~Replace `Set{keyIsActive, gid}` with `GroupingSetSpec{activeKeysMask, groupingId}`, deriving `activeKeys` once in the operator — removes three encodings of one fact and makes the 64-key bound structural (B9)~~ | done |
| Declare the barrier/spill capability regression in the PR body (§3.4) | 30m |

*Do not* let a reviewer talk you out of keeping `gid` as opaque planner data. That decision
is correct and well-argued.

**Must-fix before default-on (not before PR):**

| item | effort |
|---|---|
| Exercise the pressure path in the target deployment environment; require result identity, no OOM, the fused native operator, and positive `flushRowCount` and/or `gsagg.operatorTargetFlushes`. Treat reclaim count/bytes as informational until effective reclamation exists (D3) | 1d |
| ~~Replace the misleading memory ceiling with a safe-boundary flush target (D8)~~ | done |
| ~~Make global aggregate state pressure-drainable and include it in target accounting (D9)~~ | done |
| ~~Coordinate node-local growth reservations against operator-wide headroom (D14)~~ | done |
| ~~Drain the largest eligible table under shared-target pressure (D13)~~ | done |
| ~~Test the hard cap directly instead of calling `flushReason` in the cascade (D15)~~ | done |

### PR 4 — the fused branch. **Source-ready for a controlled Linux canary; blocked from
production/default-on.**

Upstream Velox does **not** need to provide or link the operator before this PR: PR 3 carries
the operator in Gluten. D1 is now fixed by seeing through the extraction `ProjectRel` and
restoring multi-column Spark buffers; D2 soft-degrades to a plain `ExpandNode`; and D6 is an
independent default-off bypass setting. Before broader rollout, a matched Linux package must
execute q67 flag-on/off, exercise D2 while checking metric ownership, survive constrained
memory, and provide repeatable performance evidence. The exact measured Stage 1 node ID is
needed for performance attribution, not to establish fused correctness.

---

## 7. HOW TO REPRODUCE EVERY NUMBER

### 7.1 Historical standalone Velox build and test

The commands below reproduce the pre-relocation audit environment. They are not the current
build route and do not mean Velox must ship the operator. The production operator is built
and tested from Gluten; use `DEPLOYMENT_RUNBOOK.md` for the current path.

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

./build/mvn -Pspark-4.0 -Pscala-2.13 -Pbackends-velox test-compile   # exit 0; scalastyle "Found 0 errors"
./build/mvn -Pspark-4.0 -Pscala-2.13 -pl backends-velox,gluten-substrait spotless:check   # exit 0
```

Gluten C++ now builds on the Apple-Silicon machine of record through
`dev/build-rollup-native.sh`; the current native results are 54/54 standard and 61/61
pressure, with q67-shaped differential coverage in both bypass modes. Focused Spark
planning reaches a nine-set `GroupingSetAggregation` native plan without `Expand`, and a
deliberate native rejection converts retained tagged plans to plain Expand for both child
layouts. Full Spark/JNI execution on this macOS pin is inconclusive because a feature-off
control reproduces the same Folly F14 assertion. Linux cluster execution, B2 fallback
execution/metric validation before broad rollout, and representative performance remain
separate gates.

### 7.4 The memory measurements in D8, D9, D10

The old table sampled `totalNodeBytes()` at every table insert and logged
`allocatedBytes()` immediately before `resetTable`. The current tree retains
the useful safe-boundary measurements as
`gsagg.peakSampledFlushableBytes` and
`gsagg.maxTargetOvershootBytes`; those include both hash-table allocations and
drainable global aggregate state. Reproducing the historical per-insert peak or
the D10 reset census still requires temporary instrumentation.

---

## Files changed

Gluten worktree
(`/Users/linjaboy/Documents/velox OSS/incubator-gluten/.claude/worktrees/gluten-velox-aggregate-opt-e1be35/`):

- `backends-velox/src/main/scala/org/apache/gluten/extension/LazyAggregateExpandRule.scala`
- `backends-velox/src/main/scala/org/apache/gluten/config/VeloxConfig.scala`
- `backends-velox/src/test/scala/org/apache/gluten/execution/FusedGroupingSetAggregateSuite.scala`
- `gluten-substrait/src/main/scala/org/apache/gluten/execution/ExpandExecTransformer.scala`
- `cpp/velox/substrait/SubstraitToVeloxPlan.{h,cc}` — includes `operators/plannodes/GroupingSetAggregationNode.h`
- `cpp/velox/compute/VeloxBackend.cc` — includes `operators/plannodes/MultiGroupingSetAggregation.h`
- `backends-clickhouse/src/main/scala/org/apache/gluten/extension/LazyAggregateExpandRule.scala`

Gluten-local operator (new — the relocated Velox operator, now compiled into Gluten `libvelox`):

- `cpp/velox/operators/plannodes/GroupingSetLattice.{h,cc}`
- `cpp/velox/operators/plannodes/GroupingSetAggregationNode.{h,cc}`
- `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.{h,cc}`
- `cpp/velox/CMakeLists.txt` — adds all three implementation files to `VELOX_SRCS`
- `cpp/velox/tests/MultiGroupingSetAggregationTest.cc` + `cpp/velox/tests/CMakeLists.txt`
- `docs/design/velox-patches/README.md` — records the HashAggregation patch kept and
  operator patch 0002 retired

Velox patch — the ONLY Velox change
(`ep/build-velox/src/modify_hash_aggregation_input_buffer.patch`, enforced by the normal
`build-velox.sh` path):

- `velox/exec/HashAggregation.{h,cpp}` — the standalone needsInput fix

Historical external-Velox audit artifacts (not part of the canonical HashAggregation patch
and not the production Gluten test location):

- `tests/MultiGroupingSetAggregationTest.cpp`
- `tests/AggregationTest.cpp` — historical regression-test location for the standalone fix
- `tests/Q67RollupBenchmarkTest.cpp` — **built by no CMakeLists**
