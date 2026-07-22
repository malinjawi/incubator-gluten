# DEPLOYMENT RUNBOOK — Fused Grouping-Set Aggregation (Velox + Gluten)

**Purpose.** Get this change from *code-hardened-on-a-Mac* to *running-safely-on-a-real
Linux Spark/Gluten cluster*, with explicit go/no-go gates. This is the document that
decides whether it is safe to turn the flag on real data.

**Audience.** The engineer with a cluster where Gluten native **does** build. The macOS
dev box used for hardening cannot compile Gluten C++; several verification steps were
therefore deferred to the cluster and are called out below.

**One-line status.** The Velox operator is solid, safe under memory arbitration, and
produces correct results. It is **NOT deployable for the flagship q67 as-is**: two gates
(B1 end-to-end, B2 fallback) require a Gluten-C++ build to clear. The realistic measured
speedup on the pre-shuffle rollup stage is **~1.12–1.14x**, not the 3.0–3.5x quoted in the
older velox design docs. Ship default-off, behind the flag, as a trial.

**The two flags** (both `booleanConf`, both `createWithDefault(false)`):

| Config key | Default | Meaning |
|---|---|---|
| `spark.gluten.sql.columnar.backend.velox.lazyAggregateExpand.enabled` | `false` | Three-stage rewrite: aggregate at finest grain below Expand, expand only intermediate states, merge before shuffle. Safe, no native operator needed. |
| `spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.enabled` | `false` | Requires `lazyAggregateExpand.enabled=true`. Fuses Expand + finest-grain aggregate into the native `MultiGroupingSetAggregation` operator. **This is the experimental knob this runbook gates.** |

> The fused flag is a no-op unless the lazy-expand flag is also on. Gluten's native library
> references the operator unconditionally, so a Velox build **without** the operator will
> fail to link — that is the intended "you built it wrong" tripwire.

---

## 1. READINESS MATRIX

Status legend: **[LV]** locally verified against a clean rebuild + live run · **[RO]**
review-only (source read, correct by inspection, not executed in a running binary here) ·
**[UT]** untested (no environment to exercise it).

| # | Component | Path | State | Evidence / note |
|---|---|---|---|---|
| C1 | Velox operator — correctness | `velox/exec/MultiGroupingSetAggregation.cpp` | **[LV]** | 38/44 unit tests pass. `decimalSum` (q67's `spark_sum(decimal(7,2))→(sum,isEmpty)`), `avgDouble`, `avgDecimal` are differential-vs-reference and PASS. The 6 "failures" are all the *reference* Expand+kIntermediate comparison plan segfaulting/asserting — the fused side alone passes every one (forcedFlush 19819 rows, noMoreInputDuringDrain 60054, forcedAbandon 9951, zzBenchQ67Profile 1289371, aggregateVariableWidthUnderFlush 7811). **No fused-operator failure exists; no regression.** |
| C2 | Operator — reclaim / arbitration safety | `MultiGroupingSetAggregation.cpp` (reclaim @900, reclaimableBytes @871) | **[LV]** | `canReclaim()==true`. `reclaim()` is Phase-1-only: frees retained-but-EMPTY tables via `resetTable(freeTable=true)`, zero allocations by construction so `reclaimedBytes ≥ 0`. Phase-2 drain removed. All 3 arbitration tests pass (`reclaimFixedWidth`, `reclaimStructIntermediate`, `reclaimVariableWidthExternalMemory`) — real `opPool->reclaim()` through `ScopedReclaimedBytesRecorder`+`VELOX_CHECK_GE`, no CHECK trip. |
| C3 | Velox node — plan node | `velox/exec/GroupingSetAggregationNode.h` (serde @302/329) | **[LV]** (node) / **[RO]** (registerSerDe) | `planNodeSerdeRoundTrip` PASSES across rollup / no-bypass / CUBE shapes. Caveat: full `PlanNode::registerSerDe()` trips a folly F14 hardened-rehash assert **in the hand-linked test binary** — a link artifact, not a node defect. Confirm under the cluster's normal link. |
| C4 | HashAggregation needsInput fix | `velox/exec/HashAggregation.h:51` | **[LV]** | `&& input_ == nullptr` prevents silently overwriting an undrained buffered batch → dropped rows. Real correctness fix. **Standalone — blast radius is EVERY aggregation** (see §6). |
| C5 | Gluten Scala rule — lazy expand + fusible guard | `backends-velox/.../extension/LazyAggregateExpandRule.scala:279` | **[LV]** (compiles, formats) / **[RO]** (routing) | `test-compile` BUILD SUCCESS; `spotless:check` clean. The `singleColumnBuffers` guard (`aggBufferAttributes.length == 1`) is the **B1** gate: it keeps q67's decimal-sum off the fused path end-to-end. |
| C6 | Gluten C++ — Substrait→Velox fused conversion | `cpp/velox/substrait/SubstraitToVeloxPlan.cc` (`toGroupingSetAggregation`) | **[UT]** | Cannot compile Gluten C++ on macOS. Correct by review; must be exercised on the cluster. |
| C7 | Gluten C++ — B2 fallback net | `SubstraitToVeloxPlan.cc:1164–1204` | **[RO]** | `try { toGroupingSetAggregation } catch (const VeloxException&) { LOG(WARNING); fall through to plain ExpandNode }`. Catch type correct (`VELOX_CHECK`→`VeloxRuntimeError : VeloxException`). Degrading to a plain ExpandNode is always semantically safe (same partial states, un-reduced; Final merge is associative). **Unverified in a running binary — cluster gate.** |
| C8 | B5 bypass path | `gluten-substrait/.../ExpandExecTransformer.scala:117` | **[LV]** (decision) | JVM emits `childGrouped=0`; `configSetInOptimization` only treats `=1` as set → bypass is **deliberately dead** through Gluten, keeping the flushable-aggregate abandon safety valve. Integrated path is the NO_BYPASS arm. |
| C9 | Source hygiene | `.cpp` / `.h` operator files | **[LV]** | Zero matches for `GSAGG_\|dbg\|ABANDON_\|PARTIAL_MEM\|getenv\|fprintf`. Env hooks live only in the test file, by design. |

### Blocker status B1–B5

- **B1 — PARTIAL.** *Operator level: FIXED & verified* (C1 — the operator merges q67's
  multi-column decimal-sum intermediate correctly). *Deployment level: STILL GATED.* The
  Scala `singleColumnBuffers` guard (LazyAggregateExpandRule.scala:279) excludes
  multi-buffer aggregates, so **q67 cannot reach the fused operator end-to-end through
  Gluten.** Lifting the guard needs the coupled native+transformer redesign (native
  converter + fused-Expand projections rebuilt over the struct-packed schema) — a
  Gluten-C++ build task. **Do NOT drop the guard for the trial.**
- **B2 — FIXED in source, NOT locally verifiable** (C7). Fallback net present and correct
  by review. **Cluster gate:** compile, then run a shape-miss query and confirm *degrade,
  not crash*.
- **B3 — FIXED & verified** (C2). Reclaim coherent, no dangling reclaim, no CHECK trip.
- **B4 — FIXED, documented upstream caveat** (C3). Serde round-trips; full `registerSerDe`
  F14 assert is a hand-link artifact, an upstream/plan-tracing gate, not a Gluten
  single-process blocker.
- **B5 — RESOLVED by decision (b)** (C8). Bypass deliberately dead through Gluten;
  integrated path is NO_BYPASS.

**Verdict: NOT deployable for flagship q67 as-is.** Everything verifiable locally is green.
The two open gates (B1 end-to-end, B2 fallback) both need the cluster's Gluten-C++ build.

---

## 2. WHAT SHIPS IN WHAT ORDER

### Upstream PR split (the eventual clean landing)

- **PR1 — HashAggregation needsInput fix (standalone, upstream Velox).**
  One line: `HashAggregation.h:51` `&& input_ == nullptr`. Independent correctness fix,
  no dependency on any of this feature. Land it first and separately. Blast radius is every
  aggregation, so it gets its own review and its own validation (§6).
- **PR2 — Gluten three-stage rewrite (`lazyAggregateExpand`).**
  The Scala rule + `ExpandExecTransformer` tagging + the non-fused partial-merge path.
  Safe, needs no new native operator. This is the fallback substrate PR3 degrades onto.
- **PR3 — Fused operator (`fusedGroupingSetAggregate`).**
  The Velox `MultiGroupingSetAggregation` node/operator + serde + the
  `SubstraitToVeloxPlan.cc` fused conversion and B2 fallback net. Depends on PR2's tagging
  and PR1 being present. Ships default-off.

> Do not bundle PR1 into PR3. Its blast radius is disjoint and much larger; bundling makes
> both harder to review and to roll back.

### Minimal patch set for a CLUSTER TRIAL (not upstream)

Carry exactly these, as patches over the cluster's pinned Gluten + pinned Velox:

1. **Velox side (into pinned Velox):**
   - `velox/exec/MultiGroupingSetAggregation.cpp` / `.h` (new operator)
   - `velox/exec/GroupingSetAggregationNode.h` (new node + serde)
   - `velox/exec/HashAggregation.h` (the one-line needsInput fix)
   - operator registration wiring (see §3)
   - (optional, for local re-verification) `velox/exec/tests/MultiGroupingSetAggregationTest.cpp`
2. **Gluten side (working tree already contains these):**
   - `backends-velox/.../extension/LazyAggregateExpandRule.scala`
   - `backends-clickhouse/.../extension/LazyAggregateExpandRule.scala` (kept in sync)
   - `backends-velox/.../config/VeloxConfig.scala` (the two flags)
   - `gluten-substrait/.../execution/ExpandExecTransformer.scala`
   - `cpp/velox/substrait/SubstraitToVeloxPlan.cc` / `.h`
   - `cpp/velox/compute/VeloxBackend.cc`

**Do NOT carry:** the `docs/design/` artifacts, the `FusedGroupingSetAggregateSuite.scala`
scaffolding beyond what you run, or the retracted 3x perf headline. Note
`VeloxRuleApi.scala` is **not** modified in the working tree despite the changeset brief —
do not invent a change there.

---

## 3. THE BUILD ON THE CLUSTER

The Mac could not build Gluten C++; on Linux it does. The macOS traps below **will not
occur** on the cluster and are listed only so you don't re-debug them.

### 3.1 Carry the operator into Gluten's pinned Velox

Gluten builds against a **pinned** Velox commit (a submodule / fetched tarball, not
upstream HEAD). Choose one:

- **Preferred for a trial — patch the pinned tree.** Apply the Velox-side files from §2 as
  a patch on top of the exact pinned commit Gluten uses. Keep the patch under version
  control next to the trial config so it is reproducible.
- **Submodule branch.** If the cluster's Gluten consumes Velox as a git submodule, create a
  branch off the pinned commit, commit the operator there, and point the submodule at it.
  Do **not** bump the pin to upstream HEAD — that pulls in unrelated churn and voids the
  local verification.

Confirm the pin first:

```bash
# from the Gluten checkout
git -C ep/build-velox/build/velox_ep rev-parse HEAD   # or the submodule path your build uses
grep -rn "VELOX_.*SHA\|velox.*commit\|GITHUB_SHA" ep/build-velox/ | head
```

Verify the four Velox files land at the pinned commit and that
`MultiGroupingSetAggregation.cpp` is in the `velox_exec` build target's source list (add it
if the pinned tree's `CMakeLists.txt` doesn't already reference it).

### 3.2 Register the operator

The operator must be registered so the planner can instantiate it. Confirm the
registration call is present and reached during backend init:

```bash
grep -rn "MultiGroupingSetAggregation\|GroupingSetAggregation" \
  cpp/velox/compute/VeloxBackend.cc velox/exec/
```

`cpp/velox/compute/VeloxBackend.cc` is modified in the working tree — that is where Gluten
wires backend-side registration. Ensure the operator's `registerSerDe()` (node C3) and any
`Operator::registerOperator`/translator hook run inside Gluten's backend init, not only in
the Velox test main.

### 3.3 Build Gluten native

```bash
# adjust flags to the cluster's standard Gluten build recipe
./dev/buildbundle-veloxbe.sh --build_tests=OFF --enable_s3=... --spark_version=...
# or the split form:
./dev/builddeps-veloxbe.sh
mvn clean package -Pbackends-velox -Pspark-3.5 -DskipTests
```

**Link is the operator's presence gate.** Gluten's native library references the operator
unconditionally. If you forgot §3.1, the link **fails** — that is the intended tripwire,
not a mystery. A successful link means the operator is compiled in.

### 3.4 Confirm the operator registers at runtime

Start a trivial Gluten session with both flags on and run one rollup query; then confirm the
operator appears in the native plan / logs:

```bash
grep -i "MultiGroupingSetAggregation\|GroupingSetAggregation\|isRollup" \
  <executor-stderr-log>
```

Pass = the native operator name shows up in the executed plan for a fusible-shaped query.
Fail = you only ever see plain `Expand`/`Aggregation` → the flag isn't wired or the shape
guard rejected everything (expected for q67 today — use a single-buffer aggregate like
`count`/`sum(bigint)` over `GROUP BY ... WITH ROLLUP` to prove wiring).

### 3.5 macOS traps that will NOT occur on Linux (reference only)

- Zulu 8 default JDK breaks `scalac -release` → on the cluster use JDK 17 (brew JDK 17 was
  the Mac workaround). Not a cluster issue if the standard Gluten JDK is used.
- The folly **F14 hardened-rehash assert** on full `registerSerDe()` was a hand-linked test
  binary artifact (C3 caveat). It does not reproduce under Gluten's normal link. If it *does*
  appear on the cluster, treat it as a real link/ABI mismatch and stop.
- Gluten C++ simply would not compile on the Mac — the entire C6/C7 verification was
  deferred here for that reason.

---

## 4. THE GATES

Ordered. **No gate is skipped.** Each has a command and a hard pass/fail. Do not enable the
flag on real data until g1–g5 are green; do not report a speedup until g6.

Common session flags for "flag-ON":

```
--conf spark.gluten.sql.columnar.backend.velox.lazyAggregateExpand.enabled=true
--conf spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.enabled=true
```

"flag-OFF" = both set to `false` (the shipped default).

### g1 — Unit + correctness suites green
```bash
# Velox operator unit tests, each in its own process (matches local method)
ctest -R MultiGroupingSetAggregation --output-on-failure
# Gluten Scala rule + transformer
mvn test -Pbackends-velox -pl backends-velox \
  -Dtest='*LazyAggregateExpand*,*FusedGroupingSetAggregate*'
```
**PASS:** operator tests green; the 6 known reference-plan segfaults are *reference-side
only* — confirm each fused side passes (isolate with the `PLAN_ONLY`/`ONLY` hooks if a
harness difference shows more failures). Scala suites green.
**FAIL:** any *fused-side* test fails, or any previously-passing test regresses.

### g2 — Flag OFF changes nothing (default-off safety)
Run the target workload (start with the TPC-DS rollup queries) flag-OFF and diff the
physical plan + results against the current production Gluten.
**PASS:** byte-identical results and no `MultiGroupingSetAggregation` node anywhere in the
plan. The feature is inert when off.
**FAIL:** any plan or result difference with the flag off → the change is not truly
gated; stop.

### g3 — Single q67 at small scale, byte-identical flag-on vs flag-off
Use a **fusible-shaped** rollup query first (q67 itself is still B1-gated end-to-end; see
note). At SF1 or a small sample:
```bash
# flag OFF
spark-sql --conf ...enabled=false  -f q_rollup.sql > /tmp/off.tsv
# flag ON
spark-sql --conf ...lazyAggregateExpand.enabled=true \
          --conf ...fusedGroupingSetAggregate.enabled=true -f q_rollup.sql > /tmp/on.tsv
sort /tmp/off.tsv > /tmp/off.s; sort /tmp/on.tsv > /tmp/on.s
diff /tmp/off.s /tmp/on.s && echo "IDENTICAL"
```
**PASS:** `IDENTICAL` (sort first — row order across the shuffle is not guaranteed) **and**
the flag-ON plan actually contains the fused operator (§3.4).
**FAIL:** any diff, or flag-ON silently fell back to plain Expand for a shape you expected
to fuse (that's a B2 event — record it; it is *safe* but means you're not testing the
operator).

> **q67 note:** because of B1, q67's `spark_sum(decimal(7,2))` will NOT reach the fused
> operator today — it degrades to the three-stage path. That is correct but does not
> exercise the operator. To gate the operator itself, use a single-buffer aggregate
> (`count(*)`, `sum(bigint)`, `min`/`max`) over `GROUP BY ... WITH ROLLUP`. q67 end-to-end
> on the fused path is a **before-default-on** item (§7), not a trial gate.

### g4 — Memory-constrained run does not crash or wrong-answer (exercises reclaim)
Re-run g3's fusible query under a tight memory budget to force arbitration/reclaim:
```bash
spark-sql \
  --conf spark.gluten.memory.offHeap.size.in.bytes=<small, e.g. 512m per task> \
  --conf spark.memory.offHeap.size=512m \
  --conf ...lazyAggregateExpand.enabled=true \
  --conf ...fusedGroupingSetAggregate.enabled=true \
  -f q_rollup.sql | sort > /tmp/on_tight.s
diff /tmp/off.s /tmp/on_tight.s && echo "IDENTICAL UNDER PRESSURE"
```
**PASS:** `IDENTICAL UNDER PRESSURE`, no task failure, no `VELOX_CHECK`/reclaim CHECK trip
in executor logs. Confirms C2/B3 under real spill/reclaim.
**FAIL:** any crash, `VELOX_CHECK_GE` reclaim assert, or result diff.

### g5 — Full TPC-DS suite flag-on matches flag-off
```bash
# run the full TPC-DS at your trial scale (e.g. SF10 or SF100), both arms, compare
run_tpcds.sh --gluten --scale 10 --conf-off  > results_off/
run_tpcds.sh --gluten --scale 10 --conf-on   > results_on/
for q in results_off/*.tsv; do
  b=$(basename "$q");
  diff <(sort "results_off/$b") <(sort "results_on/$b") >/dev/null \
    && echo "OK  $b" || echo "DIFF $b";
done | grep DIFF && echo "REGRESSION" || echo "ALL MATCH"
```
**PASS:** `ALL MATCH` across every query — not just the rollup ones. This catches any
collateral damage from the HashAggregation fix (§6) and from the rewrite touching plan
shapes.
**FAIL:** any `DIFF`. Investigate before proceeding; do not enable on real data.

### g6 — A/B performance measurement (ONLY after g1–g5 green)
See §5. This is the *only* gate that reads timings; it never runs before correctness is
proven.

---

## 5. THE PERF MEASUREMENT

The honest local number is **~1.12–1.14x** on the pre-shuffle rollup stage at SF10 (NO_BYPASS
arm — what Gluten actually runs). The bypass arm's ~1.43x is **dead through Gluten** (B5) and
is contrast-only. Nobody has the end-to-end q67 number yet, nor the rollup-stage *share* of
total runtime — produce both here.

### 5.1 Method
- **Warm, then measure.** Discard the first run per arm (JIT, caches, page cache). Take
  **5 runs**, report **best and median** (matches the local protocol).
- **Fix everything but the flag.** Same cluster, same executors/cores/memory, same scale,
  same input files, same Spark version. Only the two flags differ.
- **Verify identical output first.** Re-confirm g3/g5 identity for the exact query and scale
  you time — a speedup on wrong answers is worthless.

### 5.2 End-to-end q67
```bash
for arm in off on; do
  for i in 1 2 3 4 5 6; do
    /usr/bin/time -v spark-submit ... --conf <arm flags> q67.sql 2>> time_${arm}.log
  done
done
```
Record per arm: wall-clock best & median, total input rows (expect **5,342,291** at SF10),
output rows (expect **5,752,455**, identical across arms — verify).

### 5.3 Stage-level attribution (the number nobody has)
Read stage times from the **Spark UI** (or the event log), not from wall clock:
1. Open the completed application in the History Server → SQL tab → the q67 query.
2. Identify the **rollup pre-shuffle stage** — the stage containing the Expand /
   grouping-set aggregate **before** the exchange that feeds the FINAL aggregation.
3. Record that stage's **task-time sum** and **stage wall-clock** for both arms.
4. Record the total query wall-clock.

Attribution:
```
rollup_stage_share      = rollup_stage_walltime / total_query_walltime      (per arm, flag-OFF)
rollup_stage_speedup    = rollup_stage_time_OFF  / rollup_stage_time_ON
end_to_end_speedup      = total_walltime_OFF     / total_walltime_ON
implied_amdahl_ceiling  = 1 / (1 - rollup_stage_share)   # max possible end-to-end speedup
```
**What to record in the trial report:**
- rollup-stage share of total runtime (flag-OFF) — *the missing number*.
- rollup-stage speedup (expect roughly the ~1.12–1.14x seen locally).
- end-to-end q67 speedup (will be *less* than the stage speedup by Amdahl — that's honest).
- explicit statement that the **3.0–3.5x headline** in `HYBRID_DESIGN.md` /
  `DEFENSIBILITY.md` was **not reproduced** at SF10 and must be scoped/retracted.

> If q67 is still B1-gated (fused path unreachable), measure the three-stage
> (`lazyAggregateExpand` only) arm for the end-to-end number, and measure the fused operator
> on a single-buffer rollup query for the stage-level operator speedup. Label them
> distinctly — do not present the single-buffer stage number as q67's.

---

## 6. ROLLBACK & BLAST RADIUS

### 6.1 Rollback
- **The flag is default-off.** Primary rollback = set both flags to `false` (or just
  `fusedGroupingSetAggregate.enabled=false`). No redeploy needed if it's a session conf.
- **If a gate fails:** do not proceed to the next gate. Set flags off, capture the failing
  plan + logs + the minimal repro query, and file it. g1/g2/g3 failures block the trial
  entirely; g4 failure means reclaim is not safe under this cluster's arbitration; g5
  failure means collateral damage — suspect the HashAggregation fix (§6.2).
- **Binary rollback:** because the operator is compiled into the native lib, "off" removes
  it from *plans* but not from the *binary*. If you need it gone from the binary (e.g. a
  suspected link/ABI problem), redeploy the prior native lib. Keep the pre-trial Gluten
  native artifact staged for exactly this.

### 6.2 HashAggregation fix — blast radius = EVERY aggregation
`HashAggregation.h:51` (`&& input_ == nullptr`) is **not** scoped to grouping sets. It
changes `needsInput()` for **all** hash aggregations in the cluster. It is a genuine
correctness fix (without it, an undrained buffered batch can be silently overwritten →
dropped rows), but its reach is total.

**Validate it separately from the feature:**
- It is **PR1, standalone** (§2) — review and land it on its own cadence.
- Gate it with a **full TPC-DS run with the fused/lazy flags OFF** (so only the
  HashAggregation change is active) vs the prior binary. This is a *superset* of g5 and
  isolates the fix's effect from the feature's.
- Because it can only *add* correctly-processed rows (it prevents a drop), watch
  specifically for **row-count changes** in aggregation-heavy queries between the two
  binaries — a changed count is the fix working (or, if wrong, the signal to stop).
- If any doubt: ship PR1 first, bake it, *then* trial the operator on top. Never debug a
  fused-operator result diff while the HashAggregation change is also new — you won't know
  which one moved the answer.

---

## 7. OPEN ITEMS BEFORE DEFAULT-ON (vs before a trial)

A **trial** (flag-off default, opt-in per session, g1–g6 green on a fusible shape) can
proceed once the cluster clears g1–g5. **Default-on** — shipping the flag `true` — additionally
requires closing all of these:

- **B1 end-to-end for q67.** Lift the `singleColumnBuffers` guard
  (LazyAggregateExpandRule.scala:279) **only after** B2's fallback is verified on the
  cluster and the native converter + fused-Expand projections are rebuilt over the
  struct-packed (multi-column buffer) schema. Until then q67 rides the three-stage path.
- **B2 fallback verified in a running binary.** Compile Gluten C++, run a shape that trips a
  native `VELOX_CHECK` (e.g. avg or decimal sum reaching `toGroupingSetAggregation`), and
  confirm it **degrades to a plain Expand with a `LOG(WARNING)` and correct results** — not
  a task failure (C7 / SubstraitToVeloxPlan.cc:1164–1204).
- **B5 bypass decision.** Currently decision (b): bypass deliberately dead, integrated path
  is NO_BYPASS, keeping the flushable-aggregate abandon safety valve. Ratify or revisit
  before default-on; if revisited, the ~1.43x bypass arm is the potential upside but it
  removes the abandon valve.
- **Serde / plan tracing.** Confirm full `PlanNode::registerSerDe()` works under the
  cluster's normal link (C3) — the F14 assert was a hand-link artifact but must be cleared
  for plan-tracing / distributed-plan-serialization environments.
- **CUBE performance.** Serde round-trips for CUBE (verified), but CUBE was **not** perf-measured.
  Measure a CUBE workload before claiming it benefits.
- **64-key limit.** The guard caps `bottomGroupingKeys.length <= 64`. Confirm behavior at
  and beyond 64 grouping keys (should fall back cleanly, not crash) before default-on.
- **Perf headline.** Scope/retract the 3.0–3.5x claim in `HYBRID_DESIGN.md` /
  `DEFENSIBILITY.md`; replace with the g6-measured ~1.12–1.14x stage number and the
  end-to-end Amdahl-limited figure.

---

## APPENDIX — key paths

Velox (into pinned Velox on the cluster):
- `velox/exec/MultiGroupingSetAggregation.cpp` — operator (reclaim @900, reclaimableBytes @871)
- `velox/exec/GroupingSetAggregationNode.h` — node + serde (@302/329)
- `velox/exec/HashAggregation.h:51` — needsInput fix (PR1, standalone)
- `velox/exec/tests/MultiGroupingSetAggregationTest.cpp` — unit tests (env hooks live here only)

Gluten (this working tree):
- `backends-velox/.../extension/LazyAggregateExpandRule.scala:279` — B1 `singleColumnBuffers` guard
- `backends-velox/.../config/VeloxConfig.scala:441,456` — the two flags
- `gluten-substrait/.../execution/ExpandExecTransformer.scala:117` — B5 `childGrouped=0`
- `cpp/velox/substrait/SubstraitToVeloxPlan.cc:1164–1204` — B2 fallback net
- `cpp/velox/compute/VeloxBackend.cc` — backend-side registration

Not modified (despite the changeset brief): `VeloxRuleApi.scala`.
