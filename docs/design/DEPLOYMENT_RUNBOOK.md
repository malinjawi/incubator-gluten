# DEPLOYMENT RUNBOOK — Fused Grouping-Set Aggregation (Velox + Gluten)

**Purpose.** Get this change from *code-hardened-on-a-Mac* to *running-safely-on-a-real
Linux Spark/Gluten cluster*, with explicit go/no-go gates. This is the document that
decides whether it is safe to turn the flag on real data.

**Audience.** The engineer validating locally on Apple Silicon or preparing a real
Linux Spark/Gluten cluster trial. The native operator now builds and runs on the macOS
development machine; cluster-only integration gates are called out below.

**One-line status.** The source is ready to package for a **controlled Linux q67 canary**,
not for production or default-on rollout. The q67 multi-column decimal buffer shape now
routes through the fused planner and native converter, and native differential tests pass
with the finest-set bypass both off and on. Full Spark/JNI execution of that shape is not a
valid macOS gate because the pinned stock runtime reproduces the same Folly F14 assertion
with both feature flags off. Linux q67 result identity, memory pressure, AQE behavior, and
performance remain mandatory q67-canary gates. A deliberately rejected tagged plan now
converts locally to plain Expand, but executing that fallback and checking its metric
ownership remain mandatory before arbitrary-query opt-in or default-on rollout. Ship
default-off and start the q67 canary with the bypass off.

**The feature flags and fused-shape limit:**

| Config key | Default | Meaning |
|---|---|---|
| `spark.gluten.sql.columnar.backend.velox.lazyAggregateExpand.enabled` | `false` | Three-stage rewrite: aggregate at finest grain below Expand, expand only intermediate states, merge before shuffle. Safe, no native operator needed. |
| `spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.enabled` | `false` | Requires `lazyAggregateExpand.enabled=true`. Fuses Expand + finest-grain aggregate into the native `MultiGroupingSetAggregation` operator. **This is the experimental knob this runbook gates.** |
| `spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.maxGroupingSets` | `16` | Rejects larger fused shapes before per-set state allocation; valid range is 1–64. The 64-set ceiling also keeps lattice parent lookup in one candidate bitmap. Multi-root shapes retain the ordinary merge path. |
| `spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.finestSetBypass.enabled` | `false` | Optionally forwards the finest grouping set from the flushable child partial aggregate. Correct even if that child flushes or abandons because duplicate partial states remain mergeable by the final aggregate, but its workload trade-off is unmeasured. Keep it off for the first canary. |

> The fused flag is a no-op unless the lazy-expand flag is also on. The operator is
> Gluten-local; a Velox build is not expected to provide or link it. Gluten's native target
> compiles and registers the in-tree operator unconditionally. A partial Gluten changeset
> that omits its source/CMake/registration wiring will fail to link.

---

## 1. READINESS MATRIX

Status legend: **[LV]** locally verified against a clean rebuild and the applicable native
or planning test · **[RO]** review-only (source read, correct by inspection, not executed
in the target runtime here) · **[UT]** untested (no valid environment to exercise it).

| # | Component | Path | State | Evidence / note |
|---|---|---|---|---|
| C1 | Gluten-local operator — correctness | `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.cc` | **[LV]** | Clean isolated native build: **54/54** standard tests passed. The pressure command passed **61/61** selected tests, excluding only the disabled timing benchmark. The q67-shaped mixed eight-key, nine-level, nullable-decimal, multi-batch differential passes against Expand+kIntermediate with bypass both off and on. The same shape also matches a 1 GiB fused baseline at a 4 KiB budget, with bypass off and positive pressure drains. An internal-abandonment regression proves that descendant hard caps remain enforced through a pass-through level. |
| C2 | Operator — reclaim / arbitration safety | `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.cc` | **[LV]** | `reclaim()` never materializes live state and can free only retained-but-empty allocations. All three arbitration-callback tests passed (`reclaimFixedWidth`, `reclaimStructIntermediate`, `reclaimVariableWidthExternalMemory`), but the recorded run reclaimed **0 bytes** because completed drains already free their tables. The tests establish callback safety, not spillability or effective live-state reclaim. |
| C3 | Gluten-local node — plan node | `cpp/velox/operators/plannodes/GroupingSetAggregationNode.{h,cc}` | **[LV]** (node) / **[RO]** (cluster registration) | `planNodeSerdeRoundTrip` passed across rollup / no-bypass / CUBE shapes. Confirm registration under the cluster's packaged native-library load path. |
| C4 | HashAggregation needsInput fix | `velox/exec/HashAggregation.h:51` | **[LV]** | `&& input_ == nullptr` prevents silently overwriting an undrained buffered batch → dropped rows. Real correctness fix. **Standalone — blast radius is EVERY aggregation** (see §6). |
| C5 | Gluten Scala rule — lazy expand + fusible guard | `backends-velox/.../extension/LazyAggregateExpandRule.scala` | **[LV]** (planning) | The fused eligibility path accepts Spark aggregates whose accumulator is represented by multiple flattened buffer columns. Focused planning coverage asserts the q67-shaped decimal rollup is tagged for fusion and retains all nine grouping sets. |
| C6 | Gluten C++ — Substrait→Velox fused conversion | `cpp/velox/substrait/SubstraitToVeloxPlan.cc` (`toGroupingSetAggregation`) | **[LV]** (compile/link + native-plan conversion) / **[RO]** (Linux execution) | The converter accepts either a direct `AggregateRel` or Spark's buffer-extraction `ProjectRel` over an `AggregateRel`, reconstructs the packed Velox accumulator state, and replays extraction above the fused node. Local native-plan inspection for the q67 shape contains `GroupingSetAggregation`, no `Expand`, and nine sets. |
| C7 | Gluten C++ — B2 fallback net and metric ownership | `SubstraitToVeloxPlan.cc` | **[LV]** (native-plan conversion) / **[RO]** (Linux execution + metrics) | A focused test retains a tagged plan, lowers only the native grouping-set limit, and proves that both direct-aggregate and buffer-extraction-Project shapes convert to schema-compatible plain `ExpandNode` plans with no `GroupingSetAggregation`. Direct-child fallback adds the identity metric slot and extraction-Project fallback retains its normal slots. **The deliberately rejected plan must still execute on Linux and prove correct results plus stable metric association before broad rollout.** |
| C8 | B5 finest-set bypass | `ExpandExecTransformer.scala` and `VeloxConfig.scala` | **[LV]** (selection + native correctness) | `finestSetBypass.enabled` is independently selectable and defaults to `false`. Both q67-shaped native differential arms pass, including flushed and abandoned upstream states. Its performance trade-off is not yet measured, so the first canary uses `false`. |
| C9 | Source hygiene | `.cpp` / `.h` operator files | **[LV]** | Zero matches for `GSAGG_\|dbg\|ABANDON_\|PARTIAL_MEM\|getenv\|fprintf`. Env hooks live only in the test file, by design. |
| C10 | Integrated Spark/JNI execution on macOS | pinned local build | **[UT]** (platform-invalid) | The q67-shaped execution aborts in Folly `F14Table::rehashImpl`; the same assertion reproduces with lazy and fused both disabled, so it is not evidence against Stage 2. Native-plan construction remains usable locally. Execute the correctness gate on a matched Linux package. |

### Blocker status B1–B5

- **B1 — FIXED in source and locally verified below Spark execution.** The planner accepts
  multi-column partial buffers, the native converter sees through the extraction
  `ProjectRel`, and the q67-shaped native differential passes in both bypass modes. A
  matched Linux Spark/Gluten q67 run is still required; that is a deployment gate, not the
  old routing defect.
- **B2 — Native-plan fallback locally verified; rejected-plan execution remains open** (C7).
  Both supported child layouts convert to plain Expand after a deliberate native rejection.
  Before arbitrary-query opt-in or default-on, execute that retained tagged plan on Linux and
  confirm *degrade, not crash*, correct output, and correctly associated operator metrics.
- **B3 — callback safety FIXED & locally verified** (C2). There is no dangling state and no
  reclaim CHECK trip. Live aggregate state is deliberately not reclaimable; proactive
  pressure drains, not arbitration-time materialization, are the memory-control mechanism.
- **B4 — FIXED at node level; packaged Linux registration remains a gate** (C3). Serde
  round-trips locally. The currently observed macOS F14 failure also occurs in a feature-off
  baseline and cannot be used as a fused-operator verdict.
- **B5 — FIXED and selectable, default off** (C8). Correctness covers both settings; start
  q67 with bypass off until cluster performance and memory evidence justify enabling it.

**Verdict: ready to package for a controlled Linux q67 canary; not production-ready.**
Native correctness and q67 routing/conversion are green. Linux must still clear q67 result
identity, pressure behavior, exact-profile compatibility, AQE behavior, and performance.
Fallback execution and metric ownership are additional gates before arbitrary-query or
default-on rollout.

---

## 2. WHAT SHIPS IN WHAT ORDER

### Upstream PR split (the eventual clean landing)

- **PR1 — HashAggregation buffered-input fix (standalone, upstream Velox).**
  Adds `&& input_ == nullptr` to `needsInput()` and asserts the same precondition in
  `addInput()`. It is an independent correctness fix with no dependency on this feature.
  Land it first and separately. Its blast radius is every aggregation, so it gets its own
  review and validation (§6).
- **PR2 — Gluten three-stage rewrite (`lazyAggregateExpand`).**
  The Scala rule + `ExpandExecTransformer` tagging + the non-fused partial-merge path.
  Safe, needs no new native operator. This is the fallback substrate PR3 degrades onto.
- **PR3 — Fused operator (`fusedGroupingSetAggregate`).**
  The Gluten-local `MultiGroupingSetAggregation` node/operator + serde (now living in
  `cpp/velox/operators/plannodes/`, compiled into Gluten's `libvelox`) + the
  `SubstraitToVeloxPlan.cc` fused conversion and B2 fallback net. Depends on PR2's tagging
  and PR1 being present. Ships default-off. **No Velox fork needed for the operator** — it
  is a Gluten-local custom operator (same pattern as the existing cudf operator).

> Do not bundle PR1 into PR3. Its blast radius is disjoint and much larger; bundling makes
> both harder to review and to roll back.

### Patch set for a CLUSTER TRIAL (not upstream)

Carry the production sources below together, as patches over the cluster's pinned Gluten +
pinned Velox. The validation-only sources are listed separately so packaging does not depend
on guesswork:

1. **Velox side (into pinned Velox) — ONE patch only:**
   - `ep/build-velox/src/modify_hash_aggregation_input_buffer.patch`
     (`velox/exec/HashAggregation.{cpp,h}` — the buffered-input contract fix). This is the
     **only** patch applied to Velox. `ep/build-velox/src/build-velox.sh` applies and
     verifies it for the normal bundle build, and refuses to compile if the fix is absent
     and the patch no longer applies. The operator no longer travels as a Velox patch
     (retired `0002`).
2. **Gluten side (working tree already contains these):**
   - `cpp/velox/operators/plannodes/GroupingSetLattice.{h,cc}`
   - `cpp/velox/operators/plannodes/GroupingSetAggregationNode.{h,cc}` (node + serde)
   - `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.h` / `.cc` (operator)
   - `cpp/velox/CMakeLists.txt` (adds all three implementation files to `VELOX_SRCS`)
   - `backends-velox/.../extension/LazyAggregateExpandRule.scala`
   - `backends-velox/.../extension/FlushableHashAggregateRule.scala` (Stage 1 strict
     floating-point eligibility uses the aggregate's actual accumulator type)
   - `backends-clickhouse/.../extension/LazyAggregateExpandRule.scala` (kept in sync)
   - `backends-velox/.../config/VeloxConfig.scala` (feature flags, grouping-set limit,
     and finest-set bypass)
   - `backends-velox/.../execution/HashAggregateExecTransformer.scala` and
     `gluten-substrait/.../substrait/SubstraitContext.scala` (transfer ownership of the
     buffer-extraction metric slot to the fused parent without changing the Substrait shape)
   - `backends-velox/.../backendsapi/velox/VeloxMetricsApi.scala` and
     `backends-velox/.../metrics/ExpandMetricsUpdater.scala` (surface pressure-flush and
     adaptive-abandonment rows on the fused Spark metric slot)
   - `gluten-substrait/.../execution/ExpandExecTransformer.scala`
   - `cpp/velox/config/VeloxConfig.h` (native `maxGroupingSets` key and default)
   - `cpp/velox/substrait/SubstraitToVeloxPlan.cc` / `.h` (includes the new
     `operators/plannodes/GroupingSetAggregationNode.h`)
   - `cpp/velox/compute/VeloxBackend.cc` (includes the new
     `operators/plannodes/MultiGroupingSetAggregation.h`; calls
     `registerMultiGroupingSetAggregation()`)
   - `ep/build-velox/src/build-velox.sh` (idempotently applies and exactly verifies the
     required HashAggregation patch during the ordinary bundle build)

3. **Validation sources that travel with the reviewable change:**
   - `cpp/velox/tests/MultiGroupingSetAggregationTest.cc` + `cpp/velox/tests/CMakeLists.txt`
     (operator unit test, `add_velox_static_test(velox_rollup_aggregation_test ...)`)
   - `backends-velox/src/test/scala/org/apache/gluten/execution/FusedGroupingSetAggregateSuite.scala`
   - `backends-velox/src/test/scala/org/apache/gluten/execution/VeloxAggregateFunctionsSuite.scala`
     (Stage 1 strict-average execution regression; run it in the matched Linux package)
   - `gluten-substrait/src/test/scala/org/apache/gluten/execution/WholeStageTransformerSuite.scala`
     (resolves percent-encoded workspace paths)
   - `cpp/CMakeLists.txt` (a test-only build no longer requires Google Benchmark unless
     benchmarks are actually enabled)
   - `dev/build-rollup-native.sh` (repeatable local dependency, build, native, pressure, and
     focused Scala validation; not part of the runtime package)

**Do NOT carry into the runtime package:** the `docs/design/` reference artifacts, local
build outputs, or the retracted 3x perf headline. Keep the regression tests in the source
change even though they are not packaged into the runtime jar. `VeloxRuleApi.scala` is
**not** modified in the working tree despite the changeset brief — do not invent a change
there.

---

## 3. NATIVE VALIDATION AND THE BUILD ON THE CLUSTER

### 3.0 Repeatable Apple-Silicon validation

Use the checked-in helper rather than reconstructing compiler and dependency paths in the
shell:

```bash
# One-time host prerequisites on a fresh Apple-Silicon machine.
brew install python@3.11 ninja llvm@15 bison flex m4
brew install --cask zulu@8

# Read-only environment report. It is also useful after a Homebrew/Xcode change.
./dev/build-rollup-native.sh doctor

# First run: fetch the exact Velox commit and build pinned dependencies.
# This is intentionally limited to two parallel jobs by default on a 16 GiB machine.
./dev/build-rollup-native.sh setup

# Build pinned Velox, Gluten's native library, and the rollup test executable.
./dev/build-rollup-native.sh build

# Both commands perform an incremental rebuild before running, so they cannot
# accidentally validate a stale test executable.
./dev/build-rollup-native.sh test
./dev/build-rollup-native.sh test-pressure

# Clean and build the Spark 4 / Scala 2.13 reactor, then run the focused
# fused suite, including Stage 1 strict-average plan routing. The clean makes
# this safe after another Spark/Scala profile has populated Maven targets.
./dev/build-rollup-native.sh test-scala
```

`setup` pins Velox commit `053db35254cda24af0b69b8a6693d1b295b4fba5`,
CMake 3.31.1, Arrow 15.0.0, and the dependency versions in that Velox checkout. It strips
Conda and ambient CMake/package paths inside the helper process. The repository path
contains spaces, so the helper creates a stable no-space symlink and keeps the persistent
state, manifest, and timestamped logs below:

```text
ep/build-velox/build/rollup-native/dft-2026_06_05-cmake-3.31.1/
```

`setup` can install missing Homebrew formulae globally; downloaded source and compiled
artifacts otherwise stay in that persistent state directory. Use
`./dev/build-rollup-native.sh env` when an IDE needs the exact include and library paths.
Use `rerun` as a short alias for an incremental test rebuild plus the enabled suite.
For clangd or the VS Code C/C++ extension, point its compilation-database setting at:

```bash
eval "$(./dev/build-rollup-native.sh env)"
echo "$GLUTEN_BUILD_PATH/compile_commands.json"
```

That database includes the private dependency prefix containing `fmt/format.h`; installing
an unrelated global `fmt` copy is neither necessary nor desirable.

Recorded native result for this tree:

- enabled native suite: **54/54 passed**;
- fused pressure suite: **61/61 passed**, including the pressure regressions and
  excluding only `DISABLED_q67ProfileBenchmark`;
- the native q67-shaped differential covers eight mixed-type keys, nine rollup levels,
  multiple batches, a nullable decimal Spark `(sum, isEmpty)` intermediate, and bypass off
  and on; a second enabled regression proves the same no-bypass shape at a 4 KiB budget
  matches its 1 GiB baseline and emits pressure rows;
- Spark planning and native-plan conversion route that multi-column shape to
  `GroupingSetAggregation` with no `Expand`;
- deliberate native rejection of retained tagged direct and extraction-Project plans
  converts both to plain Expand;
- focused Spark 4 / Scala 2.13 suite: **14 passed, 2 macOS execution gates canceled**.
  This includes strict-average Stage 1 routing and exact analyzed-to-physical output
  attribute preservation;
- the full dependent reactor, including all 89 `backends-velox` test sources, also
  **test-compiles with Spark 3.5 / Scala 2.12 / JDK 17**;
- production operator and Substrait conversion sources: compiled; Gluten
  `libvelox.dylib`: linked.

The q67-shaped Spark/JNI execution arm is intentionally not counted as a macOS correctness
gate. It reaches native-plan construction, but execution hits Folly's
`F14Table::rehashImpl` assertion; the same assertion is reproducible with both feature flags
off in the pinned stock runtime. Run that arm from the matched Linux package before the
canary is admitted.

### 3.1 The operator is Gluten-local — only the HashAggregation patch touches Velox

The operator now lives **inside the Gluten tree** (`cpp/velox/operators/plannodes/`), compiled
into Gluten's own `libvelox` via `cpp/velox/CMakeLists.txt` — exactly like the existing
cudf custom operator (`cpp/velox/operators/plannodes/CudfVectorStream.*`). It keeps
namespace `facebook::velox::exec`, so the node type and the
`registerMultiGroupingSetAggregation()` call site stay valid without any Velox change.

The **only** patch applied to Gluten's pinned Velox is the needsInput fix. Do not apply it
manually in the normal workflow: the ordinary Velox build now applies it idempotently and
fails closed if it cannot verify the contract.

```bash
# The one-shot bundle path invokes build-velox.sh, which enforces the patch.
PROMPT_ALWAYS_RESPOND=y ./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON \
  --enable_s3=OFF --enable_gcs=OFF --enable_hdfs=OFF --enable_abfs=OFF

# Hard artifact-source verification before deployment. Match executable lines,
# not comments that happen to mention the invariant.
grep -Eq '^[[:space:]]*return !noMoreInput_ && !partialFull_ && input_ == nullptr;[[:space:]]*$' \
  ep/build-velox/build/velox_ep/velox/exec/HashAggregation.h
grep -Eq '^[[:space:]]*VELOX_CHECK_NULL\(input_\);[[:space:]]*$' \
  ep/build-velox/build/velox_ep/velox/exec/HashAggregation.cpp
```

Do **not** bump the pin to upstream HEAD — that pulls in unrelated churn and voids the
local verification. There is no operator patch to apply: `0002` has been retired (see
`docs/design/velox-patches/README.md`).

### 3.2 Register the operator

Registration is Gluten-local. `cpp/velox/compute/VeloxBackend.cc` calls
`velox::exec::registerMultiGroupingSetAggregation()` during backend init (it includes
`operators/plannodes/MultiGroupingSetAggregation.h`). Confirm both the include and the call
resolve after the relocation:

```bash
grep -rn "MultiGroupingSetAggregation\|GroupingSetAggregation" \
  cpp/velox/compute/VeloxBackend.cc \
  cpp/velox/substrait/SubstraitToVeloxPlan.cc \
  cpp/velox/operators/plannodes/
```

Ensure the operator's `registerSerDe()` (node C3) and the
`registerMultiGroupingSetAggregation()` translator/serde hook run inside Gluten's backend
init, not only in the operator test main.

### 3.3 Build Gluten native

```bash
# One-shot native backend + bundled jar build; adjust flags to the cluster recipe.
./dev/buildbundle-veloxbe.sh --build_tests=OFF --enable_s3=... --spark_version=...

# For a split/incremental build, build native first, then use Gluten's Maven wrapper.
./dev/builddeps-veloxbe.sh build_gluten_cpp
./build/mvn clean package -Pbackends-velox -Pspark-3.5 -Pscala-2.12 -DskipTests
```

Record the cluster's exact Spark, Scala, JDK, Gluten revision, Velox pin, compiler, and C++
ABI in the canary manifest. The local focused JVM result is Spark 4.0 / Scala 2.13 / JDK 17;
it does not substitute for compiling and running the package's actual profile.

**The Gluten native link is the operator-wiring gate.** Gluten's native library references
the Gluten-local operator unconditionally. If the operator source, CMake entry, or
registration wiring was omitted while transferring the Gluten changes, the link fails. This
does not require an operator-bearing Velox build; apart from
`ep/build-velox/src/modify_hash_aggregation_input_buffer.patch`, use the pinned Velox tree.
A successful link confirms that the symbols are present; §3.4 confirms runtime registration
and plan selection.

### 3.4 Confirm the operator registers at runtime

Start a trivial Gluten session with both flags on and run one rollup query; then confirm the
operator appears in the native plan / logs:

```bash
grep -i "MultiGroupingSetAggregation\|GroupingSetAggregation\|isRollup" \
  <executor-stderr-log>
```

Pass = the native operator name shows up in the executed plan for the q67-shaped query.
Fail = you only ever see plain `Expand`/`Aggregation` → the flag is not wired or native
validation rejected the shape. A deliberate rejection belongs in the B2 fallback test;
q67 itself is expected to fuse.

### 3.5 macOS constraints handled by the local helper

- The native helper intentionally selects Java 8 for the pinned Velox/JNI discovery. Do
  not reuse that shell environment for a Scala profile that requires JDK 17; follow
  Gluten's Spark/JDK build matrix for the Maven step.
- The pinned macOS Spark/JNI runtime aborts in Folly's **F14 hardened-rehash assertion**
  during the q67-shaped execution. A stock control with lazy and fused both disabled hits
  the same assertion, so do not attribute it to this operator. This makes macOS execution
  inconclusive, not green. If a matched Linux package reproduces it, stop and treat it as a
  real link/ABI/runtime defect.
- Do not mix a static gflags copy with shared glog/Folly. The helper validates that the
  rollup test loads shared gflags and shared glog.
- Do not link the rollup test to both the monolithic Velox archive and Gluten's shared
  `libvelox.dylib`; that creates two global type/function registries. The dedicated static
  test target and `doctor` linkage check prevent this.

---

## 4. THE GATES

Ordered. **No gate is skipped.** Each has a command and a hard pass/fail. Do not enable the
flag on real data until g1–g5 are green; do not report a speedup until g6.

Common session flags for the first, AQE-off "flag-ON" canary:

```
--conf spark.sql.adaptive.enabled=false
--conf spark.sql.ansi.enabled=false
--conf spark.gluten.sql.columnar.backend.velox.lazyAggregateExpand.enabled=true
--conf spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.enabled=true
--conf spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.maxGroupingSets=16
--conf spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.finestSetBypass.enabled=false
```

"flag-OFF" = both feature flags set to `false` (the shipped default). Pin and record ANSI
mode in every arm. `ansi.enabled=false` matches the focused Spark 4 validation; Spark 3.5
normally already defaults to false.

### g1 — Unit + correctness suites green
```bash
# Repeatable local target build + enabled differential tests.
./dev/build-rollup-native.sh test

# Rebuild and run the seven disabled fused-side pressure regressions too.
./dev/build-rollup-native.sh test-pressure

# Focused Spark 4 / Scala 2.13 rule and transformer coverage.
./dev/build-rollup-native.sh test-scala

# Equivalent direct invocation in a cluster build directory.
cpp/build/velox/tests/velox_rollup_aggregation_test \
  --gtest_also_run_disabled_tests \
  --gtest_filter='*-*.DISABLED_q67ProfileBenchmark'
# Gluten Scala rule + transformer. Replace the Spark/Scala/JDK profiles with
# the exact cluster package profile; this example is Spark 3.5 / Scala 2.12.
./build/mvn test -pl backends-velox \
  -Pjava-17 -Pbackends-velox -Pspark-3.5 -Pscala-2.12 \
  -DwildcardSuites='*LazyAggregateExpand*,*FusedGroupingSetAggregate*'

# Run the focused Stage 1 execution regression under the package's normal JNI
# library settings (same profiles as above).
./build/mvn -pl backends-velox \
  -Pjava-17 -Pbackends-velox -Pspark-3.5 -Pscala-2.12 \
  org.scalatest:scalatest-maven-plugin:2.2.0:test \
  -Dsuites='org.apache.gluten.execution.VeloxAggregateFunctionsFlushSuite @flushable aggregate rule - floating sum state when floatingPointMode is strict'
```
**PASS:** the standard and pressure native suites, focused Scala/planner suite, and
q67-shaped native differential are green in the packaged revision.
**FAIL:** any *fused-side* test fails, or any previously-passing test regresses.

### g2 — Flag OFF changes nothing (default-off safety)
Run the target workload (start with the TPC-DS rollup queries) flag-OFF and diff the
physical plan + results against the current production Gluten.
**PASS:** no `MultiGroupingSetAggregation` node anywhere in the plan and either
byte-identical results or a result delta reproduced by a separate candidate containing only
the HashAggregation buffered-input fix (§6.2), reviewed as recovery of rows the old binary
dropped. The lazy/fused feature is inert when both flags are off; the required Velox
correctness fix is intentionally not flag-scoped.
**FAIL:** the fused node appears with the flag off, the physical plan changes because of the
rewrite, or a result difference cannot be isolated to and justified by the standalone
HashAggregation fix.

### g3 — q67 at small scale: baseline == Stage 1 == Stage 2
At SF1 or a small sample:
```bash
# Baseline: lazy=false, fused=false.
spark-sql --conf spark.sql.adaptive.enabled=false \
          --conf spark.sql.ansi.enabled=false \
          --conf ...lazyAggregateExpand.enabled=false \
          --conf ...fusedGroupingSetAggregate.enabled=false \
          -f q67.sql > /tmp/baseline.tsv
# Stage 1 only: lazy=true, fused=false.
spark-sql --conf spark.sql.adaptive.enabled=false \
          --conf spark.sql.ansi.enabled=false \
          --conf ...lazyAggregateExpand.enabled=true \
          --conf ...fusedGroupingSetAggregate.enabled=false \
          -f q67.sql > /tmp/stage1.tsv
# Stage 2: lazy=true, fused=true.
spark-sql --conf spark.sql.adaptive.enabled=false \
          --conf spark.sql.ansi.enabled=false \
          --conf ...lazyAggregateExpand.enabled=true \
          --conf ...fusedGroupingSetAggregate.enabled=true \
          --conf ...fusedGroupingSetAggregate.maxGroupingSets=16 \
          --conf ...fusedGroupingSetAggregate.finestSetBypass.enabled=false \
          -f q67.sql > /tmp/stage2.tsv
for arm in baseline stage1 stage2; do sort "/tmp/$arm.tsv" > "/tmp/$arm.sorted"; done
diff /tmp/baseline.sorted /tmp/stage1.sorted &&
diff /tmp/baseline.sorted /tmp/stage2.sorted &&
echo "ALL THREE IDENTICAL"
```

The checked-in q67 ends in `LIMIT 100`; that comparison alone can miss a wrong rollup group
that does not reach the top 100. Also run the inner `dw1` query — the complete eight-key
`GROUP BY ROLLUP(...)` result before rank, order, and limit — for all three arms and write
typed output (prefer Parquet). Compare full multisets in both directions:

```scala
def sameMultiset(left: DataFrame, right: DataFrame): Boolean =
  left.exceptAll(right).limit(1).count() == 0 &&
    right.exceptAll(left).limit(1).count() == 0

assert(sameMultiset(baselineDw1, stage1Dw1))
assert(sameMultiset(baselineDw1, stage2Dw1))
```

**PASS:** `ALL THREE IDENTICAL`, both full-inner-result `exceptAll` checks are empty, and the
Stage 2 executed plan actually contains the fused operator (§3.4). The three arms isolate a
Stage 1 error from a Stage 2 error.

**FAIL:** any diff, or Stage 2 silently fell back to plain Expand. That fallback is
semantically safe, but it means this run did not exercise q67 through the operator.

> The macOS F14 baseline does not waive this gate. g3 must run on Linux with the exact
> native library and jar intended for the canary.

### g3b — Repeat q67 with AQE enabled

The focused fused suite deliberately disables AQE so it can inspect the physical and native
plans before an action. After g3 passes, repeat both arms with
`--conf spark.sql.adaptive.enabled=true`, force the action to complete, and inspect the
final executed plan rather than the pre-AQE plan.

**PASS:** sorted results are identical, and the final flag-ON native plan contains
`GroupingSetAggregation` with no `Expand` in that stage.

**FAIL:** any result difference, loss of fusion in the final plan, duplicate rewrite, metric
misassociation, or task failure. If the intended trial environment disables AQE, record
g3b as a default-on gate rather than silently treating the AQE-off result as coverage.

### g4 — Memory-constrained run does not crash or wrong-answer (exercises pressure)
Re-run g3's fusible query under a tight memory budget to force grouping-set pressure drains
and, depending on allocator state, memory arbitration:
```bash
# Example only: four executor task slots × 512 MiB target per task = 2 GiB
# executor-total. Reduce or scale this value until the grouping-set pressure counters move.
spark-sql \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.ansi.enabled=false \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.gluten.memory.dynamic.offHeap.sizing.enabled=false \
  --conf spark.memory.offHeap.size=2g \
  --conf spark.gluten.sql.columnar.backend.velox.showTaskMetricsWhenFinished=true \
  --conf spark.gluten.sql.columnar.backend.velox.taskMetricsToEventLog.threshold=0 \
  --conf ...lazyAggregateExpand.enabled=true \
  --conf ...fusedGroupingSetAggregate.enabled=true \
  --conf ...fusedGroupingSetAggregate.finestSetBypass.enabled=false \
  -f q67.sql | sort > /tmp/stage2_tight.sorted
diff /tmp/baseline.sorted /tmp/stage2_tight.sorted && echo "IDENTICAL UNDER PRESSURE"
```
**PASS:** `IDENTICAL UNDER PRESSURE`, no task failure, OOM, or `VELOX_CHECK` trip in
executor logs, the executed native plan still contains `GroupingSetAggregation`, and the
complete inner-rollup multiset also matches the baseline using g3's bidirectional `exceptAll`.
The captured stats must show a positive pressure signal: `flushRowCount > 0` and/or
`gsagg.operatorTargetFlushes > 0`. `flushRowCount` counts rows emitted by
soft/hard/operator pressure flushes (not the final drain), and
`abandonedPartialAggregationRows` counts grouping-set state rows forwarded after adaptive
abandonment (not planned finest-set bypass); one source contribution can therefore be
counted at multiple abandoned levels. Record both. `showTaskMetricsWhenFinished` makes the
custom `gsagg.*` stats available in executor logs independently of Spark event-log
retention; `taskMetricsToEventLog.threshold=0` also records them when Spark event logging is
enabled. Inspect the executor's `Native Plan with stats` block or parse the per-task
`velox task stats` JSON. Record `gsagg.reclaim.count` and `gsagg.reclaim.bytes` if present,
but do not require either to be positive: the local arbitration tests entered the callback
and correctly reported zero freed bytes because normal drain completion had already freed
the empty tables. `spark.memory.offHeap.size` is an executor-total setting and Gluten
divides it by the executor's task-slot count; do not set the derived internal
`spark.gluten.memory.offHeap.size.in.bytes` directly.
**FAIL:** any crash, OOM, result diff, or absence of a pressure signal after the budget was
reduced enough to intend one.

### g5 — Full TPC-DS suite flag-on matches flag-off
```bash
# Keep the candidate jar/native library fixed for the feature comparison.
run_tpcds.sh --gluten-candidate --scale 10 --conf-off > results_candidate_off/
run_tpcds.sh --gluten-candidate --scale 10 --conf-on  > results_candidate_on/
for q in results_candidate_off/*.tsv; do
  b=$(basename "$q");
  diff <(sort "results_candidate_off/$b") <(sort "results_candidate_on/$b") >/dev/null \
    && echo "OK  $b" || echo "DIFF $b";
done | grep DIFF && echo "REGRESSION" || echo "ALL MATCH"

# Separately compare the flag-off candidate with the prior binary to isolate the
# global HashAggregation fix. Use the same data, Spark conf, and result normalizer.
run_tpcds.sh --gluten-prior --scale 10 --conf-off > results_prior_off/
# Apply the same per-query sorted comparison to prior_off vs candidate_off.
```
**PASS:** `ALL MATCH` across candidate flag-off/flag-on for every query — not just the
rollup ones. The prior-binary/candidate-off comparison also matches, or every delta is
reproduced with a C4-only binary and justified as recovery of previously dropped rows
(§6.2).
**FAIL:** any unexplained `DIFF`. Investigate before proceeding; do not enable on real
data.

### g6 — A/B performance measurement (ONLY after g1–g5 green)
See §5. This is the *only* gate that reads timings; it never runs before correctness is
proven.

---

## 5. THE PERF MEASUREMENT

The old standalone stage measurements are historical evidence, not a deployment
expectation. The integrated bypass is now selectable, but neither bypass setting has a
matched Linux end-to-end q67 number, and nobody has yet measured the rollup-stage share of
total q67 runtime. Start the correctness canary with bypass off; measure bypass on as a
separate arm only after the off arm is green.

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
Record per arm: wall-clock best & median, joined/filtered rows entering the rollup, complete
inner-rollup rows, and final q67 rows. The checked-in query has `LIMIT 100`, so its final
output cannot exceed 100 rows. Historical notes list 5,342,291 input rows and 5,752,455
inner-rollup rows for one SF10 data/query variant; treat those as dataset-specific observations,
not universal q67 expectations, until the exact generator, scale, and measured physical node
are recorded.

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
- rollup-stage speedup, reported without projecting the historical standalone result onto
  the integrated plan.
- end-to-end q67 speedup (will be *less* than the stage speedup by Amdahl — that's honest).
- explicit statement that the historical **3.0–3.5x headline** recorded in
  `DEFENSIBILITY.md` was **not reproduced** at SF10 and must be scoped/retracted.

> Also identify the exact Stage 1 physical node whose counters are being quoted. That node
> ID is needed to attribute flush/abandon and extraction metrics and compare Stage 1-only
> against Stage 2; it is **not** a correctness prerequisite for starting the controlled
> q67 canary.

### 5.4 Required experiment arms

Keep cluster, data, jars, and native library fixed:

1. **Baseline:** lazy=false, fused=false.
2. **Stage 1 control:** lazy=true, fused=false.
3. **Initial Stage 2 canary:** lazy=true, fused=true, maxGroupingSets=16,
   finestSetBypass=false.
4. **Optional bypass experiment:** same as arm 3 with finestSetBypass=true, only after arm 3
   clears correctness and memory gates.

This separation tells whether a gain or regression belongs to the lazy partial aggregation,
the fused operator, or the optional bypass.

---

## 6. ROLLBACK & BLAST RADIUS

### 6.1 Rollback
- **The flags are default-off.** Primary Stage 2 rollback =
  `fusedGroupingSetAggregate.enabled=false`, which leaves the Stage 1 rewrite available.
  Full rollback = set both lazy and fused to `false`. Keep
  `finestSetBypass.enabled=false` unless explicitly running the fourth experiment arm. No
  redeploy is needed when these are session confs.
- **If a gate fails:** do not proceed to the next gate. Set flags off, capture the failing
  plan + logs + the minimal repro query, and file it. g1/g2/g3 failures block the trial
  entirely; g4 failure means the pressure path is not safe under this cluster's memory
  settings; g5
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

A **trial** (flag-off default, opt-in per session) can proceed only after the Linux package
clears g1–g5, including q67 itself. **Default-on** — shipping the flag `true` — additionally
requires closing all of these:

- **q67 end-to-end and scale coverage.** Multi-column routing is implemented and locally
  covered through native-plan construction plus native differential tests. Run q67
  flag-on/off on Linux at small scale and the intended trial scale, confirm the executed
  node and byte-identical results, and retain the plans and event logs.
- **B2 fallback verified in a running binary.** Local native-plan conversion now preserves
  the fused marker, lowers only the native grouping-set limit, and observes plain Expand for
  both child layouts. Run the same retained-plan fixture on Linux and confirm it **degrades
  with a warning, produces correct results, and reports correctly associated metrics** rather
  than failing the task. This is required before arbitrary-query opt-in/default-on, not before
  the tightly scoped q67 canary where q67 itself must fuse. Decimal sum and avg are no longer
  fallback examples; their multi-column buffers are supported.
- **B5 bypass policy.** Bypass is selectable and defaults off. Native correctness covers
  upstream flush and abandonment, but its cluster memory and performance trade-off is
  unmeasured. Keep it independently default-off unless the fourth experiment arm supplies
  repeatable evidence.
- **Serde / plan tracing.** Confirm full node registration under the cluster's normal
  packaged load path (C3). The macOS F14 baseline is inconclusive and does not clear this
  Linux gate.
- **AQE-on execution.** The source rewrite is stateless and guarded for idempotency, but
  local focused tests run AQE-off. Clear g3b before enabling the feature in a session where
  AQE is on.
- **CUBE performance.** Serde round-trips for CUBE (verified), but CUBE was **not** perf-measured.
  Measure a CUBE workload before claiming it benefits.
- **64-key limit.** The guard caps `bottomGroupingKeys.length <= 64`. Confirm behavior at
  and beyond 64 grouping keys (should fall back cleanly, not crash) before default-on.
- **Perf headline.** Scope/retract the historical 3.0–3.5x claim in
  `DEFENSIBILITY.md`; replace it only with the g6-measured stage number and the end-to-end
  Amdahl-limited figure.

---

## APPENDIX — key paths

Gluten-local operator (`cpp/velox/operators/plannodes/`, compiled into Gluten `libvelox`):
- `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.cc` — operator
- `cpp/velox/operators/plannodes/MultiGroupingSetAggregation.h`
- `cpp/velox/operators/plannodes/GroupingSetAggregationNode.{h,cc}` — node + serde
- `cpp/velox/operators/plannodes/GroupingSetLattice.{h,cc}`
- `cpp/velox/tests/MultiGroupingSetAggregationTest.cc` — unit tests (env hooks live here only)

Velox patch (into pinned Velox on the cluster) — the ONLY Velox change:
- `ep/build-velox/src/modify_hash_aggregation_input_buffer.patch` —
  `velox/exec/HashAggregation.{cpp,h}` needsInput fix (PR1, standalone and enforced by
  `build-velox.sh`)
- (retired) `0002-fused-grouping-set-operator.patch` — operator is now Gluten-local; see `velox-patches/README.md`

Gluten (this working tree):
- `backends-velox/.../extension/LazyAggregateExpandRule.scala` — fused eligibility,
  grouping-set bound, and multi-column buffer routing
- `backends-velox/.../config/VeloxConfig.scala` — lazy, fused, grouping-set maximum, and
  finest-set bypass settings
- `gluten-substrait/.../execution/ExpandExecTransformer.scala` — `isRollup` and
  `finestSetBypass` marker
- `cpp/velox/substrait/SubstraitToVeloxPlan.cc` — multi-column extraction-project
  conversion and B2 fallback net
- `cpp/velox/compute/VeloxBackend.cc` — backend-side registration

Not modified (despite the changeset brief): `VeloxRuleApi.scala`.
