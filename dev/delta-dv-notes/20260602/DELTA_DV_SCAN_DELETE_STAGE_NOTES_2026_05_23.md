# Delta DV Scan/Delete Stage Notes - 2026-05-23

This note records the local branch split used to preserve and track the Delta
Deletion Vector scan and DELETE work after native DV reader support landed in
Apache Gluten PR #12040.

## Baseline

- Native DV reader foundation: PR #12040, merged.
- Main scan follow-up worktree:
  `/Users/malinjawi/Documents/GitHub/GlutenVelox/incubator-gluten-dv-java-scan-pr`
- Main DELETE worktree:
  `/Users/malinjawi/Documents/GitHub/GlutenVelox/incubator-gluten-1-dv-delete`
- No branches were pushed during this staging pass.

## DV Scan Branches

Worktree:
`/Users/malinjawi/Documents/GitHub/GlutenVelox/incubator-gluten-dv-java-scan-pr`

Current branch after staging:
`split/delta-dv-java-scan-handoff-pr`

Backup branch:
`backup/delta-dv-java-scan-before-split-20260523-002115`

### 1. Payload Transport

Branch:
`split/delta-dv-payload-transport-pr`

Size:
`15 files, +496/-21`

Scope:

- Materialized DV payload transport through JVM/JNI/Substrait/Velox runtime.
- Split payload plumbing needed before Delta can hand materialized DV bytes to
  native scan execution.

### 2. JVM Delta DV Scan Handoff

Branch:
`split/delta-dv-java-scan-handoff-pr`

Size:
`13 files, +1535/-110`

Scope:

- Delta 3.3 and Delta 4.0 metadata utilities.
- Prepared scan hooks.
- Delta scan transformer changes.
- Delta DV post-transform rules.
- Handoff tests.

Review dependency:
Depends on `split/delta-dv-payload-transport-pr`.

### 3. Scan Hardening, Correctness, Benchmark

Branch:
`split/delta-dv-scan-hardening-benchmark-pr`

Size:
`9 files, +871/-52`

Scope:

- Broader DV scan correctness coverage.
- Read benchmark harness.
- Docs updates.
- Delta-specific candidate/validation cleanup.

Review dependency:
Depends on `split/delta-dv-java-scan-handoff-pr`.

### 4. Generic Gluten Planning Work

Branch:
`split/gluten-planning-overhead-pr`

Size:
`10 files, +103/-49`

Scope:

- Generic `HeuristicTransform` and fallback validation overhead reductions.
- Rewrite gating for simple plans.
- This is not Delta DV-specific and should be reviewed separately from scan
  correctness.

Review dependency:
Currently based on `split/delta-dv-java-scan-handoff-pr`, but conceptually a
core Gluten planning-performance follow-up.

### 5. Preserved Scan Tuning Snapshot

Branch:
`split/delta-dv-scan-tuning-wip`

Scope:

- Full preserved WIP state of the scan tuning experiments before splitting.
- Kept as a recovery branch, not intended as a review branch.

## DV DELETE Branches

Worktree:
`/Users/malinjawi/Documents/GitHub/GlutenVelox/incubator-gluten-1-dv-delete`

Current branch after staging:
`stage/delta-dv-delete-tests-benchmarks-20260523`

Backup branch:
`backup/delta-dv-delete-before-stage-20260523-003011`

Full safety snapshot:
`stage/delta-dv-delete-all-progress-20260523-003011`

Snapshot size:
`33 files, +9807/-97`

The staged DELETE stack was verified to match the all-progress snapshot exactly.

### 1. Native Bitmap Support

Branch:
`stage/delta-dv-delete-native-bitmap-20260523`

Size:
`15 files, +484/-56`

Scope:

- Native bitmap aggregation.
- `RoaringBitmapArray` changes.
- C++ function registration.
- Runtime/backend support needed by native bitmap construction.

### 2. DML Target Scan Helpers

Branch:
`stage/delta-dv-delete-dml-scan-20260523`

Size:
`5 files, +548/-33`

Scope:

- DML target scan helpers.
- Row-index scan path support.
- Delta DML utility code.
- Plain Parquet target handoff support for eligible DELETE paths.

Review dependency:
Depends on `stage/delta-dv-delete-native-bitmap-20260523`.

### 3. DELETE Command Integration

Branch:
`stage/delta-dv-delete-command-20260523`

Size:
`10 files, +3517/-8`

Scope:

- Delta 3.3 and Delta 4.0 DELETE command integration.
- `GlutenDMLWithDeletionVectorsHelper`.
- `GlutenDeleteCommand`.
- DELETE timing helper.
- Transaction/write command hooks.

Review dependency:
Depends on `stage/delta-dv-delete-dml-scan-20260523`.

### 4. DELETE Tests And Benchmarks

Branch:
`stage/delta-dv-delete-tests-benchmarks-20260523`

Size:
`3 files, +5258`

Scope:

- DELETE DV benchmark files.
- Delta native write/delete test coverage.
- Performance timing and benchmark scaffolding.

Review dependency:
Depends on `stage/delta-dv-delete-command-20260523`.

## Suggested Review Order

1. `split/delta-dv-payload-transport-pr`
2. `split/delta-dv-java-scan-handoff-pr`
3. `split/delta-dv-scan-hardening-benchmark-pr`
4. `split/gluten-planning-overhead-pr`
5. `stage/delta-dv-delete-native-bitmap-20260523`
6. `stage/delta-dv-delete-dml-scan-20260523`
7. `stage/delta-dv-delete-command-20260523`
8. `stage/delta-dv-delete-tests-benchmarks-20260523`

## Current Technical Posture

### Scan

- DV scan correctness work is separated from generic Gluten planning work.
- The scan handoff stack should stay focused on correctness and native handoff.
- Full-query wall-clock speedup can still be limited by generic Gluten planning
  and validation overhead.

### DELETE

- DELETE work is preserved but still staged as WIP.
- Main functional buckets are native bitmap construction, DML row-index/target
  scan support, command integration, and tests/benchmarks.
- Performance work should distinguish:
  - target scan and row-index materialization,
  - bitmap aggregation/construction,
  - DV storage,
  - Delta commit/checksum/post-commit cost.

## Validation Done During Staging

- Scan worktree clean after split.
- DELETE worktree clean after split.
- `git diff --check` passed.
- Final DELETE staged stack matches
  `stage/delta-dv-delete-all-progress-20260523-003011` exactly.

## DV Scan Validation Update - 2026-05-23

Branch validated:
`split/delta-dv-scan-hardening-benchmark-pr`

Fix added during validation:

- Commit `f9daab869`
  `[VL][Delta] Keep scan hardening independent of planning API`
- Reason: the scan hardening branch had accidentally depended on the generic
  planning API change from `split/gluten-planning-overhead-pr`. The branch now
  compiles independently of that planning PR.

### Correctness Results

Spark 3.5 / Delta 3.3:

- Command:
  `./dev/run-scala-test.sh --force -Pjava-17,spark-3.5,backends-velox,hadoop-3.3,spark-ut,delta -pl backends-velox -s org.apache.gluten.execution.VeloxDeltaSuite`
- Result:
  `24 tests succeeded, 0 failed`

Spark 4.0 / Delta 4.0:

- Command:
  `./dev/run-scala-test.sh --force -Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,spark-ut,delta -pl backends-velox -s org.apache.gluten.execution.VeloxDeltaSuite`
- Result:
  `24 tests succeeded, 0 failed`

Core DV scan coverage now validated in both supported runtime profiles:

- base DV scan,
- `deletionVectors.useMetadataRowIndex=false`,
- partitioned table with DVs,
- multiple DV-bearing data files,
- column mapping + DV,
- prepared scan / stats skipping.

### Performance Results

Benchmark class:
`org.apache.gluten.execution.DeltaDeletionVectorReadBenchmark`

Spark 3.5 / Delta 3.3, 5M rows, 16 files, delete every 10th row.

Scan-only materialize mode:

- Command args:
  `5000000 16 10 3 1 1.0 0.0 true false materialize deltaDv`
- Spark median:
  `planning=179.0 ms, execution=273.4 ms, total=452.4 ms`
- Gluten median:
  `planning=183.8 ms, execution=202.8 ms, total=387.9 ms`
- Speedup:
  `planning=0.97x, execution=1.35x, total=1.17x`

Scan-only count mode:

- Command args:
  `5000000 16 10 5 2 1.5 0.0 true false count deltaDv`
- Spark median:
  `planning=190.5 ms, execution=231.0 ms, total=422.2 ms`
- Gluten median:
  `planning=190.2 ms, execution=99.1 ms, total=289.5 ms`
- Speedup:
  `planning=1.00x, execution=2.33x, total=1.46x`

Full native count mode:

- Command args:
  `5000000 16 10 5 2 1.5 0.0 false false count deltaDv`
- Spark median:
  `planning=178.7 ms, execution=204.2 ms, total=382.9 ms`
- Gluten median:
  `planning=266.9 ms, execution=70.0 ms, total=336.1 ms`
- Speedup:
  `planning=0.67x, execution=2.92x, total=1.14x`

Current scan posture:

- Core correctness gaps are closed for the supported Spark/Delta profiles.
- Native DV scan execution meets the 2x target in scan-consuming count mode.
- Materialized row output is faster but not 2x because row conversion/materialization is included.
- Full-query total speedup is still limited by generic Gluten planning overhead, not by DV scan
  correctness.
