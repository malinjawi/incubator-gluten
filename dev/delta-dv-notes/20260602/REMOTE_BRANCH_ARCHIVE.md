# Delta DV Remote Archive - 2026-06-02

This branch preserves the local Delta deletion-vector scan, DELETE, bitmap, planning,
notes, and benchmark state that existed under:

`/Users/malinjawi/Documents/GitHub/GlutenVelox`

It is an archive branch for recovery and tracking, not a PR candidate.

## Active pushed branches

These branches matched `origin` exactly after the 2026-06-02 audit:

- `stage/delta-dv-delete-native-bitmap-20260523`
- `stage/delta-dv-delete-dml-scan-20260523`
- `stage/delta-dv-delete-command-20260523`
- `stage/delta-dv-delete-tests-benchmarks-20260523`
- `stage/delta-dv-delete-all-progress-20260523-003011`
- `stage/dv-scan-planning-tuning-20260523`
- `split/delta-dv-java-scan-handoff-pr-clean`
- `split/delta-dv-read-planning-performance-pr`
- `split/delta-dv-scan-hardening-benchmark-pr`
- `split/delta-planning-tuning-pr`
- `split/gluten-planning-overhead-pr`
- `split/gluten-planning-tuning-pr`
- `split/gluten-planning-tuning-pr-clean`
- `tmp/delta-planning-tuning-on-scan-hardening`

## Archive branches pushed

Local-only or diverged branches were pushed without overwriting active remote
branches. Each local head was preserved under an `archive/...-20260602` branch:

- `archive/backup/delta-dv-delete-before-stage-20260523-003011-20260602`
- `archive/backup/delta-dv-java-scan-before-split-20260523-002115-20260602`
- `archive/backup/split-delta-dv-java-scan-pr-before-rebase-20260602`
- `archive/delta-dv-fallback-coverage-20260602`
- `archive/delta-dv-java-materialized-handoff-clean-20260602`
- `archive/delta-dv-maintenance-20260602`
- `archive/delta-dv-read-foundation-20260602`
- `archive/delta-dv-read-foundation-backup-20260419-20260602`
- `archive/delta-dv-read-foundation-clean-20260602`
- `archive/delta-dv-read-foundation-sync-20260602`
- `archive/dv-ci-fix-20260602`
- `archive/fix/pr12040-dv-reader-contained-test-20260602`
- `archive/split/delta-dv-delete-pr-20260602`
- `archive/split/delta-dv-java-scan-handoff-pr-20260602`
- `archive/split/delta-dv-java-scan-pr-20260602`
- `archive/split/delta-dv-java-scan-pr-clean-20260602`
- `archive/split/delta-dv-java-scan-pr-review-clean-20260602`
- `archive/split/delta-dv-java-scan-pr-squashed-20260602`
- `archive/split/delta-dv-native-bitmap-pr-20260602`
- `archive/split/delta-dv-native-reader-20260602`
- `archive/split/delta-dv-native-reader-pr-20260602`
- `archive/split/delta-dv-payload-transport-pr-20260602`
- `archive/split/delta-dv-scan-info-utils-pr-20260602`
- `archive/tmp/delta-dv-planning-investigation-20260602`
- `archive/tmp/dv-scan-hardening-plus-planning-20260602`
- `archive/wip-delta-dv-gluten-integration-20260602`

## Notes and benchmark artifacts preserved here

- `DELTA_DV_SCAN_DELETE_STAGE_NOTES_2026_05_23.md`
- `DV_STACKS_2026-05-23.md`
- `BENCHMARK_RESULTS_ANALYSIS.md`
- `benchmark_results_gluten.json`
- `benchmark_results_spark.json`

## Caveat

The local `incubator-gluten-main` worktree still had uncommitted changes during
this audit. Those changes were not committed or pushed because they were broader
than the Delta DV scan/DELETE branch stack and should be reviewed separately.
