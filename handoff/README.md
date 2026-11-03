# Handoff: native Lance writer for Gluten (2026-11-03)

This branch (`handoff/lance-native-write`) is the writer branch `feat/lance-native-write` plus one
commit that adds this `handoff/` folder. **Never push this commit or put it in a PR.** Do code work on
`feat/lance-native-write`.

## In one paragraph

Gluten can now write Lance tables through Velox. Each task exports its Velox batches as Arrow and
hands them to Lance's `Fragment.create`, and lance-spark commits the fragments. On a laptop the
writer is 2.5x faster than vanilla Spark for ordinary tables and 1.7x faster for vector tables. The
code is done, reviewed twice and audited, and passes on Spark 3.5 to 4.2. It goes upstream as three
PRs, one at a time:

1. **Version bump** apache/gluten#13515: open, waiting for a committer.
2. **Core writer**: not opened yet.
3. **Vectors**: not opened yet.

Nothing is blocked on code. The next step is waiting for #13515 to merge.

## Where everything is

| What | Where |
|---|---|
| Writer worktree | `~/Documents/GitHub/gluten-lance-writer` (a worktree of `~/Documents/GitHub/incubator-gluten`) |
| Writer branch | `feat/lance-native-write`, based on main `3ab17271e` (20+ commits behind main now) |
| Core writer commit | `7c4a9be5d` "[VL] Offload Lance table writes to Velox" (still includes the pom bump) |
| Vectors commit | `c8f17e537` "[VL] Write Lance fixed-size list columns natively" |
| Bulk-copy fix for null vectors | `7372e9fdd` "fixup! ...": **local only**, not on the fork |
| Fork branch | `fork-ssh` (`git@github.com:malinjawi/incubator-gluten.git`) `feat/lance-native-write` = `c8f17e537` |
| Pre-audit backup | local branch `lance-writer-pre-audit` (`be4d19e78`) |
| Bump PR branch | fork `chore/lance-0.9.0-beta.2` = `f7c0a7929` = local `lance-bump-pr`. Local `chore/lance-0.9.0-beta.2` (`c36a05d64`) is stale. |
| PR drafts (not posted) | `handoff/pr/core-writer.md`, `handoff/pr/vectors.md` |
| Benchmark harness | `~/Documents/GitHub/gluten-lance-bench` (not a git repo; copy in `handoff/bench/`, without jars and data) |
| Test runner | `handoff/scripts/run-lance-suites.sh` |
| Native build scripts | `~/Documents/GitHub/gluten-local-build/build-velox.sh`, `build-gluten-cpp.sh` (hardcoded to this worktree) |

Upstream pieces this builds on, all merged:
- lance-format/lance-spark#715 `LanceBatchWrite.taskCommit`;
- lance-format/lance-spark#892 `LanceBatchWrite.getFragmentWriteSpec`.

Both are first released in lance-spark `0.9.0-beta.2`, on Maven Central since 2026-10-31. lance-core
stays 12.0.0.

## Status of the PRs (2026-11-03)

- **#13515, the bump** `[GLUTEN-12263][VL] Bump lance-spark to 0.9.0-beta.2`:
  - open, CI green, mergeable;
  - sezruby approved; it still needs a committer's approval;
  - the user already posted the cc (zhouyuan, FelixYBW, sezruby). Don't ping again.
  - Its description promises to move to the next official lance-spark release once there is one.
- **Core writer and vectors:** not opened. The user has 17 open apache/gluten PRs, and a maintainer
  asked them to keep that number low.

## Next steps, in order

Each step that pushes, opens or posts anything needs the user's explicit OK in chat first.

1. **Wait for #13515 to merge.**
2. **Rebase the writer onto main:**
   - `git fetch origin`, then rebase `feat/lance-native-write` onto `origin/main`.
   - `pom.xml` will conflict: the core commit bumps `lance.version` with a different comment than
     #13515. Take main's version, so the core commit no longer touches `pom.xml`.
   - Drop the last paragraph of the core commit message ("This needs lance-spark 0.9.0-beta.2 ...").
   - Squash `7372e9fdd` into the vectors commit, with
     `git -c sequence.editor=: rebase -i --autosquash origin/main`.
   - If main's Velox pin (`ep/build-velox/src/get-velox.sh`, `VELOX_BRANCH`) changed, rebuild the
     native libraries first (see "Build and test").
3. **Two small additions to the core commit:**
   - **Memory paragraph** in `docs/backends/velox/get-started/VeloxLance.md`. Lance encodes and
     buffers data in its own native memory, which neither Spark nor Gluten's off-heap accounting
     sees. Users should size `spark.executor.memoryOverhead` for it. lance-spark's own writer has the
     same property.
   - **Benchmark numbers** for the PR body. They're already in `handoff/pr/core-writer.md`.
4. **Run the local checks** (see "Before pushing").
5. **Push and open the core PR, with the user's OK:**
   - Ask the user before force-pushing the rebased branch to the fork.
   - Put only the core commit on its own fork branch, for example `feat/lance-native-write-core`.
   - Run `gh pr list --repo apache/gluten --author malinjawi --state open`, tell the user the count
     and get their OK.
   - Open the PR with `handoff/pr/core-writer.md`. The user decides whom to cc.
6. **Before taking it out of draft or asking for review,** the user runs on their deployment:
   - a write to S3 or other object storage (never tested; everything so far was local disk);
   - a peak-memory measurement.

   If either finds a bug, fix it in the open PR.
7. **After the core PR merges:** open the vectors PR from `handoff/pr/vectors.md` and fill in the
   core PR number.
8. **When lance-spark publishes its next official release:** bump `lance.version` to it, as #13515
   promised.

## Rules from the user (these override defaults)

- **Ask before acting outside the machine.** Never push, force-push, open a PR, post a comment, cc or
  ping anyone without an explicit go in chat. Local commits are fine.
- **Don't amend a pushed commit.** The permission classifier blocks it; make a `fixup!` commit and
  ask before any force-push.
- **Identity:** commit as `linjawi.mohammad@gmail.com`, already set in this worktree; never the IBM
  address.
- **Attribution:** no `Co-Authored-By` trailer in commits. PR bodies end with
  `Generated-by: IBM Bob` and have no "Generated with Claude Code" footer.
- **PR title:** `[GLUTEN-12263][VL] ...`, since #12263 is the Lance write issue.
- **Editing a PR:** `gh pr edit` fails on apache/gluten. Use
  `gh api -X PATCH repos/apache/gluten/pulls/<n> -f title="..." -F body=@body.md`.
- **Pushing:** push over SSH (`fork-ssh`); HTTPS is rejected for anything touching
  `.github/workflows/`.
- **CI load:** keep open PRs few. Batch changes into one push, never push only to re-trigger CI, and
  run every check locally first.
- **Answers:** the user prefers short answers that lead with the result.

## Build and test

### Native libraries

`cpp/build/releases/libgluten.dylib` and `libvelox.dylib` in the worktree are built against main's
pinned Velox (`dft-2026_10_26`, at `ep/build-velox/build/velox_ep`).

If the pin moves:
1. Rebuild Velox with `~/Documents/GitHub/gluten-local-build/build-velox.sh` (about 21 minutes).
2. Rebuild Gluten's cpp with `build-gluten-cpp.sh` (about 2 minutes).
3. Copy the new dylibs into `backends-velox/target/scala-2.12/classes/darwin/aarch64/`.
   `run-scala-test.sh` does not re-copy them; check with `cmp`.

The writer itself has no C++ changes.

### Tests

```bash
handoff/scripts/run-lance-suites.sh /tmp/lance-logs 35 40          # CI covers these two
handoff/scripts/run-lance-suites.sh /tmp/lance-logs 41 42 34       # extra local coverage
```

- Spark 3.5 runs with JDK `temurin-21`. Spark 4.x and 3.4 run with `zulu-17` and `-Pjava-17`.
- The script calls `./dev/run-scala-test.sh` with `-P<spark>,backends-velox,lance,lance-test`.
- It needs bash 4+, which is why it uses `/opt/homebrew/bin/bash`.

Expected results:

| Commit | Spark 3.5 | 4.0 | 4.1 | 4.2 | 3.4 |
|---|---|---|---|---|---|
| `7372e9fdd` (with the fixup) | 33/33 | 33/33 | not run | not run | not run |
| `c8f17e537` | 32/32 | 32/32 | 32/32 | 32/32 | 31/32 |

- The tests are `VeloxLanceWriteSuite` (21 tests in the core commit, 23 with vectors),
  `VeloxLanceSuite` (5, the scan) and `LanceFixedSizeListsSuite` (4, 5 with the fixup).
- On Spark 3.4, "large string types fall back to lance-spark's writer" fails. That's Gluten's Lance
  scan crashing on LargeUtf8 when reading back on Spark 3.4, not the writer.
- Every run must show `leaks=0`. A leaked exported Velox batch aborts the JVM at task end.
- Each commit must compile and pass on its own, so run the suites at each commit before opening its
  PR.

### Before pushing

```bash
./dev/format-scala-code.sh                                   # spotless; covers src-lance
build/mvn -q -B scalastyle:check -Pspark-3.5 -Pbackends-velox -Plance -Plance-test -pl backends-velox
.github/workflows/util/check.py header main --fix            # dev/check.py header is dead
```

CI's scalastyle doesn't cover `src-lance`, so run it locally. Always run the Spark 4.0 suites as
well: Scala 2.13 broke twice where 2.12 compiled.

## Benchmark

The harness is `~/Documents/GitHub/gluten-lance-bench`. `handoff/bench/README.md` explains the
setups, how to run it locally or on a cluster, and how to read the numbers.

Jars:
- `jars/` holds the Gluten bundle built from this branch with the fixup.
- `gluten-bundle-before-bulk-copy.jar` is the bundle without the fixup.
- The lance-spark bundle is 0.9.0-beta.2 from Central.

To rebuild the Gluten bundle:

```bash
build/mvn -B package -Pbackends-velox,spark-3.5,scala-2.12,lance,fast-build -DskipTests -pl package -am
```

To run it locally:
- `SPARK_HOME=~/spark-3.5.7-bin-hadoop3`
- `JAVA_HOME=temurin-21`: the bundle is class version 65, so JDK 17 fails.
- `PYSPARK_PYTHON=/usr/bin/python3`

Results from 2026-11-02 (`local[12]`, M-series laptop, local disk, medians of 3 after a warm-up, all
read-back checksums equal; `handoff/bench/results-local.csv`):

| Data | Vanilla | Gluten, lance-spark writer | Gluten, native writer | Native vs vanilla |
|---|---|---|---|---|
| lineitem-like, 60M rows, 3.4 GB | 7.17 s, 86 s CPU | 6.17 s, 74 s CPU | 2.86 s, 34 s CPU | 2.51x |
| 1M x 768-float vectors, 2.9 GB | 1.59 s, 14.0 s CPU | 1.61 s, 13.9 s CPU | 0.95 s, 10.2 s CPU | 1.67x |
| same, 1% null vectors | 1.38 s, 13.8 s CPU | 1.28 s, 14.0 s CPU | 1.11 s, 13.0 s CPU | 1.24x |

Fixup A/B on the 1%-null data, 5 reps (`ab-*.csv`): 1.25 s and 14.6 s CPU without it, 1.01 s and
12.1 s CPU with it.

What the measurements do and don't capture:
- **CPU** is the whole JVM process's CPU. Spark's task CPU misses Lance's own threads.
- **Scan time** must not be subtracted from write time, because the native writer overlaps the scan.
- **No cluster or S3 numbers yet.** The user will run those on their deployment.

## How the writer works

All code is in `backends-velox/src-lance/` and builds only with `-Plance`.

### Planning
- `component/VeloxLanceComponent.scala` registers the write rules only when lance-spark has
  `org.lance.spark.write.FragmentWriteSpec`.
- `config/VeloxLanceConfig.scala` defines
  `spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite` (default `false`).
- `extension/OffloadLanceWrite.scala` holds the `OffloadSingleNode` rules that replace
  `AppendDataExec` and `OverwriteByExpressionExec` on lance-spark's `SparkWrite`. The pattern mirrors
  `src-iceberg`.
- `execution/LanceWriteExec.scala` is the shared trait plus the plan-time checks: is it a
  `SparkWrite`, is the table sharded (which needs a clustered distribution), and is any column an
  Arrow type Velox exports with a different layout. The exec nodes are
  `VeloxLanceAppendDataExec.scala` and `VeloxLanceOverwriteByExpressionExec.scala`.

### Per task (`connector/write/LanceColumnarBatchDataWriter.scala`)
1. Each Velox batch is exported to Arrow C structs on the task thread.
2. The first batch's schema is checked against the table's commit schema.
3. Batches go into an `ArrayBlockingQueue` of capacity 4.
4. A background thread runs `Fragment.create(uri, ArrowArrayStream, spec.toWriteParams())` over a
   `QueueReader`, which is an `ArrowReader` that drains the queue.
5. On commit the task returns `LanceBatchWrite.taskCommit(fragments)`, and lance-spark commits on the
   driver.
6. On abort it stops the thread, waiting up to 5 minutes, and frees every queued batch.

### Vectors (`connector/write/LanceFixedSizeLists.scala`)
Velox exports fixed-size lists as plain lists, so the writer moves them into the commit schema's
fixed-size list vectors:
- **No null rows:** a zero-copy transfer.
- **Some null rows:** one memcpy per row.
- **Null elements:** element by element.

The copy can't reuse buffers from the previous batch, because Lance may still be reading it.

### Tests and docs
- Tests: `src-lance/test/.../execution/VeloxLanceWriteSuite.scala`,
  `.../connector/write/LanceFixedSizeListsSuite.scala`, and
  `org/apache/spark/sql/gluten/LanceTestUtils.scala`.
- Docs: `docs/backends/velox/get-started/VeloxLance.md` covers the write requirements, the config and
  the fallback table.

## Known issues: when to deal with each

There are no known correctness bugs. All setups wrote identical data in every benchmark, and the
suites pass.

**With the writer PRs:** the bulk-copy fixup (squash into vectors), the memory docs paragraph, and the
benchmark numbers in the PR bodies.

**Before review, on the user's deployment, with no commit unless it finds a bug:**
- S3 and object-storage writes, including credentials vended by a Lance namespace;
- peak executor memory.

**Later, as separate small PRs, one at a time:**
- Native writes for what falls back today:
  - large strings and binaries;
  - unsigned integers (lance-spark beta #863 keeps them unsigned in the commit schema);
  - half floats and 256-bit decimals;
  - blobs;
  - nested or half-float vectors.

  Some of these need Velox or lance-spark changes first.
- Moving to the next official lance-spark release.
- Running the Lance tests on Spark 4.1 and 4.2 in CI.
- Reporting bytes written before commit. Today Lance reports sizes only at commit.
- Zero-copy null vectors. This needs Velox to export fixed-size lists, or a Lance change.
- Turning the writer on by default, after deployment numbers.

**Leave as is:**
- **Same as lance-spark's writer:**
  - the 5-minute wait for a stuck Lance write on abort, which then logs and leaks one Arrow struct;
  - orphaned files from failed or speculative tasks, until Lance cleanup removes them;
  - Spark's task CPU missing Lance's threads.
- **Spark 3.4 `CREATE TABLE AS SELECT` and `REPLACE TABLE AS SELECT`:** Spark 3.4 doesn't plan them as
  a nested append, so they stay on lance-spark's writer.
- **Read-path problems:**
  - Gluten's Lance scan crashes on fixed-size lists, apache/gluten#13487. sezruby's #13488 fixes it.
    Until then, the tests read vectors back with `spark.gluten.sql.columnar.batchscan=false`.
  - The Spark 3.4 LargeUtf8 scan crash.

## Gotchas learned the hard way

- **Arrow C import takes ownership** of the source struct. Never `release()` after import; `close()` is
  idempotent.
- **Scala 2.13:**
  - `Iterator.toSeq` is strict, so use `collectFirst` for a lazy first match.
  - An inner `ArrowReader`'s inherited `allocator` is ambiguous.
  - `f(x = ...)` parses as a named argument.
- **lance-spark's write validator matches `INSERT` columns by name,** so alias the `SELECT` columns in
  tests.
- **Appends must be written in APPEND mode** and overwrites in OVERWRITE mode, or field ids break on
  tables with dropped columns. `getFragmentWriteSpec` handles this; don't build write parameters by
  hand.
- **Blob detection:**
  - Blob v1 is the field metadata `lance-encoding:blob=true` (file format 2.1).
  - Blob v2 is the Arrow extension `lance.blob.v2` on a struct (2.2).
  - Use lance-spark's `BlobUtils` for both.
- **Sharded tables** come from `CREATE TABLE ... USING lance PARTITIONED BY (col)`.
- **`~/.m2` lance-spark 0.8.0 is a local build, not the release.** For Lance work, test against
  versions downloaded from Central.
- **Local benchmark runs need:**
  - `-Dio.netty.tryReflectionSetAccessible=true`;
  - both jars on `spark.driver.extraClassPath`, so Gluten sees lance-spark;
  - in macOS bash 3.2 with `set -u`, `${arr[@]+"${arr[@]}"}` for possibly empty arrays.

## Related work, not on this branch

- **Lance scan fallback:** fork branch `fix/lance-scan-fallback` (`34bef100e`, on top of #13488) adds
  the fallback for scans the Arrow stream can't carry. Open its PR only after #13488 merges, rebased
  so that it drops sezruby's commit.
- **Parked lance-spark follow-ups:** `~/Documents/GitHub/lance-spark-followups` holds the field-id
  validator and stream writer, not needed for this work.
- **First prototype:** `~/Documents/GitHub/gluten-lance-write`, historical only.
