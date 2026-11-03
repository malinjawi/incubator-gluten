Title: [GLUTEN-12263][VL] Offload Lance table writes to Velox

<!-- Draft, not posted. Open only after apache/gluten#13515 merges and only with the user's OK.
     The commit must no longer touch pom.xml by then (see handoff/README.md, "Next steps"). -->

## What changes are proposed in this pull request?

This is the write half of #12263, using the lance-spark path agreed there. Gluten passes Velox's batches to Lance as Arrow, with no row conversion, and lance-spark still does the commit.

The feature is experimental and off by default. With `spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite=true`, writes to Lance tables run through Velox:
- appends and overwrites (`INSERT INTO`, `INSERT OVERWRITE`, `mode("append")` and `mode("overwrite")`)
- the nested writes of `CREATE TABLE AS SELECT` and `REPLACE TABLE AS SELECT` (Spark 3.5 and later)

How it works:
- Each task exports its Velox batches as an Arrow C stream. It calls `Fragment.create` with the write parameters lance-spark resolves for the table, from `LanceBatchWrite.getFragmentWriteSpec()` (lance-format/lance-spark#892). These include the write mode, file format version, file size limits and storage options.
- The task returns `LanceBatchWrite.taskCommit(fragments)` (lance-format/lance-spark#715). lance-spark then commits or stages the fragments on the driver, as it does for its own writer.
- Tasks report the same `recordsWritten` and `bytesWritten` metrics as lance-spark's writer, plus Gluten's native write metrics.

Some writes stay on lance-spark's writer, decided at plan time:
- sharded tables
- blob columns (v1 and v2)
- large strings and binaries
- half floats
- unsigned integers
- fixed-size lists, such as vectors (a follow-up writes them natively)

The first batch each task exports is also checked against the table's Arrow schema, so a layout mismatch fails the task instead of writing bad data.

This builds on #13515, which moved `lance.version` to 0.9.0-beta.2, the first lance-spark release with both changes.

## How was this patch tested?

Tests run on Spark 3.5 and 4.0:

`VeloxLanceWriteSuite` (21 tests) covers:
- append and overwrite, including `write_mode=overwrite`
- several tasks and several batches per task
- an empty append
- a table with dropped columns
- struct, array and map columns
- a table pinned to file format 2.1: appended files stay 2.1 and roll over at `max_row_per_file`
- metrics
- SQL `INSERT INTO`, `INSERT OVERWRITE`, `CREATE TABLE AS SELECT` and `REPLACE TABLE AS SELECT` through a Lance catalog
- a task that fails mid-write and a failure inside Lance: the table is unchanged and no Arrow memory leaks
- fallbacks: sharded tables, blob v1 and v2 columns, large strings and unsigned integers

`VeloxLanceSuite` still passes.

Local benchmark (laptop, `local[12]`, Spark 3.5, writing a 60M-row, 16-column table shaped like TPC-H `lineitem` to local disk, median of 3): 2.86 s with the native writer, against 7.17 s for vanilla Spark (2.5x) and 6.17 s for Gluten with lance-spark's writer (2.2x), using 34 s of CPU instead of 86 s and 74 s. All three wrote the same data.

## Was this patch authored or co-authored using generative AI tooling?

Generated-by: IBM Bob
