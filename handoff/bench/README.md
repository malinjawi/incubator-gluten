# Lance write benchmark for Gluten

Times writes of the same Parquet input to new Lance tables in three setups:

| Setup | What runs |
|---|---|
| `vanilla` | Spark with lance-spark, no Gluten |
| `gluten-row` | Gluten, with `enableNativeWrite=false`: Velox batches go through lance-spark's row writer |
| `gluten-native` | Gluten, with `enableNativeWrite=true`: Velox batches go to Lance as Arrow |

Each write goes to a new table, after one warm-up write. Every setup reads its last table back and
compares the row count and an `xxhash64` checksum with the input, so all setups must write the same
data.

## Files

- `lance_write_bench.py`: `gen` writes the input Parquet; `write` times the writes and appends to a CSV.
- `run_local.sh`: generates the inputs and runs every setup on one machine, each in its own `spark-submit`.
- `summarize.py`: prints a Markdown table of medians and speedups from the CSV.

## Inputs

| Name | Rows (default) | Columns |
|---|---|---|
| `lineitem` | 60M (about TPC-H SF10) | TPC-H `lineitem`'s 16 columns and types: longs, ints, decimals, dates, short and long strings |
| `vectors` | 1M | `id`, and a 768-float vector (a fixed-size list in Lance) |
| `vectors-null` | 1M | as `vectors`, with 1% null vectors |

Any Parquet input works with `write`; pass `--vector-col NAME --dim N` for a vector column.

## Run on one machine

```bash
export SPARK_HOME=/path/to/spark-3.5.x
export JAVA_HOME=/path/to/jdk-21          # the JDK the Gluten bundle was built with, or newer
export GLUTEN_JAR=/path/to/gluten-velox-bundle-spark3.5_2.12-<os>_<arch>-<version>.jar  # built with -Plance
export LANCE_JAR=/path/to/lance-spark-bundle-3.5_2.12-0.9.0-beta.2.jar
CORES=12 ./run_local.sh
```

Other knobs: `ROWS`, `VECTOR_ROWS`, `DIM`, `REPS`, `DATASETS`, `SETUPS`, `DATA_DIR`, `RESULTS`,
`HEAP_VANILLA`, `HEAP_GLUTEN`, `OFFHEAP_GLUTEN`. Vanilla Spark gets all its memory as heap, and Gluten
the same total split into heap and off-heap.

## Run on a cluster

Generate the inputs once (`gen`), then run `write` once per setup with your usual `spark-submit`
options. Pass `--label` to name the setup, `--target` for where the tables go (a local path, HDFS or an
object store), and the same `--results` file for every run. Both jars must be on the driver and
executor classpaths (`spark.driver.extraClassPath`, `spark.executor.extraClassPath`) as well as in
`--jars`, so Gluten can see lance-spark. Gluten setups also need:

```
--conf spark.plugins=org.apache.gluten.GlutenPlugin
--conf spark.memory.offHeap.enabled=true
--conf spark.memory.offHeap.size=<size>
--conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
--conf spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true
--conf spark.executor.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true
--conf spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite=true   # false for gluten-row
```

## Reading the results

- **Write** is the wall time of the whole write: the Parquet scan plus the Lance write and commit.
  **Scan only** times the same scan into Spark's no-op sink. Don't subtract it from the write: the
  native writer runs Lance on a background thread that overlaps the scan.
- **Process CPU** is the CPU time of the whole JVM during the write, including Velox's and Lance's own
  threads. It is only measured in local mode, where the driver runs every task.
- **Task CPU** is Spark's `executorCpuTime`. It counts task threads only, so it misses the Lance
  encoding and writing threads, and undercounts both writers.
- **Output** is the table size on disk (local paths only).
