# Delta Lake Feature Support Status in Apache Gluten (Velox Backend)

This document summarizes the support status of **Delta Lake table features** when used with **Apache Gluten (Velox backend)**.

## Supported Spark / Delta combinations

| Spark profile | Spark version | Scala version | Delta Lake version | Status |
|---|---|---|---|---|
| `spark-3.5` | Spark 3.5.x | 2.12 | 3.3.x | Supported |
| `spark-4.0` | Spark 4.0.x | 2.13 | 4.0.x | Supported |

Native Delta write is supported in both Spark 3.5 and Spark 4.0 profiles. The difference between
the two rows above is the Spark/Delta compatibility target (Spark 3.5 + Delta 3.3 vs Spark 4.0 +
Delta 4.0), not a native-write capability gap.

## Build and runtime notes

Build Gluten with Delta support by enabling `-Pdelta` together with the Velox backend profile and a Spark profile.

- Spark 3.5 build example:
  - `mvn clean package -Pbackends-velox -Pdelta -Pspark-3.5 -DskipTests`
- Spark 4.0 build example:
  - `mvn clean package -Pbackends-velox -Pdelta -Pspark-4.0 -Pscala-2.13 -Pjava-17 -DskipTests`

Native Delta write is controlled by:

- `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite`
  - Default: `false`
  - Type: experimental

## Deletion Vector Read Benchmarking

When benchmarking native Delta deletion-vector reads, start with scan-only mode to isolate the
read-scan path:

- `spark.gluten.sql.columnar.scanOnly=true`

This keeps the Delta DV scan in Velox while leaving wider aggregation and shuffle planning to Spark.
That is the recommended posture for evaluating DV read-scan acceleration. To measure a normal native
scan-consuming query, set the benchmark's `scanOnly` argument to `false`; full query wall time can
still be dominated by broader Gluten validation and planning costs.

For review and performance analysis, report both:

- Physical planning time: time to materialize `queryExecution.executedPlan`.
- Post-planning execution time: action time after the executed plan has already been materialized.

The split avoids hiding a fast native DV scan behind unrelated physical planning overhead.

The focused benchmark is:

- `org.apache.gluten.execution.DeltaDeletionVectorReadBenchmark`

Its arguments are:

- `rows files deleteModulo iterations warmups minExecutionSpeedup minTotalSpeedup scanOnly printPlans mode`

The `mode` argument is either `materialize` or `count`. `materialize` reads the data rows and also
reports the native columnar child below the top row conversion when available. `count` runs a normal
`count(*)` query over the DV-bearing table.

The benchmark disables Gluten fallback reporting because fallback reporting is diagnostic logging,
not part of the scan path being measured. Native validation remains enabled.

For example, a native count-scan gate that requires at least 2x post-planning execution speedup is:

- `org.apache.gluten.execution.DeltaDeletionVectorReadBenchmark 5000000 16 10 5 2 2.0 0.0 false false count`

Use the same test classpath as `VeloxDeltaSuite`; the benchmark compares Spark and Gluten in one
process, validates the row count for both variants, and verifies that the Gluten variant uses
`DeltaScanTransformer`.

Local measurements should report the execution speedup and total speedup separately. The native DV
scan can be more than 2x faster after planning, while full wall-clock speedup may still be bounded by
shared Delta `PrepareDeltaScan` work and generic Gluten physical planning.

For correctness, run the Delta DV scan tests in both supported profiles:

- Spark 3.5 / Delta 3.3: `org.apache.gluten.execution.VeloxDeltaSuite`
- Spark 4.0 / Delta 4.0: `org.apache.gluten.execution.VeloxDeltaSuite`

The DV read tests cover metadata row-index enabled and disabled, partitioned tables, multiple
DV-bearing files, column mapping, and prepared scans with stats skipping.

| Feature | Delta minWriterVersion | Delta minReaderVersion | Iceberg format-version | Feature type | Supported by Gluten (Velox) |
|---|---:|---:|---:|---|---|
| Basic functionality | 2 | 1 | 1 | Writer | Yes |
| CHECK constraints | 3 | 1 | N/A | Writer | No |
| Change data feed | 4 | 1 | N/A | Writer | Yes |
| Generated columns | 4 | 1 | N/A | Writer | Partial |
| Column mapping | 5 | 2 | N/A | Reader and writer | Yes |
| Identity columns | 6 | 1 | N/A | Writer | Yes |
| Row tracking | 7 | 1 | 3 | Writer | Partial |
| Deletion vectors | 7 | 3 | 3 | Reader and writer | Partial |
| TimestampNTZ | 7 | 3 | 1 | Reader and writer | No |
| Liquid clustering | 7 | 3 | 1 | Reader and writer | Yes |
| Iceberg readers (UniForm) | 7 | 2 | N/A | Writer | Not tested |
| Type widening | 7 | 3 | N/A | Reader and writer | Partial |
| Variant | 7 | 3 | 3 | Reader and writer | Not tested |
| Variant shredding | 7 | 3 | 3 | Reader and writer | Not tested |
| Collations | 7 | 3 | N/A | Reader and writer | Not tested |
| Protected checkpoints | 7 | 1 | N/A | Writer | Not tested |
