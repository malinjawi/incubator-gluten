/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.streaming

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.{BroadcastHashJoinExecTransformer, FileSourceScanExecTransformer, FilterExecTransformer, ProjectExecTransformer, RowToVeloxColumnarExec, ShuffledHashJoinExecTransformer, SparkOwnedMicroBatchScanExec, VeloxColumnarToRowExec}
import org.apache.gluten.execution.streaming.state.VeloxNativeStateStoreProvider

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenStreamingSQLTestsTrait, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.{VeloxNativeStreamingCountExec, VeloxNativeStreamingLongSumExec}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.{broadcast, expr, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType}

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.Date

import scala.collection.mutable.{ArrayBuffer, LinkedHashSet}
import scala.jdk.CollectionConverters._

class GlutenVeloxNativeStreamingTpchStatefulBenchmarkSuite
  extends StreamTest
  with GlutenStreamingSQLTestsTrait {

  import testImplicits._

  // Enable the Velox AsyncDataCache at backend init so the native file-source scan routes its
  // IO through Velox's CachedBufferedInput instead of the crashing GlutenDirectBufferedInput
  // (see cpp/velox/memory/GlutenBufferedInputBuilder.h). This is an INIT-TIME config, so it must
  // live on the base sparkConf, not inside withSQLConf.
  override def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.gluten.sql.columnar.backend.velox.cacheEnabled", "true")
      .set("spark.gluten.sql.columnar.backend.velox.memCacheSize", "536870912")
      .set("spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled", "true")
      .set("spark.gluten.sql.columnar.backend.velox.loadQuantum", "8388608")

  testGluten("TPC-H Q12-lite real fixture stream native-vs-vanilla count benchmark") {
    val rowCount = tpchScaleRows
    val rows = loadQ12EventRows(rowCount)
    val splitAt = math.max(1, rowCount / 2)
    val firstBatchRows = rows.take(splitAt)
    val secondBatchRows = rows.drop(splitAt)

    val nativeEvidence = withNativeStatefulAggregation {
      runQ12ShippingPriorityScenario(
        mode = "native",
        expectNative = true,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }
    val vanillaEvidence = withVanillaStreaming {
      runQ12ShippingPriorityScenario(
        mode = "vanilla",
        expectNative = false,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }

    assertQ12EvidenceParity(nativeEvidence, vanillaEvidence)
    tpchReportPath.foreach {
      reportPath =>
        appendQ12Evidence(
          reportPath,
          Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
    }
  }

  testGluten("TPC-H Q12-lite real fixture stream native-vs-vanilla long-sum benchmark") {
    val rowCount = tpchScaleRows
    val rows = loadQ12EventRows(rowCount)
    val splitAt = math.max(1, rowCount / 2)
    val firstBatchRows = rows.take(splitAt)
    val secondBatchRows = rows.drop(splitAt)

    val nativeEvidence = withNativeStatefulAggregation {
      runQ12ShippingPriorityLongSumScenario(
        mode = "native",
        expectNative = true,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }
    val vanillaEvidence = withVanillaStreaming {
      runQ12ShippingPriorityLongSumScenario(
        mode = "vanilla",
        expectNative = false,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }

    assertQ12EvidenceParity(nativeEvidence, vanillaEvidence)
    tpchReportPath.foreach {
      reportPath =>
        appendQ12Evidence(
          reportPath,
          Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
    }
  }

  testGluten("TPC-H Q12-lite real fixture stream native-vs-vanilla long-sum update benchmark") {
    val rowCount = tpchScaleRows
    val rows = loadQ12EventRows(rowCount)
    val splitAt = math.max(1, rowCount / 2)
    val firstBatchRows = rows.take(splitAt)
    val secondBatchRows = rows.drop(splitAt)

    val nativeEvidence = withNativeStatefulAggregation {
      runQ12ShippingPriorityLongSumScenario(
        mode = "native",
        expectNative = true,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount,
        outputMode = "update")
    }
    val vanillaEvidence = withVanillaStreaming {
      runQ12ShippingPriorityLongSumScenario(
        mode = "vanilla",
        expectNative = false,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount,
        outputMode = "update")
    }

    assertQ12EvidenceParity(nativeEvidence, vanillaEvidence)
    tpchReportPath.foreach {
      reportPath =>
        appendQ12Evidence(
          reportPath,
          Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
    }
  }

  testGluten(
    "TPC-H Q12-lite real fixture stream native-vs-vanilla orderkey long-sum update benchmark") {
    val rowCount = tpchScaleRows
    val rows = loadQ12EventRows(rowCount)
    val splitAt = math.max(1, rowCount / 2)
    val firstBatchRows = rows.take(splitAt)
    val secondBatchRows = rows.drop(splitAt)

    val nativeEvidence = withNativeStatefulAggregation {
      runQ12ShippingPriorityLongSumScenario(
        mode = "native",
        expectNative = true,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount,
        outputMode = "update",
        groupByOrderKey = true)
    }
    val vanillaEvidence = withVanillaStreaming {
      runQ12ShippingPriorityLongSumScenario(
        mode = "vanilla",
        expectNative = false,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount,
        outputMode = "update",
        groupByOrderKey = true)
    }

    assertQ12EvidenceParity(nativeEvidence, vanillaEvidence)
    tpchReportPath.foreach {
      reportPath =>
        appendQ12Evidence(
          reportPath,
          Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
    }
  }

  // ===========================================================================================
  // RIGOROUS STATEFUL BENCHMARK: clean resident-state native long-sum vs vanilla Spark on a
  // HIGH-CARDINALITY GROUP BY l_orderkey, SUM(...) Update-mode streaming aggregate.
  //
  // This is the decisive measurement for the question: does a CLEAN resident-state native stateful
  // operator -- one that returns the changed Update groups as a single primitive long[] instead of
  // a per-key byte[][] -- BEAT vanilla Spark (HDFSBackedStateStore + codegen) at scale?
  //
  // Rigor:
  //   - Native runs with the typed-primitive-output gate ON; vanilla runs on plain Spark.
  //   - Each engine runs GLUTEN_NATIVE_STREAMING_TPCH_REPEATS (default 5) repeats in ONE compile.
  //   - The source rows are split into RIGOR_PART_FILES MemoryStream batches; the FIRST batch is the
  //     dropped warmup; steady-state throughput is the mean processedRowsPerSecond over the rest.
  //   - Every run must produce the SAME final output checksum (native==vanilla parity), asserted.
  //   - One TSV row per (engine, repeat) -> target/stateful-clean-bench.tsv, appended incrementally
  //     so a kill leaves usable partial data.
  // Gated behind GLUTEN_NATIVE_STREAMING_TPCH_STATEFUL_RIGOR=1.
  // ===========================================================================================
  testGluten("TPC-H stateful clean resident-state orderkey long-sum native-vs-vanilla benchmark") {
    assume(
      sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_STATEFUL_RIGOR").exists(_.nonEmpty),
      "Set GLUTEN_NATIVE_STREAMING_TPCH_STATEFUL_RIGOR=1 to run the stateful clean benchmark")

    val rowCount = tpchScaleRows
    val repeats = rigorRepeats
    val rows = loadQ12EventRows(rowCount)

    // Append each repeat to the TSV as it completes so a kill leaves usable partial data.
    val reportPath = statefulRigorReportPath
    val nativeResults: Seq[StatefulRigorResult] = (0 until repeats).map {
      repeatIndex =>
        val r = withNativeStatefulAggregation {
          withSQLConf(
            GlutenConfig.NATIVE_STREAMING_STATEFUL_TYPED_PRIMITIVE_OUTPUT_ENABLED.key -> "true") {
            runStatefulRigorScenario(
              engine = "native",
              expectNative = true,
              rows = rows,
              scaleRows = rowCount,
              repeatIndex = repeatIndex)
          }
        }
        appendStatefulRigorResults(reportPath, Seq(r))
        r
    }
    val vanillaResults: Seq[StatefulRigorResult] = (0 until repeats).map {
      repeatIndex =>
        val r = withVanillaStreaming {
          runStatefulRigorScenario(
            engine = "vanilla",
            expectNative = false,
            rows = rows,
            scaleRows = rowCount,
            repeatIndex = repeatIndex)
        }
        appendStatefulRigorResults(reportPath, Seq(r))
        r
    }

    val allResults = nativeResults ++ vanillaResults
    // CORRECTNESS: every run must agree on the final aggregate output checksum.
    val referenceChecksum = allResults.head.outputChecksum
    val referenceCount = allResults.head.outputRowCount
    allResults.foreach {
      r =>
        assert(
          r.outputChecksum == referenceChecksum,
          s"Stateful rigor output checksum diverged: ${r.engine} repeat=${r.repeatIndex} " +
            s"checksum=${r.outputChecksum} != reference=$referenceChecksum")
        assert(
          r.outputRowCount == referenceCount,
          s"Stateful rigor output row count diverged: ${r.engine} repeat=${r.repeatIndex} " +
            s"count=${r.outputRowCount} != reference=$referenceCount")
    }
    nativeResults.foreach(r => assert(r.nativePlanNodes > 0, s"Expected native run to offload: $r"))
    vanillaResults.foreach(
      r => assert(r.nativePlanNodes == 0, s"Expected vanilla run to stay on Spark: $r"))

    // Summary stats: mean +/- stddev rows/s, CV%, speedup, robust-vs-noise (+/-2 sigma).
    def stats(rs: Seq[StatefulRigorResult]): (Double, Double, Double) = {
      val xs = rs.map(_.steadyStateRowsPerSecond)
      val mean = xs.sum / xs.length
      val variance =
        if (xs.length > 1) xs.map(x => (x - mean) * (x - mean)).sum / (xs.length - 1) else 0.0
      val sd = math.sqrt(variance)
      val cv = if (mean != 0.0) sd / mean * 100.0 else 0.0
      (mean, sd, cv)
    }
    val (nMean, nSd, nCv) = stats(nativeResults)
    val (vMean, vSd, vCv) = stats(vanillaResults)
    val speedup = if (vMean != 0.0) nMean / vMean else 0.0
    val robustWin = (nMean - 2 * nSd) > (vMean + 2 * vSd)
    val robustLoss = (nMean + 2 * nSd) < (vMean - 2 * vSd)
    val verdict =
      if (robustWin) "ROBUST_WIN"
      else if (robustLoss) "ROBUST_LOSS"
      else "NOISE_OVERLAP"
    // scalastyle:off println
    println(
      f"STATEFUL_CLEAN_BENCH scale=$rowCount repeats=$repeats%n" +
        f"  native  mean=${nMean}%.1f rows/s sd=${nSd}%.1f cv=${nCv}%.1f%%%n" +
        f"  vanilla mean=${vMean}%.1f rows/s sd=${vSd}%.1f cv=${vCv}%.1f%%%n" +
        f"  speedup(native/vanilla)=${speedup}%.3fx verdict=$verdict")
    // scalastyle:on println
  }

  testGluten(
    "TPC-H Q12-lite native parquet file-source orderkey long-sum update benchmark") {
    assume(
      sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_FILE_SOURCE").exists(_.nonEmpty),
      "Set GLUTEN_NATIVE_STREAMING_TPCH_FILE_SOURCE=1 to run the native file-source benchmark")

    val rowCount = tpchScaleRows
    val rows = loadQ12EventRows(rowCount)
    val splitAt = math.max(1, rowCount / 2)
    val firstBatchRows = rows.take(splitAt)
    val secondBatchRows = rows.drop(splitAt)

    val nativeEvidence = withNativeStatefulAggregation {
      withSQLConf(
        GlutenConfig.NATIVE_STREAMING_FILE_SOURCE_ENABLED.key -> "true",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "true",
        GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "true",
        // Parquet columns are nullable by default, so allow the native long-sum to engage on a
        // nullable summed value column (the fixture data is free of nulls).
        GlutenConfig.NATIVE_STREAMING_STATEFUL_LONG_SUM_NULLABLE_VALUE_ENABLED.key -> "true"
      ) {
        runQ12FileSourceOrderKeyLongSumScenario(
          mode = "native",
          expectNative = true,
          firstBatchRows = firstBatchRows,
          secondBatchRows = secondBatchRows,
          scaleRows = rowCount)
      }
    }
    val vanillaEvidence = withVanillaStreaming {
      runQ12FileSourceOrderKeyLongSumScenario(
        mode = "vanilla",
        expectNative = false,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }

    assertQ12EvidenceParity(nativeEvidence, vanillaEvidence)
    tpchReportPath.foreach {
      reportPath =>
        appendQ12Evidence(
          reportPath,
          Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
    }
  }

  testGluten("TPC-H Q12-lite native parquet file-source stream DIAG") {
    assume(
      sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_FILE_SOURCE").exists(_.nonEmpty),
      "Set GLUTEN_NATIVE_STREAMING_TPCH_FILE_SOURCE=1 to run the native file-source DIAG test")

    val rowCount = math.min(tpchScaleRows, 2000)
    val rows = loadQ12EventRows(rowCount)

    withTempDir {
      sourceDir =>
        withTempDir {
          checkpointDir =>
            val sourcePath = sourceDir.getCanonicalPath
            val schema = StructType(
              Seq(
                StructField("l_orderkey", LongType),
                StructField("l_linenumber", IntegerType),
                StructField("l_shipmode", StringType),
                StructField("l_shipdate", DateType),
                StructField("l_commitdate", DateType),
                StructField("l_receiptdate", DateType),
                StructField("o_orderpriority", StringType)
              ))

            // Materialize the fixture rows as Parquet so a streaming file source can read them.
            spark
              .createDataFrame(
                spark.sparkContext.parallelize(rows.map {
                  case (orderKey, lineNumber, shipMode, shipDate, commitDate, receiptDate, prio) =>
                    Row(orderKey, lineNumber, shipMode, shipDate, commitDate, receiptDate, prio)
                }),
                schema)
              .write
              .mode("overwrite")
              .parquet(sourcePath)

            withNativeStatefulAggregation {
              withSQLConf(
                GlutenConfig.NATIVE_STREAMING_FILE_SOURCE_ENABLED.key -> "true",
                GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "true",
                GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "true"
              ) {
                val queryName =
                  s"gluten_tpch_q12_filesrc_${checkpointDir.getName}".replaceAll("\\W", "_")
                val streamDf = spark.readStream
                  .schema(schema)
                  .parquet(sourcePath)
                  .where($"l_shipmode".isin("MAIL", "SHIP"))
                  .select($"l_orderkey", $"l_shipmode")

                val query = streamDf.writeStream
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpointDir.getCanonicalPath)
                  .start()
                try {
                  query.processAllAvailable()
                  val plan = executedPlan(query)
                  // scalastyle:off println
                  println("DIAGPLAN_START")
                  println(plan.toString)
                  println(
                    "DIAGPLAN_END rowToColumnar=" + rowToColumnarNodeCount(plan) +
                      " columnarToRow=" + columnarToRowNodeCount(plan))
                  // scalastyle:on println
                } finally {
                  query.stop()
                  spark.catalog.dropTempView(queryName)
                }
              }
            }
        }
    }
  }

  // DIAGNOSTIC: stateless compute-bound stream-to-broadcast-static join. Dumps the executed plan
  // so we can see whether the join offloads to a native BroadcastHashJoinExecTransformer or stays
  // vanilla / falls back (fenced by the streaming validator allowlist).
  testGluten("TPC-H compute-join stateless native streaming DIAG") {
    assume(
      sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_COMPUTE_JOIN").exists(_.nonEmpty),
      "Set GLUTEN_NATIVE_STREAMING_TPCH_COMPUTE_JOIN=1 to run the compute-join DIAG test")

    val rowCount = math.min(tpchScaleRows, 5000)
    val rows = loadQ12EventRows(rowCount)

    withTempDir {
      checkpointDir =>
        withNativeComputeJoinStreaming {
          val inputData = MemoryStream[Q12EventRow]
          val queryName =
            s"gluten_tpch_compute_join_diag_${checkpointDir.getName}".replaceAll("\\W", "_")
          val resultDf = computeJoinResultDf(inputData)
          val query = resultDf.writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .start()
          try {
            inputData.addData(rows: _*)
            query.processAllAvailable()
            val plan = executedPlan(query)
            // scalastyle:off println
            println("COMPUTEJOIN_DIAGPLAN_START")
            println(plan.toString)
            println(
              "COMPUTEJOIN_DIAGPLAN_END rowToColumnar=" + computeJoinRowToColumnarNodeCount(plan) +
                " columnarToRow=" + computeJoinColumnarToRowNodeCount(plan) +
                " nativeBroadcastJoin=" + nativeBroadcastJoinNodeCount(plan) +
                " nativeShuffledJoin=" + nativeShuffledJoinNodeCount(plan) +
                " vanillaBroadcastJoin=" + vanillaBroadcastJoinNodeCount(plan) +
                " nativeStatelessNodes=" + computeJoinNativeStatelessNodeCount(plan))
            // scalastyle:on println
          } finally {
            query.stop()
            spark.catalog.dropTempView(queryName)
          }
        }
    }
  }

  // BENCHMARK: native-vs-vanilla on a stateless compute-bound stream-to-broadcast-static join with
  // a heavy projection (arithmetic/string/conditional). No aggregation, no state store. Tests
  // whether Velox vectorization beats Spark whole-stage codegen when the state/control tax is absent.
  testGluten("TPC-H compute-join stateless native-vs-vanilla heavy-expr benchmark") {
    assume(
      sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_COMPUTE_JOIN").exists(_.nonEmpty),
      "Set GLUTEN_NATIVE_STREAMING_TPCH_COMPUTE_JOIN=1 to run the compute-join benchmark")

    val rowCount = tpchScaleRows
    val rows = loadQ12EventRows(rowCount)
    val splitAt = math.max(1, rowCount / 2)
    val firstBatchRows = rows.take(splitAt)
    val secondBatchRows = rows.drop(splitAt)

    val nativeEvidence = withNativeComputeJoinStreaming {
      runComputeJoinScenario(
        mode = "native",
        expectNative = true,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }
    val vanillaEvidence = withVanillaStreaming {
      runComputeJoinScenario(
        mode = "vanilla",
        expectNative = false,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }

    assertComputeJoinEvidenceParity(nativeEvidence, vanillaEvidence)
    tpchReportPath.foreach {
      reportPath =>
        appendQ12Evidence(
          reportPath,
          Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
    }
  }

  // CONVERGENCE BENCHMARK: combine the native PARQUET FILE source (no stream-side
  // RowToVeloxColumnar) with the stateless broadcast-static join and a HEAVY per-row projection.
  // No aggregation, no state store. This is the decisive "does native beat Spark on compute-bound
  // streaming" test: the stream enters native via FileSourceScanExecTransformer (rowToColumnar=0 on
  // the stream side; only the tiny static dim leaf is rowified), the join + projects run native, and
  // the only columnar->row boundary is the sink.
  testGluten("TPC-H converge native parquet file-source compute-join heavy-expr benchmark") {
    assume(
      sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_CONVERGE").exists(_.nonEmpty),
      "Set GLUTEN_NATIVE_STREAMING_TPCH_CONVERGE=1 to run the converged file-source compute-join " +
        "benchmark")

    val rowCount = tpchScaleRows
    val rows = loadQ12EventRows(rowCount)
    val splitAt = math.max(1, rowCount / 2)
    val firstBatchRows = rows.take(splitAt)
    val secondBatchRows = rows.drop(splitAt)

    val nativeEvidence = withNativeConvergeFileSourceJoinStreaming {
      runConvergeFileSourceJoinScenario(
        mode = "native",
        expectNative = true,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }
    val vanillaEvidence = withVanillaStreaming {
      runConvergeFileSourceJoinScenario(
        mode = "vanilla",
        expectNative = false,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }

    assertComputeJoinEvidenceParity(nativeEvidence, vanillaEvidence)
    tpchReportPath.foreach {
      reportPath =>
        appendQ12Evidence(
          reportPath,
          Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
    }
  }

  // HEAVY-COMPUTE BENCHMARK: same native PARQUET FILE source ⋈ broadcast-static-dim shape as the
  // converge benchmark, but with a MUCH heavier per-row projection (dozens of derived columns:
  // arithmetic chains, sqrt/pow/log, decimal math, date arithmetic, substring/concat/upper/lower/
  // length/regexp_extract string ops, nested CASE WHEN, hash/md5). No aggregation => stateless.
  // Uses a NON-ACCUMULATING foreachBatch sink (running row count + running XOR/sum checksum) so the
  // sink does not retain output rows and 10M does not OOM. This pushes the per-row compute toward
  // the regime where Velox vectorization is genuinely several-x faster than Spark whole-stage
  // codegen, to see how high native climbs above vanilla and whether the win keeps growing with
  // scale.
  testGluten("TPC-H heavy-compute native parquet file-source join non-accumulating benchmark") {
    assume(
      sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_HEAVYCOMPUTE").exists(_.nonEmpty),
      "Set GLUTEN_NATIVE_STREAMING_TPCH_HEAVYCOMPUTE=1 to run the heavy-compute file-source join " +
        "benchmark")

    val rowCount = tpchScaleRows
    val rows = loadQ12EventRows(rowCount)
    val splitAt = math.max(1, rowCount / 2)
    val firstBatchRows = rows.take(splitAt)
    val secondBatchRows = rows.drop(splitAt)

    val nativeEvidence = withNativeConvergeFileSourceJoinStreaming {
      runHeavyComputeJoinScenario(
        mode = "native",
        expectNative = true,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }
    val vanillaEvidence = withVanillaStreaming {
      runHeavyComputeJoinScenario(
        mode = "vanilla",
        expectNative = false,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }

    assertHeavyComputeEvidenceParity(nativeEvidence, vanillaEvidence)
    tpchReportPath.foreach {
      reportPath =>
        appendQ12Evidence(
          reportPath,
          Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
    }
  }

  // ===========================================================================================
  // RIGOROUS BENCHMARK: controlled native-vs-vanilla streaming compute/scale sweep.
  //
  // Same native island as the heavy-compute benchmark (Parquet file source ⋈ broadcast-static dim
  // + N derived columns, non-accumulating foreachBatch sink folding count + running xxhash64), but
  // built for STATISTICAL rigor instead of n=1:
  //   - ONE test invocation runs the query REPEATS times for native AND REPEATS times for vanilla,
  //     so a single compile covers all repeats. Native vs vanilla differ ONLY by the gate config
  //     (withNativeConvergeFileSourceJoinStreaming vs withVanillaStreaming) -- identical query,
  //     identical sink, identical data, identical trigger.
  //   - The input is materialized as several Parquet part-files and consumed with
  //     maxFilesPerTrigger=1 so each part-file is its own micro-batch. Throughput is measured as the
  //     mean processedRowsPerSecond over STEADY-STATE batches, EXCLUDING batch 0 (the warmup batch).
  //   - Every native run and every vanilla run must produce the SAME running count + running
  //     xxhash64 checksum; parity is asserted and the test aborts if it diverges.
  //   - One TSV row per (engine, compute_level, scale, repeat_index) is appended to
  //     target/rigor-bench.tsv with steady-state rows/s + max addBatch/trigger.
  //
  // Parameterized by:
  //   GLUTEN_NATIVE_STREAMING_TPCH_COMPUTE_LEVEL in {light, medium, heavy} -> ~3 / ~12 / ~30
  //     derived columns (light/medium are prefixes of the heavy column set).
  //   GLUTEN_NATIVE_STREAMING_TPCH_SCALE_ROWS (existing) -> total source rows.
  //   GLUTEN_NATIVE_STREAMING_TPCH_REPEATS (default 5) -> repeats per engine.
  // ===========================================================================================
  testGluten("TPC-H rigorous native-vs-vanilla streaming compute/scale benchmark") {
    assume(
      sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_RIGOR").exists(_.nonEmpty),
      "Set GLUTEN_NATIVE_STREAMING_TPCH_RIGOR=1 to run the rigorous compute/scale benchmark")

    val computeLevel = rigorComputeLevel
    val rowCount = tpchScaleRows
    val repeats = rigorRepeats
    val rows = loadQ12EventRows(rowCount)

    // Run all native repeats, then all vanilla repeats, within ONE invocation/compile.
    val nativeResults: Seq[RigorResult] = (0 until repeats).map {
      repeatIndex =>
        withNativeConvergeFileSourceJoinStreaming {
          runRigorScenario(
            engine = "native",
            expectNative = true,
            computeLevel = computeLevel,
            rows = rows,
            scaleRows = rowCount,
            repeatIndex = repeatIndex)
        }
    }
    val vanillaResults: Seq[RigorResult] = (0 until repeats).map {
      repeatIndex =>
        withVanillaStreaming {
          runRigorScenario(
            engine = "vanilla",
            expectNative = false,
            computeLevel = computeLevel,
            rows = rows,
            scaleRows = rowCount,
            repeatIndex = repeatIndex)
        }
    }

    // CORRECTNESS: every native run and every vanilla run must agree on the running count + running
    // xxhash64 checksum. Abort the test if any pair diverges.
    val allResults = nativeResults ++ vanillaResults
    val referenceCount = allResults.head.runningCount
    val referenceChecksum = allResults.head.runningChecksum
    allResults.foreach {
      r =>
        assert(
          r.runningCount == referenceCount,
          s"Rigor running row count diverged: ${r.engine} repeat=${r.repeatIndex} " +
            s"count=${r.runningCount} != reference=$referenceCount")
        assert(
          r.runningChecksum == referenceChecksum,
          s"Rigor running checksum diverged: ${r.engine} repeat=${r.repeatIndex} " +
            s"checksum=${r.runningChecksum} != reference=$referenceChecksum")
    }
    // Plan-shape sanity: native runs must offload, vanilla runs must not.
    nativeResults.foreach {
      r => assert(r.nativePlanNodes > 0, s"Expected native rigor run to offload: $r")
    }
    vanillaResults.foreach {
      r => assert(r.nativePlanNodes == 0, s"Expected vanilla rigor run to stay on Spark: $r")
    }

    // Emit one TSV row per (engine, compute_level, scale, repeat_index).
    appendRigorResults(rigorReportPath, allResults)
  }

  // DIAGNOSTIC: dump the executed plan for the converged file-source compute-join stream so we can
  // confirm the native file scan + native broadcast join + native projects engage with no
  // stream-side Row->Columnar bridge.
  testGluten("TPC-H converge native parquet file-source compute-join DIAG") {
    assume(
      sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_CONVERGE").exists(_.nonEmpty),
      "Set GLUTEN_NATIVE_STREAMING_TPCH_CONVERGE=1 to run the converged compute-join DIAG test")

    val rowCount = math.min(tpchScaleRows, 5000)
    val rows = loadQ12EventRows(rowCount)

    withTempDir {
      sourceDir =>
        withTempDir {
          checkpointDir =>
            val sourcePath = sourceDir.getCanonicalPath
            writeQ12EventParquetBatch(sourcePath, rows)
            withNativeConvergeFileSourceJoinStreaming {
              val queryName =
                s"gluten_tpch_converge_diag_${checkpointDir.getName}".replaceAll("\\W", "_")
              val resultDf = convergeFileSourceJoinResultDf(sourcePath)
              val query = resultDf.writeStream
                .format("memory")
                .queryName(queryName)
                .outputMode("append")
                .option("checkpointLocation", checkpointDir.getCanonicalPath)
                .start()
              try {
                query.processAllAvailable()
                val plan = executedPlan(query)
                // scalastyle:off println
                println("CONVERGE_DIAGPLAN_START")
                println(plan.toString)
                println(
                  "CONVERGE_DIAGPLAN_END rowToColumnar=" +
                    computeJoinRowToColumnarNodeCount(plan) +
                    " columnarToRow=" + computeJoinColumnarToRowNodeCount(plan) +
                    " nativeFileScan=" + convergeNativeFileScanNodeCount(plan) +
                    " nativeBroadcastJoin=" + nativeBroadcastJoinNodeCount(plan) +
                    " nativeShuffledJoin=" + nativeShuffledJoinNodeCount(plan) +
                    " vanillaBroadcastJoin=" + vanillaBroadcastJoinNodeCount(plan) +
                    " vanillaFileScan=" + convergeVanillaFileScanNodeCount(plan) +
                    " nativeStatelessNodes=" + computeJoinNativeStatelessNodeCount(plan))
                // scalastyle:on println
              } finally {
                query.stop()
                spark.catalog.dropTempView(queryName)
              }
            }
        }
    }
  }

  // Native streaming config for the CONVERGED file-source + stateless-join probe. Enables the native
  // file-source gate (so the Parquet readStream owns the scan natively) PLUS the columnar broadcast
  // join / exchange / project / filter / scan and the stateless-join gate. No state store.
  private def withNativeConvergeFileSourceJoinStreaming[T](testBody: => T): T = {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_JOIN_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_FILE_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_BROADCAST_JOIN_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_BROADCAST_EXCHANGE_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> (10 * 1024 * 1024).toString,
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      testBody
    }
  }

  // Build the converged result DataFrame: a Parquet FILE-source stream filtered, inner-joined to the
  // broadcast static dim table, then a HEAVY projection. Heavier than computeJoinResultDf: more
  // arithmetic, string, and conditional expressions to give Velox real vectorization work. No
  // groupBy/aggregate => no state store.
  private def convergeFileSourceJoinResultDf(sourcePath: String) = {
    val streamDf = spark.readStream
      .schema(q12EventSchema)
      .parquet(sourcePath)
      .where(
        $"l_shipmode".isin("MAIL", "SHIP") &&
          $"l_commitdate" < $"l_receiptdate" &&
          $"l_shipdate" < $"l_commitdate" &&
          $"l_receiptdate" >= q12StartReceiptDate &&
          $"l_receiptdate" < q12EndReceiptDate)

    streamDf
      .join(broadcast(shipModeDimDf), $"l_shipmode" === $"dim_shipmode", "inner")
      .select(
        $"l_orderkey",
        $"l_linenumber",
        $"dim_carrier_class",
        // Heavy arithmetic chain over the joined columns.
        expr(
          "cast(round((l_orderkey % 100000) * dim_rate_multiplier " +
            "+ l_linenumber * dim_priority_weight " +
            "+ datediff(l_receiptdate, l_shipdate) * 1.5 " +
            "- datediff(l_receiptdate, l_commitdate) * 0.75, 2) as double)").as("freight_cost"),
        // Second arithmetic chain: powers, sqrt, abs, log over the same inputs.
        expr(
          "cast(round(sqrt(abs((l_orderkey % 50000) * 1.0)) * dim_rate_multiplier " +
            "+ pow(l_linenumber % 97 + 1, 1.5) " +
            "- log(2.0, (l_orderkey % 9973) + 2.0) * dim_priority_weight, 4) as double)")
          .as("risk_score"),
        // Conditional / nested CASE expression.
        expr(
          "case when datediff(l_receiptdate, l_commitdate) > 30 then 'LATE' " +
            "when datediff(l_receiptdate, l_commitdate) > 7 then 'DELAYED' " +
            "when datediff(l_receiptdate, l_shipdate) > 14 then 'SLOW_SHIP' " +
            "else 'ON_TIME' end").as("delivery_bucket"),
        // Conditional bucketing over the arithmetic output.
        expr(
          "case when (l_orderkey % 100000) * dim_rate_multiplier > 50000 then 'TIER_A' " +
            "when (l_orderkey % 100000) * dim_rate_multiplier > 10000 then 'TIER_B' " +
            "else 'TIER_C' end").as("value_tier"),
        // String expressions: concat / upper / lower / substring / lpad.
        expr(
          "concat(upper(dim_carrier_class), '-', substring(o_orderpriority, 4, 20), '-', " +
            "lpad(cast(l_orderkey % 1000 as string), 4, '0'))").as("route_label"),
        // Second string expression: reverse / translate / length-driven.
        expr(
          "concat(lower(substring(l_shipmode, 1, 2)), '_', " +
            "cast(length(o_orderpriority) as string), '_', " +
            "reverse(cast(l_linenumber % 1000 as string)))").as("ship_tag"),
        // Nested arithmetic / modulo with a guard.
        expr(
          "cast((l_orderkey * 31 + l_linenumber * 17) % 9973 as int)").as("hash_bucket"),
        // Second hash with different mixing constants.
        expr(
          "cast((l_orderkey * 1009 + l_linenumber * 131 + " +
            "datediff(l_receiptdate, l_shipdate) * 7) % 100003 as int)").as("hash_bucket_2"))
  }

  private def runConvergeFileSourceJoinScenario(
      mode: String,
      expectNative: Boolean,
      firstBatchRows: Seq[Q12EventRow],
      secondBatchRows: Seq[Q12EventRow],
      scaleRows: Int): Q12Evidence = {
    var result: Q12Evidence = null
    withTempDir {
      sourceDir =>
        withTempDir {
          checkpointDir =>
            val sourcePath = sourceDir.getCanonicalPath
            val checkpointPath = checkpointDir.getCanonicalPath
            val queryName =
              s"gluten_tpch_converge_${checkpointDir.getName}".replaceAll("\\W", "_")
            val progress = ArrayBuffer.empty[StreamingQueryProgress]
            val nativeStatelessCounts = ArrayBuffer.empty[Int]
            val nativeJoinCounts = ArrayBuffer.empty[Int]
            val nativeFileScanCounts = ArrayBuffer.empty[Int]
            val rowToColumnarCounts = ArrayBuffer.empty[Int]
            val columnarToRowCounts = ArrayBuffer.empty[Int]
            val seenProgressBatchIds = LinkedHashSet.empty[Long]
            val resultDf = convergeFileSourceJoinResultDf(sourcePath)

            def startQuery(): StreamingQuery = {
              resultDf.writeStream
                .format("memory")
                .queryName(queryName)
                .outputMode("append")
                .option("checkpointLocation", checkpointPath)
                .start()
            }

            def collectEvidence(query: StreamingQuery): Unit = {
              val meaningfulProgress = query.recentProgress.filter {
                p =>
                  p.numInputRows > 0L || Option(p.sink).exists(_.numOutputRows > 0L)
              }.filterNot(p => seenProgressBatchIds.contains(p.batchId))
              if (meaningfulProgress.nonEmpty) {
                progress ++= meaningfulProgress
                seenProgressBatchIds ++= meaningfulProgress.map(_.batchId)
              } else {
                Option(query.lastProgress)
                  .filterNot(p => seenProgressBatchIds.contains(p.batchId))
                  .foreach {
                    p =>
                      progress += p
                      seenProgressBatchIds += p.batchId
                  }
              }
              val plan = executedPlan(query)
              nativeStatelessCounts += computeJoinNativeStatelessNodeCount(plan)
              nativeJoinCounts += nativeBroadcastJoinNodeCount(plan) +
                nativeShuffledJoinNodeCount(plan)
              nativeFileScanCounts += convergeNativeFileScanNodeCount(plan)
              rowToColumnarCounts += computeJoinRowToColumnarNodeCount(plan)
              columnarToRowCounts += computeJoinColumnarToRowNodeCount(plan)
              if (expectNative) {
                assertNativeConvergeJoinPlan(plan)
              } else {
                assertNoNativeComputeJoinPlan(plan)
              }
            }

            val query = startQuery()
            try {
              writeQ12EventParquetBatch(sourcePath, firstBatchRows)
              query.processAllAvailable()
              collectEvidence(query)

              writeQ12EventParquetBatch(sourcePath, secondBatchRows)
              query.processAllAvailable()
              collectEvidence(query)
            } finally {
              query.stop()
            }

            val outputRows = spark.table(queryName).collect().toSeq
            val nativeNodes = nativeStatelessCounts.sum + nativeJoinCounts.sum
            val rowToColumnarNodes = rowToColumnarCounts.sum
            val columnarToRowNodes = columnarToRowCounts.sum
            val nativeFileScanNodes = nativeFileScanCounts.sum
            result = Q12Evidence(
              run = currentRunLabel,
              scenario = "converge-filesource-join-heavy",
              scaleRows = scaleRows,
              mode = mode,
              status = "PASS",
              progressBatches = progress.length,
              progressInputRows = progress.map(_.numInputRows).sum,
              expectedSourceRows = firstBatchRows.size + secondBatchRows.size,
              outputRows = outputRows.length,
              outputChecksum = checksumRows(outputRows),
              commitEpochs = committedEpochs(checkpointDir).toSeq.sorted.mkString(","),
              nativePlanNodes = nativeNodes,
              nativeDirectColumnarStatefulNodes = 0,
              nativeDirectColumnarCountNodes = 0,
              nativeDirectColumnarLongSumNodes = nativeJoinCounts.sum,
              nativeRowInputStatefulNodes = 0,
              rowToColumnarNodes = rowToColumnarNodes,
              columnarToRowNodes = columnarToRowNodes,
              sparkOwnedSourceNodes = nativeFileScanNodes,
              nativePlanNodeShare = planNodeShare(
                nativeNodes,
                rowToColumnarNodes + columnarToRowNodes),
              maxBatchDurationMs = maxBatchDurationMs(progress),
              maxTriggerExecutionMs = maxDurationMs(progress, "triggerExecution"),
              maxAddBatchMs = maxDurationMs(progress, "addBatch"),
              maxQueryPlanningMs = maxDurationMs(progress, "queryPlanning"),
              maxGetBatchMs = maxDurationMs(progress, "getBatch"),
              maxLatestOffsetMs = maxDurationMs(progress, "latestOffset"),
              maxWalCommitMs = maxDurationMs(progress, "walCommit"),
              maxCommitOffsetsMs = maxDurationMs(progress, "commitOffsets"),
              avgProcessedRowsPerSecond = avgProcessedRowsPerSecond(progress),
              throughputSampleCount = progress.length
            )
            spark.catalog.dropTempView(queryName)
        }
    }
    assert(result != null)
    result
  }

  private def convergeNativeFileScanNodeCount(plan: SparkPlan): Int =
    flattenPlan(plan).count(_.isInstanceOf[FileSourceScanExecTransformer])

  private def convergeVanillaFileScanNodeCount(plan: SparkPlan): Int =
    flattenPlan(plan).count(_.isInstanceOf[FileSourceScanExec])

  private def assertNativeConvergeJoinPlan(plan: SparkPlan): Unit = {
    assert(
      convergeNativeFileScanNodeCount(plan) > 0,
      s"Expected native FileSourceScanExecTransformer to own the stream scan:\n$plan")
    assert(
      convergeVanillaFileScanNodeCount(plan) == 0,
      s"Expected no vanilla FileSourceScanExec in native converged stream:\n$plan")
    assert(
      nativeBroadcastJoinNodeCount(plan) + nativeShuffledJoinNodeCount(plan) > 0,
      s"Expected native join transformer in converged stream:\n$plan")
    assert(
      vanillaBroadcastJoinNodeCount(plan) == 0,
      s"Expected no vanilla BroadcastHashJoinExec in native converged stream:\n$plan")
    assert(
      computeJoinNativeStatelessNodeCount(plan) > 0,
      s"Expected native stateless project/filter in converged stream:\n$plan")
  }

  // Build the HEAVY-compute result DataFrame: a Parquet FILE-source stream filtered, inner-joined to
  // the broadcast static dim table, then a VERY heavy per-row projection. Dozens of derived columns
  // exercising arithmetic chains, sqrt/pow/log/exp, decimal math, date arithmetic, string ops
  // (substring/concat/upper/lower/length/lpad/rpad/reverse/translate/regexp_extract), nested
  // CASE WHEN conditionals, and hash/md5. No groupBy/aggregate => no state store. This is the same
  // native island as convergeFileSourceJoinResultDf (file scan + broadcast join + project) but with
  // far more vectorizable expression work to widen the native-vs-vanilla gap.
  private def heavyComputeJoinResultDf(sourcePath: String) = {
    val streamDf = spark.readStream
      .schema(q12EventSchema)
      .parquet(sourcePath)
      .where(
        $"l_shipmode".isin("MAIL", "SHIP") &&
          $"l_commitdate" < $"l_receiptdate" &&
          $"l_shipdate" < $"l_commitdate" &&
          $"l_receiptdate" >= q12StartReceiptDate &&
          $"l_receiptdate" < q12EndReceiptDate)

    streamDf
      .join(broadcast(shipModeDimDf), $"l_shipmode" === $"dim_shipmode", "inner")
      .select(
        $"l_orderkey",
        $"l_linenumber",
        $"dim_carrier_class",
        // --- Arithmetic chains -------------------------------------------------------------
        expr(
          "cast(round((l_orderkey % 100000) * dim_rate_multiplier " +
            "+ l_linenumber * dim_priority_weight " +
            "+ datediff(l_receiptdate, l_shipdate) * 1.5 " +
            "- datediff(l_receiptdate, l_commitdate) * 0.75, 2) as double)").as("freight_cost"),
        expr(
          "cast(round(sqrt(abs((l_orderkey % 50000) * 1.0)) * dim_rate_multiplier " +
            "+ pow(l_linenumber % 97 + 1, 1.5) " +
            "- log(2.0, (l_orderkey % 9973) + 2.0) * dim_priority_weight, 4) as double)")
          .as("risk_score"),
        expr(
          "cast(round(exp(least(greatest((l_orderkey % 13) * 0.1, 0.0), 4.0)) " +
            "+ sin((l_orderkey % 360) * 0.01745) * 10.0 " +
            "+ cos((l_linenumber % 360) * 0.01745) * 5.0, 5) as double)").as("trig_score"),
        expr(
          "cast(round((l_orderkey % 100000) * dim_rate_multiplier / " +
            "(1.0 + abs(datediff(l_receiptdate, l_shipdate))) " +
            "+ pow(dim_priority_weight, 2) - sqrt((l_linenumber % 311) + 1), 3) as double)")
          .as("density_index"),
        // --- Decimal math ------------------------------------------------------------------
        expr(
          "cast((l_orderkey % 100000) as decimal(18,4)) * " +
            "cast(dim_rate_multiplier as decimal(8,4)) " +
            "+ cast(l_linenumber as decimal(18,4)) * cast(1.075 as decimal(8,4))")
          .as("billed_amount"),
        expr(
          "cast(round(cast((l_orderkey % 100000) as decimal(18,4)) " +
            "* cast(0.0825 as decimal(8,4)), 4) as decimal(20,4))").as("tax_amount"),
        // --- Date arithmetic ---------------------------------------------------------------
        expr("datediff(l_receiptdate, l_shipdate)").as("transit_days"),
        expr("date_add(l_receiptdate, (l_linenumber % 30) + 1)").as("sla_due_date"),
        expr(
          "cast(months_between(l_receiptdate, l_shipdate) as int)").as("transit_months"),
        expr("dayofweek(l_receiptdate)").as("receipt_dow"),
        expr("weekofyear(l_shipdate)").as("ship_week"),
        // --- Conditional / nested CASE -----------------------------------------------------
        expr(
          "case when datediff(l_receiptdate, l_commitdate) > 30 then 'LATE' " +
            "when datediff(l_receiptdate, l_commitdate) > 7 then 'DELAYED' " +
            "when datediff(l_receiptdate, l_shipdate) > 14 then 'SLOW_SHIP' " +
            "else 'ON_TIME' end").as("delivery_bucket"),
        expr(
          "case when (l_orderkey % 100000) * dim_rate_multiplier > 50000 then 'TIER_A' " +
            "when (l_orderkey % 100000) * dim_rate_multiplier > 10000 then 'TIER_B' " +
            "when (l_orderkey % 100000) * dim_rate_multiplier > 1000 then 'TIER_C' " +
            "else 'TIER_D' end").as("value_tier"),
        expr(
          "case when l_shipmode = 'MAIL' and dim_carrier_class = 'GROUND' then 0 " +
            "when l_shipmode = 'SHIP' and dim_carrier_class = 'OCEAN' then 1 " +
            "when dim_priority_weight >= 7 then 2 else 3 end").as("routing_code"),
        // --- String ops --------------------------------------------------------------------
        expr(
          "concat(upper(dim_carrier_class), '-', substring(o_orderpriority, 4, 20), '-', " +
            "lpad(cast(l_orderkey % 1000 as string), 4, '0'))").as("route_label"),
        expr(
          "concat(lower(substring(l_shipmode, 1, 2)), '_', " +
            "cast(length(o_orderpriority) as string), '_', " +
            "reverse(cast(l_linenumber % 1000 as string)))").as("ship_tag"),
        expr(
          "rpad(translate(upper(o_orderpriority), '-ABCDEFGHIJKLMNOPQRSTUVWXYZ ', " +
            "'_abcdefghijklmnopqrstuvwxyz0'), 24, '*')").as("normalized_priority"),
        expr(
          "regexp_extract(o_orderpriority, '([0-9]+)', 1)").as("priority_rank"),
        expr(
          "concat_ws('|', upper(l_shipmode), dim_carrier_class, " +
            "substring(o_orderpriority, 1, 8), cast(l_orderkey % 97 as string))")
          .as("composite_key"),
        expr(
          "cast(length(concat(o_orderpriority, l_shipmode, dim_carrier_class)) as int)")
          .as("label_len"),
        // --- Hash / digest -----------------------------------------------------------------
        expr(
          "cast((l_orderkey * 31 + l_linenumber * 17) % 9973 as int)").as("hash_bucket"),
        expr(
          "cast((l_orderkey * 1009 + l_linenumber * 131 + " +
            "datediff(l_receiptdate, l_shipdate) * 7) % 100003 as int)").as("hash_bucket_2"),
        expr("hash(l_orderkey, l_linenumber, l_shipmode, o_orderpriority)").as("spark_hash"),
        expr(
          "substring(md5(concat(cast(l_orderkey as string), '-', l_shipmode, '-', " +
            "o_orderpriority)), 1, 16)").as("md5_prefix"),
        expr(
          "crc32(concat(cast(l_orderkey as string), cast(l_linenumber as string)))")
          .as("crc_value"))
  }

  // Mutable driver-side accumulator for the NON-ACCUMULATING sink: tracks a running row count and a
  // running XOR-folded + summed checksum over each batch's per-row hashes. Never retains rows.
  private class HeavyComputeRunningState {
    var rowCount: Long = 0L
    var xorChecksum: Long = 0L
    var sumChecksum: Long = 0L
  }

  private def runHeavyComputeJoinScenario(
      mode: String,
      expectNative: Boolean,
      firstBatchRows: Seq[Q12EventRow],
      secondBatchRows: Seq[Q12EventRow],
      scaleRows: Int): Q12Evidence = {
    var result: Q12Evidence = null
    withTempDir {
      sourceDir =>
        withTempDir {
          checkpointDir =>
            val sourcePath = sourceDir.getCanonicalPath
            val checkpointPath = checkpointDir.getCanonicalPath
            val queryName =
              s"gluten_tpch_heavycompute_${checkpointDir.getName}".replaceAll("\\W", "_")
            val progress = ArrayBuffer.empty[StreamingQueryProgress]
            val nativeStatelessCounts = ArrayBuffer.empty[Int]
            val nativeJoinCounts = ArrayBuffer.empty[Int]
            val nativeFileScanCounts = ArrayBuffer.empty[Int]
            val rowToColumnarCounts = ArrayBuffer.empty[Int]
            val columnarToRowCounts = ArrayBuffer.empty[Int]
            val seenProgressBatchIds = LinkedHashSet.empty[Long]
            val runningState = new HeavyComputeRunningState
            val resultDf = heavyComputeJoinResultDf(sourcePath)

            // NON-ACCUMULATING sink: for each micro-batch, fold the heavy projection into a single
            // (count, xorHash, sumHash) row via an aggregate. The aggregate returns ONE row so no
            // output rows are retained on the driver, and 10M output rows never materialize. The
            // batch DataFrame is still the full native island (file scan + join + heavy project);
            // the agg + collect of the single summary row is the only added work.
            def startQuery(): StreamingQuery = {
              resultDf.writeStream
                .queryName(queryName)
                .outputMode("append")
                .option("checkpointLocation", checkpointPath)
                .foreachBatch {
                  (batchDf: org.apache.spark.sql.DataFrame, _: Long) =>
                    // Per-row stable hash over a representative subset of derived columns. xxhash64
                    // is deterministic and identical between native (Velox) and vanilla, so the
                    // XOR/SUM folds match across engines for the same logical rows.
                    val perRow = org.apache.spark.sql.functions.xxhash64(
                      batchDf("l_orderkey"),
                      batchDf("l_linenumber"),
                      batchDf("freight_cost"),
                      batchDf("delivery_bucket"),
                      batchDf("route_label"),
                      batchDf("hash_bucket"),
                      batchDf("spark_hash"),
                      batchDf("md5_prefix"))
                    val summary = batchDf
                      .agg(
                        org.apache.spark.sql.functions.count(org.apache.spark.sql.functions.lit(1))
                          .as("cnt"),
                        org.apache.spark.sql.functions
                          .bit_xor(perRow)
                          .as("xor_hash"),
                        org.apache.spark.sql.functions
                          .sum(perRow)
                          .as("sum_hash"))
                      .collect()
                    val row = summary.headOption
                    val batchCount = row.map(_.getLong(0)).getOrElse(0L)
                    val batchXor = row.flatMap(r => Option(r.get(1)).map(_ => r.getLong(1)))
                      .getOrElse(0L)
                    val batchSum = row.flatMap(r => Option(r.get(2)).map(_ => r.getLong(2)))
                      .getOrElse(0L)
                    runningState.rowCount += batchCount
                    runningState.xorChecksum ^= batchXor
                    runningState.sumChecksum += batchSum
                }
                .start()
            }

            def collectEvidence(query: StreamingQuery): Unit = {
              val meaningfulProgress = query.recentProgress.filter {
                p =>
                  p.numInputRows > 0L || Option(p.sink).exists(_.numOutputRows > 0L)
              }.filterNot(p => seenProgressBatchIds.contains(p.batchId))
              if (meaningfulProgress.nonEmpty) {
                progress ++= meaningfulProgress
                seenProgressBatchIds ++= meaningfulProgress.map(_.batchId)
              } else {
                Option(query.lastProgress)
                  .filterNot(p => seenProgressBatchIds.contains(p.batchId))
                  .foreach {
                    p =>
                      progress += p
                      seenProgressBatchIds += p.batchId
                  }
              }
              val plan = executedPlan(query)
              nativeStatelessCounts += computeJoinNativeStatelessNodeCount(plan)
              nativeJoinCounts += nativeBroadcastJoinNodeCount(plan) +
                nativeShuffledJoinNodeCount(plan)
              nativeFileScanCounts += convergeNativeFileScanNodeCount(plan)
              rowToColumnarCounts += computeJoinRowToColumnarNodeCount(plan)
              columnarToRowCounts += computeJoinColumnarToRowNodeCount(plan)
              if (expectNative) {
                assertNativeConvergeJoinPlan(plan)
              } else {
                assertNoNativeComputeJoinPlan(plan)
              }
            }

            val query = startQuery()
            try {
              writeQ12EventParquetBatch(sourcePath, firstBatchRows)
              query.processAllAvailable()
              collectEvidence(query)

              writeQ12EventParquetBatch(sourcePath, secondBatchRows)
              query.processAllAvailable()
              collectEvidence(query)
            } finally {
              query.stop()
            }

            val nativeNodes = nativeStatelessCounts.sum + nativeJoinCounts.sum
            val rowToColumnarNodes = rowToColumnarCounts.sum
            val columnarToRowNodes = columnarToRowCounts.sum
            val nativeFileScanNodes = nativeFileScanCounts.sum
            // Encode the running checksum (count|xor|sum) as the output checksum field so
            // native-vs-vanilla parity compares the running row count and running hash folds.
            val runningChecksum =
              s"${runningState.rowCount}:${runningState.xorChecksum}:${runningState.sumChecksum}"
            result = Q12Evidence(
              run = currentRunLabel,
              scenario = "heavy-compute-join",
              scaleRows = scaleRows,
              mode = mode,
              status = "PASS",
              progressBatches = progress.length,
              progressInputRows = progress.map(_.numInputRows).sum,
              expectedSourceRows = firstBatchRows.size + secondBatchRows.size,
              outputRows = runningState.rowCount,
              outputChecksum = runningChecksum,
              commitEpochs = committedEpochs(checkpointDir).toSeq.sorted.mkString(","),
              nativePlanNodes = nativeNodes,
              nativeDirectColumnarStatefulNodes = 0,
              nativeDirectColumnarCountNodes = 0,
              nativeDirectColumnarLongSumNodes = nativeJoinCounts.sum,
              nativeRowInputStatefulNodes = 0,
              rowToColumnarNodes = rowToColumnarNodes,
              columnarToRowNodes = columnarToRowNodes,
              sparkOwnedSourceNodes = nativeFileScanNodes,
              nativePlanNodeShare = planNodeShare(
                nativeNodes,
                rowToColumnarNodes + columnarToRowNodes),
              maxBatchDurationMs = maxBatchDurationMs(progress),
              maxTriggerExecutionMs = maxDurationMs(progress, "triggerExecution"),
              maxAddBatchMs = maxDurationMs(progress, "addBatch"),
              maxQueryPlanningMs = maxDurationMs(progress, "queryPlanning"),
              maxGetBatchMs = maxDurationMs(progress, "getBatch"),
              maxLatestOffsetMs = maxDurationMs(progress, "latestOffset"),
              maxWalCommitMs = maxDurationMs(progress, "walCommit"),
              maxCommitOffsetsMs = maxDurationMs(progress, "commitOffsets"),
              avgProcessedRowsPerSecond = avgProcessedRowsPerSecond(progress),
              throughputSampleCount = progress.length
            )
        }
    }
    assert(result != null)
    result
  }

  private def assertHeavyComputeEvidenceParity(
      nativeEvidence: Q12Evidence,
      vanillaEvidence: Q12Evidence): Unit = {
    assert(
      nativeEvidence.outputRows == vanillaEvidence.outputRows,
      s"Native and vanilla heavy-compute running row counts diverged: " +
        s"native=${nativeEvidence.outputRows}, vanilla=${vanillaEvidence.outputRows}"
    )
    assert(
      nativeEvidence.outputChecksum == vanillaEvidence.outputChecksum,
      s"Native and vanilla heavy-compute running checksums diverged: " +
        s"native=${nativeEvidence.outputChecksum}, vanilla=${vanillaEvidence.outputChecksum}"
    )
    assert(
      nativeEvidence.progressInputRows == nativeEvidence.expectedSourceRows,
      s"Native heavy-compute progress input rows should match deterministic source rows: " +
        s"progress=${nativeEvidence.progressInputRows}, " +
        s"expected=${nativeEvidence.expectedSourceRows}"
    )
    assert(
      nativeEvidence.nativePlanNodes > 0,
      s"Expected native heavy-compute run to contain native nodes: $nativeEvidence")
    assert(
      vanillaEvidence.nativePlanNodes == 0,
      s"Expected vanilla heavy-compute run to avoid native nodes: $vanillaEvidence")
  }

  // ===========================================================================================
  // RIGOROUS BENCHMARK helpers
  // ===========================================================================================

  // The ordered set of HEAVY derived columns, reused from heavyComputeJoinResultDf. light/medium/
  // heavy compute levels take a PREFIX of this list (3 / 12 / all). Same join + file source for all
  // levels; only the projection width changes.
  private def rigorDerivedColumns: Seq[org.apache.spark.sql.Column] = Seq(
    // --- Arithmetic chains (light = first 3) -------------------------------------------------
    expr(
      "cast(round((l_orderkey % 100000) * dim_rate_multiplier " +
        "+ l_linenumber * dim_priority_weight " +
        "+ datediff(l_receiptdate, l_shipdate) * 1.5 " +
        "- datediff(l_receiptdate, l_commitdate) * 0.75, 2) as double)").as("freight_cost"),
    expr(
      "cast(round(sqrt(abs((l_orderkey % 50000) * 1.0)) * dim_rate_multiplier " +
        "+ pow(l_linenumber % 97 + 1, 1.5) " +
        "- log(2.0, (l_orderkey % 9973) + 2.0) * dim_priority_weight, 4) as double)")
      .as("risk_score"),
    expr(
      "case when datediff(l_receiptdate, l_commitdate) > 30 then 'LATE' " +
        "when datediff(l_receiptdate, l_commitdate) > 7 then 'DELAYED' " +
        "when datediff(l_receiptdate, l_shipdate) > 14 then 'SLOW_SHIP' " +
        "else 'ON_TIME' end").as("delivery_bucket"),
    // --- medium adds up to 12 ----------------------------------------------------------------
    expr(
      "cast(round(exp(least(greatest((l_orderkey % 13) * 0.1, 0.0), 4.0)) " +
        "+ sin((l_orderkey % 360) * 0.01745) * 10.0 " +
        "+ cos((l_linenumber % 360) * 0.01745) * 5.0, 5) as double)").as("trig_score"),
    expr(
      "cast(round((l_orderkey % 100000) * dim_rate_multiplier / " +
        "(1.0 + abs(datediff(l_receiptdate, l_shipdate))) " +
        "+ pow(dim_priority_weight, 2) - sqrt((l_linenumber % 311) + 1), 3) as double)")
      .as("density_index"),
    expr(
      "cast((l_orderkey % 100000) as decimal(18,4)) * " +
        "cast(dim_rate_multiplier as decimal(8,4)) " +
        "+ cast(l_linenumber as decimal(18,4)) * cast(1.075 as decimal(8,4))")
      .as("billed_amount"),
    expr("datediff(l_receiptdate, l_shipdate)").as("transit_days"),
    expr("date_add(l_receiptdate, (l_linenumber % 30) + 1)").as("sla_due_date"),
    expr(
      "case when (l_orderkey % 100000) * dim_rate_multiplier > 50000 then 'TIER_A' " +
        "when (l_orderkey % 100000) * dim_rate_multiplier > 10000 then 'TIER_B' " +
        "when (l_orderkey % 100000) * dim_rate_multiplier > 1000 then 'TIER_C' " +
        "else 'TIER_D' end").as("value_tier"),
    expr(
      "concat(upper(dim_carrier_class), '-', substring(o_orderpriority, 4, 20), '-', " +
        "lpad(cast(l_orderkey % 1000 as string), 4, '0'))").as("route_label"),
    expr(
      "concat(lower(substring(l_shipmode, 1, 2)), '_', " +
        "cast(length(o_orderpriority) as string), '_', " +
        "reverse(cast(l_linenumber % 1000 as string)))").as("ship_tag"),
    expr(
      "cast((l_orderkey * 31 + l_linenumber * 17) % 9973 as int)").as("hash_bucket"),
    // --- heavy adds the remaining (up to ~30) ------------------------------------------------
    expr(
      "cast(months_between(l_receiptdate, l_shipdate) as int)").as("transit_months"),
    expr("dayofweek(l_receiptdate)").as("receipt_dow"),
    expr("weekofyear(l_shipdate)").as("ship_week"),
    expr(
      "cast(round(cast((l_orderkey % 100000) as decimal(18,4)) " +
        "* cast(0.0825 as decimal(8,4)), 4) as decimal(20,4))").as("tax_amount"),
    expr(
      "case when l_shipmode = 'MAIL' and dim_carrier_class = 'GROUND' then 0 " +
        "when l_shipmode = 'SHIP' and dim_carrier_class = 'OCEAN' then 1 " +
        "when dim_priority_weight >= 7 then 2 else 3 end").as("routing_code"),
    expr(
      "rpad(translate(upper(o_orderpriority), '-ABCDEFGHIJKLMNOPQRSTUVWXYZ ', " +
        "'_abcdefghijklmnopqrstuvwxyz0'), 24, '*')").as("normalized_priority"),
    expr(
      "regexp_extract(o_orderpriority, '([0-9]+)', 1)").as("priority_rank"),
    expr(
      "concat_ws('|', upper(l_shipmode), dim_carrier_class, " +
        "substring(o_orderpriority, 1, 8), cast(l_orderkey % 97 as string))")
      .as("composite_key"),
    expr(
      "cast(length(concat(o_orderpriority, l_shipmode, dim_carrier_class)) as int)")
      .as("label_len"),
    expr(
      "cast((l_orderkey * 1009 + l_linenumber * 131 + " +
        "datediff(l_receiptdate, l_shipdate) * 7) % 100003 as int)").as("hash_bucket_2"),
    expr("hash(l_orderkey, l_linenumber, l_shipmode, o_orderpriority)").as("spark_hash"),
    expr(
      "substring(md5(concat(cast(l_orderkey as string), '-', l_shipmode, '-', " +
        "o_orderpriority)), 1, 16)").as("md5_prefix"),
    expr(
      "crc32(concat(cast(l_orderkey as string), cast(l_linenumber as string)))")
      .as("crc_value"),
    expr(
      "cast(round((l_orderkey % 100000) * dim_rate_multiplier " +
        "- l_linenumber * dim_priority_weight, 2) as double)").as("net_margin"),
    expr(
      "concat(substring(reverse(o_orderpriority), 1, 4), '#', " +
        "cast((l_orderkey % 7919) as string))").as("scramble_tag"),
    expr(
      "case when (l_orderkey + l_linenumber) % 3 = 0 then 'A' " +
        "when (l_orderkey + l_linenumber) % 3 = 1 then 'B' else 'C' end").as("rr_class"),
    expr(
      "cast(abs(hash(o_orderpriority, l_shipmode)) % 1000 as int)").as("priority_hash"))

  private def rigorColumnCountFor(level: String): Int = level match {
    case "light" => 3
    case "medium" => 12
    case "heavy" => rigorDerivedColumns.length
    case other => fail(s"Unsupported rigor compute level: $other")
  }

  // Build the rigor result DataFrame: a Parquet FILE-source stream filtered, inner-joined to the
  // broadcast static dim, then a projection of the first N derived columns (N set by compute level).
  // maxFilesPerTrigger=1 makes each Parquet part-file its own micro-batch.
  private def rigorResultDf(sourcePath: String, computeLevel: String) = {
    val n = rigorColumnCountFor(computeLevel)
    val streamDf = spark.readStream
      .schema(q12EventSchema)
      .option("maxFilesPerTrigger", "1")
      .parquet(sourcePath)
      .where(
        $"l_shipmode".isin("MAIL", "SHIP") &&
          $"l_commitdate" < $"l_receiptdate" &&
          $"l_shipdate" < $"l_commitdate" &&
          $"l_receiptdate" >= q12StartReceiptDate &&
          $"l_receiptdate" < q12EndReceiptDate)

    val baseCols = Seq($"l_orderkey", $"l_linenumber", $"dim_carrier_class")
    streamDf
      .join(broadcast(shipModeDimDf), $"l_shipmode" === $"dim_shipmode", "inner")
      .select((baseCols ++ rigorDerivedColumns.take(n)): _*)
  }

  private case class RigorResult(
      engine: String,
      computeLevel: String,
      scaleRows: Int,
      repeatIndex: Int,
      derivedColumns: Int,
      steadyStateRowsPerSecond: Double,
      steadyStateBatchCount: Int,
      totalBatchCount: Int,
      maxAddBatchMs: Long,
      maxTriggerExecutionMs: Long,
      runningCount: Long,
      runningChecksum: String,
      nativePlanNodes: Int)

  // Run ONE repeat of the rigor scenario for the given engine. Materializes the source rows as
  // RIGOR_PART_FILES Parquet part-files BEFORE starting the query so that, with maxFilesPerTrigger=1,
  // the stream processes them as that many micro-batches in a single processAllAvailable(). Steady-
  // state throughput is the mean processedRowsPerSecond over batches EXCLUDING batch 0.
  private def runRigorScenario(
      engine: String,
      expectNative: Boolean,
      computeLevel: String,
      rows: Seq[Q12EventRow],
      scaleRows: Int,
      repeatIndex: Int): RigorResult = {
    var result: RigorResult = null
    withTempDir {
      sourceDir =>
        withTempDir {
          checkpointDir =>
            val sourcePath = sourceDir.getCanonicalPath
            val checkpointPath = checkpointDir.getCanonicalPath
            val queryName =
              s"gluten_tpch_rigor_${checkpointDir.getName}".replaceAll("\\W", "_")
            val runningState = new HeavyComputeRunningState

            // Materialize the input as RIGOR_PART_FILES part-files up front. Each coalesce(1).append
            // write produces one Parquet file, so the streaming file source with maxFilesPerTrigger=1
            // sees one batch per part-file (the first being the dropped warmup batch).
            val parts = rigorPartFiles
            val chunkSize = math.max(1, (rows.length + parts - 1) / parts)
            rows.grouped(chunkSize).foreach(chunk => writeQ12EventParquetBatch(sourcePath, chunk))

            val resultDf = rigorResultDf(sourcePath, computeLevel)

            def startQuery(): StreamingQuery = {
              resultDf.writeStream
                .queryName(queryName)
                .outputMode("append")
                .option("checkpointLocation", checkpointPath)
                .foreachBatch {
                  (batchDf: org.apache.spark.sql.DataFrame, _: Long) =>
                    // Stable per-row xxhash64 over a fixed subset present at EVERY compute level
                    // (the base cols + the always-present first derived column freight_cost), so the
                    // running folds are identical between native and vanilla for the same logical
                    // rows. Deterministic across engines.
                    val perRow = org.apache.spark.sql.functions.xxhash64(
                      batchDf("l_orderkey"),
                      batchDf("l_linenumber"),
                      batchDf("dim_carrier_class"),
                      batchDf("freight_cost"))
                    val summary = batchDf
                      .agg(
                        org.apache.spark.sql.functions.count(org.apache.spark.sql.functions.lit(1))
                          .as("cnt"),
                        org.apache.spark.sql.functions.bit_xor(perRow).as("xor_hash"),
                        org.apache.spark.sql.functions.sum(perRow).as("sum_hash"))
                      .collect()
                    val row = summary.headOption
                    val batchCount = row.map(_.getLong(0)).getOrElse(0L)
                    val batchXor =
                      row.flatMap(r => Option(r.get(1)).map(_ => r.getLong(1))).getOrElse(0L)
                    val batchSum =
                      row.flatMap(r => Option(r.get(2)).map(_ => r.getLong(2))).getOrElse(0L)
                    runningState.rowCount += batchCount
                    runningState.xorChecksum ^= batchXor
                    runningState.sumChecksum += batchSum
                }
                .start()
            }

            val query = startQuery()
            var nativePlanNodes = 0
            try {
              query.processAllAvailable()
              val plan = executedPlan(query)
              nativePlanNodes = computeJoinNativeStatelessNodeCount(plan) +
                nativeBroadcastJoinNodeCount(plan) + nativeShuffledJoinNodeCount(plan)
              if (expectNative) {
                assertNativeConvergeJoinPlan(plan)
              } else {
                assertNoNativeComputeJoinPlan(plan)
              }
            } finally {
              query.stop()
            }

            // STEADY-STATE: drop batch 0 (warmup). recentProgress is ordered by batchId; keep only
            // batches that actually processed input rows, then exclude the lowest batchId.
            val realProgress = query.recentProgress.toSeq
              .filter(_.numInputRows > 0L)
              .sortBy(_.batchId)
            val steadyProgress =
              if (realProgress.length > 1) realProgress.drop(1) else realProgress
            val steadyRowsPerSecond =
              if (steadyProgress.isEmpty) 0.0
              else steadyProgress.map(_.processedRowsPerSecond).sum / steadyProgress.length
            val runningChecksum =
              s"${runningState.rowCount}:${runningState.xorChecksum}:${runningState.sumChecksum}"

            result = RigorResult(
              engine = engine,
              computeLevel = computeLevel,
              scaleRows = scaleRows,
              repeatIndex = repeatIndex,
              derivedColumns = rigorColumnCountFor(computeLevel),
              steadyStateRowsPerSecond = steadyRowsPerSecond,
              steadyStateBatchCount = steadyProgress.length,
              totalBatchCount = realProgress.length,
              maxAddBatchMs = maxDurationMs(steadyProgress, "addBatch"),
              maxTriggerExecutionMs = maxDurationMs(steadyProgress, "triggerExecution"),
              runningCount = runningState.rowCount,
              runningChecksum = runningChecksum,
              nativePlanNodes = nativePlanNodes)
        }
    }
    assert(result != null)
    result
  }

  private def rigorComputeLevel: String =
    sys.env
      .get("GLUTEN_NATIVE_STREAMING_TPCH_COMPUTE_LEVEL")
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .getOrElse("heavy")

  private def rigorRepeats: Int =
    sys.env
      .get("GLUTEN_NATIVE_STREAMING_TPCH_REPEATS")
      .filter(_.nonEmpty)
      .map {
        value =>
          assert(
            value.matches("[0-9]+") && value.toInt >= 1,
            s"GLUTEN_NATIVE_STREAMING_TPCH_REPEATS must be a positive integer: $value")
          value.toInt
      }
      .getOrElse(5)

  private def rigorPartFiles: Int =
    sys.env
      .get("GLUTEN_NATIVE_STREAMING_TPCH_PART_FILES")
      .filter(_.nonEmpty)
      .map {
        value =>
          assert(
            value.matches("[0-9]+") && value.toInt >= 2,
            s"GLUTEN_NATIVE_STREAMING_TPCH_PART_FILES must be an integer >= 2: $value")
          value.toInt
      }
      .getOrElse(6)

  private def rigorReportPath: String =
    sys.env
      .get("GLUTEN_NATIVE_STREAMING_TPCH_RIGOR_REPORT")
      .filter(_.nonEmpty)
      .getOrElse(new File(new File(sys.props.getOrElse("user.dir", ".")), "target/rigor-bench.tsv")
        .getCanonicalPath)

  private def appendRigorResults(reportPath: String, results: Seq[RigorResult]): Unit = {
    val file = new File(reportPath)
    Option(file.getParentFile).foreach(_.mkdirs())
    val shouldWriteHeader = !file.exists() || file.length() == 0L
    val writer = new BufferedWriter(new FileWriter(file, true))
    try {
      if (shouldWriteHeader) {
        writer.write(
          "engine\tcompute_level\tscale_rows\trepeat_index\tderived_columns\t" +
            "steady_state_rows_per_second\tsteady_state_batch_count\ttotal_batch_count\t" +
            "max_add_batch_ms\tmax_trigger_execution_ms\trunning_count\trunning_checksum\t" +
            "native_plan_nodes")
        writer.newLine()
      }
      results.foreach {
        r =>
          writer.write(
            Seq(
              r.engine,
              r.computeLevel,
              r.scaleRows.toString,
              r.repeatIndex.toString,
              r.derivedColumns.toString,
              f"${r.steadyStateRowsPerSecond}%.2f",
              r.steadyStateBatchCount.toString,
              r.totalBatchCount.toString,
              r.maxAddBatchMs.toString,
              r.maxTriggerExecutionMs.toString,
              r.runningCount.toString,
              r.runningChecksum,
              r.nativePlanNodes.toString).mkString("\t"))
          writer.newLine()
      }
    } finally {
      writer.close()
    }
  }

  // ===========================================================================================
  // RIGOROUS STATEFUL BENCHMARK helpers
  // ===========================================================================================

  private case class StatefulRigorResult(
      engine: String,
      scaleRows: Int,
      repeatIndex: Int,
      steadyStateRowsPerSecond: Double,
      steadyStateBatchCount: Int,
      totalBatchCount: Int,
      maxAddBatchMs: Long,
      maxTriggerExecutionMs: Long,
      outputRowCount: Long,
      outputChecksum: String,
      nativePlanNodes: Int)

  // Runs ONE repeat of the high-cardinality GROUP BY l_orderkey, SUM(l_orderkey) Update-mode
  // streaming aggregate for the given engine. Splits the source rows into rigorPartFiles
  // MemoryStream micro-batches; batch 0 is the dropped warmup, steady-state throughput is the mean
  // processedRowsPerSecond over the remaining batches.
  private def runStatefulRigorScenario(
      engine: String,
      expectNative: Boolean,
      rows: Seq[Q12EventRow],
      scaleRows: Int,
      repeatIndex: Int): StatefulRigorResult = {
    var result: StatefulRigorResult = null
    withTempDir {
      checkpointDir =>
        val inputData = MemoryStream[Q12EventRow]
        val checkpointPath = checkpointDir.getCanonicalPath
        val queryName =
          s"gluten_tpch_stateful_rigor_${checkpointDir.getName}".replaceAll("\\W", "_")
        val q12FilteredRows = inputData
          .toDF()
          .toDF(
            "l_orderkey",
            "l_linenumber",
            "l_shipmode",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "o_orderpriority")
          .where(
            $"l_shipmode".isin("MAIL", "SHIP") &&
              $"l_commitdate" < $"l_receiptdate" &&
              $"l_shipdate" < $"l_commitdate" &&
              $"l_receiptdate" >= q12StartReceiptDate &&
              $"l_receiptdate" < q12EndReceiptDate)
        val q12OrderKeySums = q12FilteredRows
          .select($"l_orderkey", $"l_orderkey".as("orderkey_amount"))
          .groupBy($"l_orderkey")
          .agg(sum($"orderkey_amount").as("orderkey_sum"))

        val query = q12OrderKeySums.writeStream
          .format("memory")
          .queryName(queryName)
          .outputMode("update")
          .option("checkpointLocation", checkpointPath)
          .start()
        var nativePlanNodes = 0
        try {
          // Split the rows into rigorPartFiles micro-batches; one addData+processAllAvailable per
          // chunk so each chunk is its own trigger (the first being the dropped warmup batch).
          val parts = rigorPartFiles
          val chunkSize = math.max(1, (rows.length + parts - 1) / parts)
          rows.grouped(chunkSize).foreach {
            chunk =>
              inputData.addData(chunk: _*)
              query.processAllAvailable()
          }
          val plan = executedPlan(query)
          nativePlanNodes = nativeStatelessNodeCount(plan) + nativeStatefulNodeCount(plan)
          if (expectNative) {
            assertNativeQ12LongSumPlan(plan)
          } else {
            assertNoNativeQ12Plan(plan)
          }
        } finally {
          query.stop()
        }

        // STEADY-STATE: drop batch 0 (warmup). recentProgress is ordered by batchId; keep only the
        // batches that processed input rows, then exclude the lowest batchId.
        val realProgress = query.recentProgress.toSeq
          .filter(_.numInputRows > 0L)
          .sortBy(_.batchId)
        val steadyProgress =
          if (realProgress.length > 1) realProgress.drop(1) else realProgress
        val steadyRowsPerSecond =
          if (steadyProgress.isEmpty) 0.0
          else steadyProgress.map(_.processedRowsPerSecond).sum / steadyProgress.length

        val outputRows = spark.table(queryName).collect().toSeq
        result = StatefulRigorResult(
          engine = engine,
          scaleRows = scaleRows,
          repeatIndex = repeatIndex,
          steadyStateRowsPerSecond = steadyRowsPerSecond,
          steadyStateBatchCount = steadyProgress.length,
          totalBatchCount = realProgress.length,
          maxAddBatchMs = maxDurationMs(steadyProgress, "addBatch"),
          maxTriggerExecutionMs = maxDurationMs(steadyProgress, "triggerExecution"),
          outputRowCount = outputRows.length.toLong,
          outputChecksum = checksumRows(outputRows),
          nativePlanNodes = nativePlanNodes)
        spark.catalog.dropTempView(queryName)
    }
    assert(result != null)
    result
  }

  private def statefulRigorReportPath: String =
    sys.env
      .get("GLUTEN_NATIVE_STREAMING_TPCH_STATEFUL_RIGOR_REPORT")
      .filter(_.nonEmpty)
      .getOrElse(
        new File(
          new File(sys.props.getOrElse("user.dir", ".")),
          "target/stateful-clean-bench.tsv").getCanonicalPath)

  private def appendStatefulRigorResults(
      reportPath: String,
      results: Seq[StatefulRigorResult]): Unit = {
    val file = new File(reportPath)
    Option(file.getParentFile).foreach(_.mkdirs())
    val shouldWriteHeader = !file.exists() || file.length() == 0L
    val writer = new BufferedWriter(new FileWriter(file, true))
    try {
      if (shouldWriteHeader) {
        writer.write(
          "engine\tscale_rows\trepeat_index\tsteady_state_rows_per_second\t" +
            "steady_state_batch_count\ttotal_batch_count\tmax_add_batch_ms\t" +
            "max_trigger_execution_ms\toutput_row_count\toutput_checksum\tnative_plan_nodes")
        writer.newLine()
      }
      results.foreach {
        r =>
          writer.write(
            Seq(
              r.engine,
              r.scaleRows.toString,
              r.repeatIndex.toString,
              f"${r.steadyStateRowsPerSecond}%.2f",
              r.steadyStateBatchCount.toString,
              r.totalBatchCount.toString,
              r.maxAddBatchMs.toString,
              r.maxTriggerExecutionMs.toString,
              r.outputRowCount.toString,
              r.outputChecksum,
              r.nativePlanNodes.toString).mkString("\t"))
          writer.newLine()
      }
    } finally {
      writer.close()
    }
  }

  // Native streaming config for the STATELESS compute-bound join probe. Enables columnar
  // broadcast join / exchange / project / filter (disabled by GlutenStreamingTestsTraits base
  // conf) plus the stateless-join streaming gate. No state store, no stateful-aggregation gate.
  private def withNativeComputeJoinStreaming[T](testBody: => T): T = {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_JOIN_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_BROADCAST_JOIN_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_BROADCAST_EXCHANGE_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> (10 * 1024 * 1024).toString,
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      testBody
    }
  }

  // A tiny static dim table keyed by l_shipmode, broadcast into the streaming side.
  private def shipModeDimDf = {
    spark
      .createDataFrame(
        Seq(
          ("MAIL", "GROUND", 3, 1.15),
          ("SHIP", "OCEAN", 7, 1.42),
          ("AIR", "EXPRESS", 1, 2.10),
          ("RAIL", "GROUND", 5, 1.05),
          ("TRUCK", "GROUND", 4, 1.20),
          ("FOB", "OCEAN", 9, 1.33),
          ("REG AIR", "EXPRESS", 2, 1.95)
        ))
      .toDF("dim_shipmode", "dim_carrier_class", "dim_priority_weight", "dim_rate_multiplier")
  }

  // Build the stateless compute-bound result DataFrame: stream filtered, inner-joined to the
  // broadcast dim table, then a heavy projection of arithmetic/string/conditional expressions.
  // No groupBy/aggregate => no state store.
  private def computeJoinResultDf(inputData: MemoryStream[Q12EventRow]) = {
    val streamDf = inputData
      .toDF()
      .toDF(
        "l_orderkey",
        "l_linenumber",
        "l_shipmode",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "o_orderpriority")
      .where(
        $"l_shipmode".isin("MAIL", "SHIP") &&
          $"l_commitdate" < $"l_receiptdate" &&
          $"l_shipdate" < $"l_commitdate" &&
          $"l_receiptdate" >= q12StartReceiptDate &&
          $"l_receiptdate" < q12EndReceiptDate)

    streamDf
      .join(broadcast(shipModeDimDf), $"l_shipmode" === $"dim_shipmode", "inner")
      .select(
        $"l_orderkey",
        $"l_linenumber",
        $"dim_carrier_class",
        // Heavy arithmetic chain over the joined columns.
        expr(
          "cast(round((l_orderkey % 100000) * dim_rate_multiplier " +
            "+ l_linenumber * dim_priority_weight " +
            "+ datediff(l_receiptdate, l_shipdate) * 1.5 " +
            "- datediff(l_receiptdate, l_commitdate) * 0.75, 2) as double)").as("freight_cost"),
        // Conditional / CASE expression.
        expr(
          "case when datediff(l_receiptdate, l_commitdate) > 30 then 'LATE' " +
            "when datediff(l_receiptdate, l_commitdate) > 7 then 'DELAYED' " +
            "else 'ON_TIME' end").as("delivery_bucket"),
        // String expressions: concat / upper / substring.
        expr(
          "concat(upper(dim_carrier_class), '-', substring(o_orderpriority, 4, 20), '-', " +
            "cast(l_orderkey % 1000 as string))").as("route_label"),
        // Nested arithmetic / modulo with a guard.
        expr(
          "cast((l_orderkey * 31 + l_linenumber * 17) % 9973 as int)").as("hash_bucket"))
  }

  private def runComputeJoinScenario(
      mode: String,
      expectNative: Boolean,
      firstBatchRows: Seq[Q12EventRow],
      secondBatchRows: Seq[Q12EventRow],
      scaleRows: Int): Q12Evidence = {
    var result: Q12Evidence = null
    withTempDir {
      checkpointDir =>
        val inputData = MemoryStream[Q12EventRow]
        val checkpointPath = checkpointDir.getCanonicalPath
        val queryName =
          s"gluten_tpch_compute_join_${checkpointDir.getName}".replaceAll("\\W", "_")
        val progress = ArrayBuffer.empty[StreamingQueryProgress]
        val nativeStatelessCounts = ArrayBuffer.empty[Int]
        val nativeJoinCounts = ArrayBuffer.empty[Int]
        val rowToColumnarCounts = ArrayBuffer.empty[Int]
        val columnarToRowCounts = ArrayBuffer.empty[Int]
        val sparkOwnedSourceCounts = ArrayBuffer.empty[Int]
        val seenProgressBatchIds = LinkedHashSet.empty[Long]
        val resultDf = computeJoinResultDf(inputData)

        def startQuery(): StreamingQuery = {
          resultDf.writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .option("checkpointLocation", checkpointPath)
            .start()
        }

        def collectEvidence(query: StreamingQuery): Unit = {
          val meaningfulProgress = query.recentProgress.filter {
            p =>
              p.numInputRows > 0L || Option(p.sink).exists(_.numOutputRows > 0L)
          }.filterNot(p => seenProgressBatchIds.contains(p.batchId))
          if (meaningfulProgress.nonEmpty) {
            progress ++= meaningfulProgress
            seenProgressBatchIds ++= meaningfulProgress.map(_.batchId)
          } else {
            Option(query.lastProgress)
              .filterNot(p => seenProgressBatchIds.contains(p.batchId))
              .foreach {
                p =>
                  progress += p
                  seenProgressBatchIds += p.batchId
              }
          }
          val plan = executedPlan(query)
          nativeStatelessCounts += computeJoinNativeStatelessNodeCount(plan)
          nativeJoinCounts += nativeBroadcastJoinNodeCount(plan) + nativeShuffledJoinNodeCount(plan)
          rowToColumnarCounts += computeJoinRowToColumnarNodeCount(plan)
          columnarToRowCounts += computeJoinColumnarToRowNodeCount(plan)
          sparkOwnedSourceCounts += computeJoinSparkOwnedSourceNodeCount(plan)
          if (expectNative) {
            assertNativeComputeJoinPlan(plan)
          } else {
            assertNoNativeComputeJoinPlan(plan)
          }
        }

        val query = startQuery()
        try {
          inputData.addData(firstBatchRows: _*)
          query.processAllAvailable()
          collectEvidence(query)

          inputData.addData(secondBatchRows: _*)
          query.processAllAvailable()
          collectEvidence(query)
        } finally {
          query.stop()
        }

        val outputRows = spark.table(queryName).collect().toSeq
        val nativeNodes = nativeStatelessCounts.sum + nativeJoinCounts.sum
        val rowToColumnarNodes = rowToColumnarCounts.sum
        val columnarToRowNodes = columnarToRowCounts.sum
        val sparkOwnedSourceNodes = sparkOwnedSourceCounts.sum
        result = Q12Evidence(
          run = currentRunLabel,
          scenario = "compute-join-heavy-expr",
          scaleRows = scaleRows,
          mode = mode,
          status = "PASS",
          progressBatches = progress.length,
          progressInputRows = progress.map(_.numInputRows).sum,
          expectedSourceRows = firstBatchRows.size + secondBatchRows.size,
          outputRows = outputRows.length,
          outputChecksum = checksumRows(outputRows),
          commitEpochs = committedEpochs(checkpointDir).toSeq.sorted.mkString(","),
          nativePlanNodes = nativeNodes,
          nativeDirectColumnarStatefulNodes = 0,
          nativeDirectColumnarCountNodes = 0,
          nativeDirectColumnarLongSumNodes = nativeJoinCounts.sum,
          nativeRowInputStatefulNodes = 0,
          rowToColumnarNodes = rowToColumnarNodes,
          columnarToRowNodes = columnarToRowNodes,
          sparkOwnedSourceNodes = sparkOwnedSourceNodes,
          nativePlanNodeShare = planNodeShare(
            nativeNodes,
            rowToColumnarNodes + columnarToRowNodes + sparkOwnedSourceNodes),
          maxBatchDurationMs = maxBatchDurationMs(progress),
          maxTriggerExecutionMs = maxDurationMs(progress, "triggerExecution"),
          maxAddBatchMs = maxDurationMs(progress, "addBatch"),
          maxQueryPlanningMs = maxDurationMs(progress, "queryPlanning"),
          maxGetBatchMs = maxDurationMs(progress, "getBatch"),
          maxLatestOffsetMs = maxDurationMs(progress, "latestOffset"),
          maxWalCommitMs = maxDurationMs(progress, "walCommit"),
          maxCommitOffsetsMs = maxDurationMs(progress, "commitOffsets"),
          avgProcessedRowsPerSecond = avgProcessedRowsPerSecond(progress),
          throughputSampleCount = progress.length
        )
        spark.catalog.dropTempView(queryName)
    }
    assert(result != null)
    result
  }

  private def assertNativeComputeJoinPlan(plan: SparkPlan): Unit = {
    assert(
      nativeBroadcastJoinNodeCount(plan) + nativeShuffledJoinNodeCount(plan) > 0,
      s"Expected native join transformer in compute-join stream:\n$plan")
    assert(
      vanillaBroadcastJoinNodeCount(plan) == 0,
      s"Expected no vanilla BroadcastHashJoinExec in native compute-join stream:\n$plan")
    assert(
      computeJoinNativeStatelessNodeCount(plan) > 0,
      s"Expected native stateless project/filter in compute-join stream:\n$plan")
  }

  private def assertNoNativeComputeJoinPlan(plan: SparkPlan): Unit = {
    assert(
      computeJoinNativeStatelessNodeCount(plan) +
        nativeBroadcastJoinNodeCount(plan) + nativeShuffledJoinNodeCount(plan) == 0,
      s"Expected vanilla compute-join benchmark to avoid native execution:\n$plan")
  }

  private def assertComputeJoinEvidenceParity(
      nativeEvidence: Q12Evidence,
      vanillaEvidence: Q12Evidence): Unit = {
    assert(
      nativeEvidence.outputChecksum == vanillaEvidence.outputChecksum,
      s"Native and vanilla compute-join output checksums diverged: " +
        s"native=${nativeEvidence.outputChecksum}, vanilla=${vanillaEvidence.outputChecksum}"
    )
    assert(
      nativeEvidence.outputRows == vanillaEvidence.outputRows,
      s"Native and vanilla compute-join row counts diverged: " +
        s"native=${nativeEvidence.outputRows}, vanilla=${vanillaEvidence.outputRows}"
    )
    assert(
      nativeEvidence.progressInputRows == nativeEvidence.expectedSourceRows,
      s"Native compute-join progress input rows should match deterministic source rows: " +
        s"progress=${nativeEvidence.progressInputRows}, " +
        s"expected=${nativeEvidence.expectedSourceRows}"
    )
    assert(
      nativeEvidence.nativePlanNodes > 0,
      s"Expected native compute-join run to contain native nodes: $nativeEvidence")
    assert(
      vanillaEvidence.nativePlanNodes == 0,
      s"Expected vanilla compute-join run to avoid native nodes: $vanillaEvidence")
  }

  // The compute-join plan is AQE-wrapped (AdaptiveSparkPlanExec + BroadcastQueryStage), and a
  // plain plan.collect does not descend into the executed final plan / query stages. Flatten the
  // whole tree (including AQE sub-plans) before counting so the join/exchange nodes are visible.
  private def flattenPlan(plan: SparkPlan): Seq[SparkPlan] = {
    val expanded: Seq[SparkPlan] = plan match {
      case aqe: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec =>
        Seq(aqe.executedPlan)
      case stage: org.apache.spark.sql.execution.adaptive.QueryStageExec =>
        Seq(stage.plan)
      case _ => Seq.empty
    }
    plan +: (plan.children ++ expanded).flatMap(flattenPlan)
  }

  private def computeJoinNativeStatelessNodeCount(plan: SparkPlan): Int =
    flattenPlan(plan).count {
      case _: ProjectExecTransformer => true
      case _: FilterExecTransformer => true
      case _ => false
    }

  private def computeJoinRowToColumnarNodeCount(plan: SparkPlan): Int =
    flattenPlan(plan).count(_.isInstanceOf[RowToVeloxColumnarExec])

  private def computeJoinColumnarToRowNodeCount(plan: SparkPlan): Int =
    flattenPlan(plan).count(_.isInstanceOf[VeloxColumnarToRowExec])

  private def computeJoinSparkOwnedSourceNodeCount(plan: SparkPlan): Int =
    flattenPlan(plan).count {
      case _: SparkOwnedMicroBatchScanExec => true
      case _: MicroBatchScanExec => true
      case _ => false
    }

  private def nativeBroadcastJoinNodeCount(plan: SparkPlan): Int =
    flattenPlan(plan).count(_.isInstanceOf[BroadcastHashJoinExecTransformer])

  private def nativeShuffledJoinNodeCount(plan: SparkPlan): Int =
    flattenPlan(plan).count(_.isInstanceOf[ShuffledHashJoinExecTransformer])

  private def vanillaBroadcastJoinNodeCount(plan: SparkPlan): Int =
    flattenPlan(plan).count(_.isInstanceOf[BroadcastHashJoinExec])

  private def withNativeStatefulAggregation[T](testBody: => T): T = {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[VeloxNativeStateStoreProvider].getName,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATEFUL_AGGREGATION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      // Keep the committed native map resident across batches and checkpoint each batch as an
      // O(changed-keys) delta (full snapshot only every minDeltasForSnapshot versions) so the
      // per-batch commit cost no longer grows with cumulative state size.
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_RESIDENT_REUSE_ENABLED.key -> "true",
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_DELTA_CHECKPOINT_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      testBody
    }
  }

  private def withVanillaStreaming[T](testBody: => T): T = {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "0",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "0",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "0",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      testBody
    }
  }

  private type Q12EventRow = (Long, Int, String, Date, Date, Date, String)

  private def runQ12ShippingPriorityScenario(
      mode: String,
      expectNative: Boolean,
      firstBatchRows: Seq[Q12EventRow],
      secondBatchRows: Seq[Q12EventRow],
      scaleRows: Int): Q12Evidence = {
    var result: Q12Evidence = null
    withTempDir {
      checkpointDir =>
        withTempDir {
          outputDir =>
            val inputData = MemoryStream[Q12EventRow]
            val checkpointPath = checkpointDir.getCanonicalPath
            val queryName =
              s"gluten_tpch_q12_${checkpointDir.getName}_${outputDir.getName}"
                .replaceAll("\\W", "_")
            val progress = ArrayBuffer.empty[StreamingQueryProgress]
            val nativeNodeCounts = ArrayBuffer.empty[Int]
            val nativeStatefulCounts = ArrayBuffer.empty[Int]
            val directColumnarStatefulCounts = ArrayBuffer.empty[Int]
            val directColumnarCountCounts = ArrayBuffer.empty[Int]
            val directColumnarLongSumCounts = ArrayBuffer.empty[Int]
            val rowInputStatefulCounts = ArrayBuffer.empty[Int]
            val rowToColumnarCounts = ArrayBuffer.empty[Int]
            val columnarToRowCounts = ArrayBuffer.empty[Int]
            val sparkOwnedSourceCounts = ArrayBuffer.empty[Int]
            val q12PriorityCounts = inputData
              .toDF()
              .toDF(
                "l_orderkey",
                "l_linenumber",
                "l_shipmode",
                "l_shipdate",
                "l_commitdate",
                "l_receiptdate",
                "o_orderpriority")
              .where(
                $"l_shipmode".isin("MAIL", "SHIP") &&
                  $"l_commitdate" < $"l_receiptdate" &&
                  $"l_shipdate" < $"l_commitdate" &&
                  $"l_receiptdate" >= q12StartReceiptDate &&
                  $"l_receiptdate" < q12EndReceiptDate)
              .select($"l_shipmode", $"o_orderpriority")
              .groupBy($"l_shipmode", $"o_orderpriority")
              .count()

            def startQuery(): StreamingQuery = {
              q12PriorityCounts.writeStream
                .format("memory")
                .queryName(queryName)
                .outputMode("complete")
                .option("checkpointLocation", checkpointPath)
                .start()
            }

            def collectEvidence(query: StreamingQuery): Unit = {
              val meaningfulProgress = query.recentProgress.filter {
                progress =>
                  progress.numInputRows > 0L ||
                  Option(progress.sink).exists(_.numOutputRows > 0L)
              }
              if (meaningfulProgress.nonEmpty) {
                progress ++= meaningfulProgress
              } else {
                Option(query.lastProgress).foreach(progress += _)
              }
              val plan = executedPlan(query)
              nativeNodeCounts += nativeStatelessNodeCount(plan)
              nativeStatefulCounts += nativeStatefulNodeCount(plan)
              directColumnarStatefulCounts += nativeDirectColumnarStatefulNodeCount(plan)
              directColumnarCountCounts += nativeDirectColumnarCountNodeCount(plan)
              directColumnarLongSumCounts += nativeDirectColumnarLongSumNodeCount(plan)
              rowInputStatefulCounts += nativeRowInputStatefulNodeCount(plan)
              rowToColumnarCounts += rowToColumnarNodeCount(plan)
              columnarToRowCounts += columnarToRowNodeCount(plan)
              sparkOwnedSourceCounts += sparkOwnedSourceNodeCount(plan)
              if (expectNative) {
                assertNativeQ12Plan(plan)
              } else {
                assertNoNativeQ12Plan(plan)
              }
            }

            val firstQuery = startQuery()
            try {
              inputData.addData(firstBatchRows: _*)
              firstQuery.processAllAvailable()
              collectEvidence(firstQuery)
            } finally {
              firstQuery.stop()
            }

            assertMemoryRows(queryName, expectedQ12OutputRows(firstBatchRows))
            assertCommitEpochs(checkpointDir, Set("0"))
            spark.catalog.dropTempView(queryName)

            val restarted = startQuery()
            try {
              inputData.addData(secondBatchRows: _*)
              restarted.processAllAvailable()
              collectEvidence(restarted)
            } finally {
              restarted.stop()
            }

            val expectedRows = expectedQ12OutputRows(firstBatchRows ++ secondBatchRows)
            assertMemoryRows(queryName, expectedRows)
            assertCommitEpochs(checkpointDir, Set("0", "1"))

            val rows = spark.table(queryName).collect().toSeq
            val nativeNodes = nativeNodeCounts.sum + nativeStatefulCounts.sum
            val rowToColumnarNodes = rowToColumnarCounts.sum
            val columnarToRowNodes = columnarToRowCounts.sum
            val sparkOwnedSourceNodes = sparkOwnedSourceCounts.sum
            result = Q12Evidence(
              run = currentRunLabel,
              scenario = "tpch-q12-lite-shipping-priority-count-restart",
              scaleRows = scaleRows,
              mode = mode,
              status = "PASS",
              progressBatches = progress.length,
              progressInputRows = progress.map(_.numInputRows).sum,
              expectedSourceRows = firstBatchRows.size + secondBatchRows.size,
              outputRows = rows.length,
              outputChecksum = checksumRows(rows),
              commitEpochs = committedEpochs(checkpointDir).toSeq.sorted.mkString(","),
              nativePlanNodes = nativeNodes,
              nativeDirectColumnarStatefulNodes = directColumnarStatefulCounts.sum,
              nativeDirectColumnarCountNodes = directColumnarCountCounts.sum,
              nativeDirectColumnarLongSumNodes = directColumnarLongSumCounts.sum,
              nativeRowInputStatefulNodes = rowInputStatefulCounts.sum,
              rowToColumnarNodes = rowToColumnarNodes,
              columnarToRowNodes = columnarToRowNodes,
              sparkOwnedSourceNodes = sparkOwnedSourceNodes,
              nativePlanNodeShare = planNodeShare(
                nativeNodes,
                rowToColumnarNodes + columnarToRowNodes + sparkOwnedSourceNodes),
              maxBatchDurationMs = maxBatchDurationMs(progress),
              maxTriggerExecutionMs = maxDurationMs(progress, "triggerExecution"),
              maxAddBatchMs = maxDurationMs(progress, "addBatch"),
              maxQueryPlanningMs = maxDurationMs(progress, "queryPlanning"),
              maxGetBatchMs = maxDurationMs(progress, "getBatch"),
              maxLatestOffsetMs = maxDurationMs(progress, "latestOffset"),
              maxWalCommitMs = maxDurationMs(progress, "walCommit"),
              maxCommitOffsetsMs = maxDurationMs(progress, "commitOffsets"),
              avgProcessedRowsPerSecond = avgProcessedRowsPerSecond(progress),
              throughputSampleCount = progress.length
            )
            spark.catalog.dropTempView(queryName)
        }
    }
    assert(result != null)
    result
  }

  private def runQ12ShippingPriorityLongSumScenario(
      mode: String,
      expectNative: Boolean,
      firstBatchRows: Seq[Q12EventRow],
      secondBatchRows: Seq[Q12EventRow],
      scaleRows: Int,
      outputMode: String = "complete",
      groupByOrderKey: Boolean = false): Q12Evidence = {
    var result: Q12Evidence = null
    withTempDir {
      checkpointDir =>
        withTempDir {
          outputDir =>
            val inputData = MemoryStream[Q12EventRow]
            val checkpointPath = checkpointDir.getCanonicalPath
            val queryName =
              s"gluten_tpch_q12_sum_${checkpointDir.getName}_${outputDir.getName}"
                .replaceAll("\\W", "_")
            val progress = ArrayBuffer.empty[StreamingQueryProgress]
            val nativeNodeCounts = ArrayBuffer.empty[Int]
            val nativeStatefulCounts = ArrayBuffer.empty[Int]
            val directColumnarStatefulCounts = ArrayBuffer.empty[Int]
            val directColumnarCountCounts = ArrayBuffer.empty[Int]
            val directColumnarLongSumCounts = ArrayBuffer.empty[Int]
            val rowInputStatefulCounts = ArrayBuffer.empty[Int]
            val rowToColumnarCounts = ArrayBuffer.empty[Int]
            val columnarToRowCounts = ArrayBuffer.empty[Int]
            val sparkOwnedSourceCounts = ArrayBuffer.empty[Int]
            val seenProgressBatchIds = LinkedHashSet.empty[Long]
            val q12FilteredRows = inputData
              .toDF()
              .toDF(
                "l_orderkey",
                "l_linenumber",
                "l_shipmode",
                "l_shipdate",
                "l_commitdate",
                "l_receiptdate",
                "o_orderpriority")
              .where(
                $"l_shipmode".isin("MAIL", "SHIP") &&
                  $"l_commitdate" < $"l_receiptdate" &&
                  $"l_shipdate" < $"l_commitdate" &&
                  $"l_receiptdate" >= q12StartReceiptDate &&
                  $"l_receiptdate" < q12EndReceiptDate)
            val q12PriorityOrderKeySums =
              if (groupByOrderKey) {
                q12FilteredRows
                  .select($"l_orderkey", $"l_orderkey".as("orderkey_amount"))
                  .groupBy($"l_orderkey")
                  .agg(sum($"orderkey_amount").as("orderkey_sum"))
              } else {
                q12FilteredRows
                  .select(
                    $"l_shipmode",
                    $"o_orderpriority",
                    $"l_orderkey".as("orderkey_amount"))
                  .groupBy($"l_shipmode", $"o_orderpriority")
                  .agg(sum($"orderkey_amount").as("orderkey_sum"))
              }

            def startQuery(): StreamingQuery = {
              q12PriorityOrderKeySums.writeStream
                .format("memory")
                .queryName(queryName)
                .outputMode(outputMode)
                .option("checkpointLocation", checkpointPath)
                .start()
            }

            def collectEvidence(query: StreamingQuery): Unit = {
              val meaningfulProgress = query.recentProgress.filter {
                progress =>
                  progress.numInputRows > 0L ||
                  Option(progress.sink).exists(_.numOutputRows > 0L)
              }.filterNot {
                progress => seenProgressBatchIds.contains(progress.batchId)
              }
              if (meaningfulProgress.nonEmpty) {
                progress ++= meaningfulProgress
                seenProgressBatchIds ++= meaningfulProgress.map(_.batchId)
              } else {
                Option(query.lastProgress)
                  .filterNot(progress => seenProgressBatchIds.contains(progress.batchId))
                  .foreach {
                    lastProgress =>
                      progress += lastProgress
                      seenProgressBatchIds += lastProgress.batchId
                  }
              }
              val plan = executedPlan(query)
              nativeNodeCounts += nativeStatelessNodeCount(plan)
              nativeStatefulCounts += nativeStatefulNodeCount(plan)
              directColumnarStatefulCounts += nativeDirectColumnarStatefulNodeCount(plan)
              directColumnarCountCounts += nativeDirectColumnarCountNodeCount(plan)
              directColumnarLongSumCounts += nativeDirectColumnarLongSumNodeCount(plan)
              rowInputStatefulCounts += nativeRowInputStatefulNodeCount(plan)
              rowToColumnarCounts += rowToColumnarNodeCount(plan)
              columnarToRowCounts += columnarToRowNodeCount(plan)
              sparkOwnedSourceCounts += sparkOwnedSourceNodeCount(plan)
              if (expectNative) {
                assertNativeQ12LongSumPlan(plan)
              } else {
                assertNoNativeQ12Plan(plan)
              }
            }

            val firstBatchExpected = expectedQ12LongSumRows(
              firstBatchRows,
              firstBatchRows,
              outputMode,
              groupByOrderKey)
            val secondBatchExpected = expectedQ12LongSumRows(
              firstBatchRows ++ secondBatchRows,
              secondBatchRows,
              outputMode,
              groupByOrderKey)
            val expectedRows =
              if (outputMode == "update") {
                val query = startQuery()
                try {
                  inputData.addData(firstBatchRows: _*)
                  query.processAllAvailable()
                  collectEvidence(query)
                  assertMemoryRows(queryName, firstBatchExpected)
                  assertCommitEpochs(checkpointDir, Set("0"))

                  inputData.addData(secondBatchRows: _*)
                  query.processAllAvailable()
                  collectEvidence(query)
                  firstBatchExpected ++ secondBatchExpected
                } finally {
                  query.stop()
                }
              } else {
                val firstQuery = startQuery()
                try {
                  inputData.addData(firstBatchRows: _*)
                  firstQuery.processAllAvailable()
                  collectEvidence(firstQuery)
                } finally {
                  firstQuery.stop()
                }

                assertMemoryRows(queryName, firstBatchExpected)
                assertCommitEpochs(checkpointDir, Set("0"))
                spark.catalog.dropTempView(queryName)

                val restarted = startQuery()
                try {
                  inputData.addData(secondBatchRows: _*)
                  restarted.processAllAvailable()
                  collectEvidence(restarted)
                } finally {
                  restarted.stop()
                }
                secondBatchExpected
              }
            assertMemoryRows(queryName, expectedRows)
            assertCommitEpochs(checkpointDir, Set("0", "1"))

            val rows = spark.table(queryName).collect().toSeq
            val nativeNodes = nativeNodeCounts.sum + nativeStatefulCounts.sum
            val rowToColumnarNodes = rowToColumnarCounts.sum
            val columnarToRowNodes = columnarToRowCounts.sum
            val sparkOwnedSourceNodes = sparkOwnedSourceCounts.sum
            result = Q12Evidence(
              run = currentRunLabel,
              scenario =
                if (groupByOrderKey) {
                  q12OrderKeyLongSumScenarioName(outputMode)
                } else {
                  q12LongSumScenarioName(outputMode)
                },
              scaleRows = scaleRows,
              mode = mode,
              status = "PASS",
              progressBatches = progress.length,
              progressInputRows = progress.map(_.numInputRows).sum,
              expectedSourceRows = firstBatchRows.size + secondBatchRows.size,
              outputRows = rows.length,
              outputChecksum = checksumRows(rows),
              commitEpochs = committedEpochs(checkpointDir).toSeq.sorted.mkString(","),
              nativePlanNodes = nativeNodes,
              nativeDirectColumnarStatefulNodes = directColumnarStatefulCounts.sum,
              nativeDirectColumnarCountNodes = directColumnarCountCounts.sum,
              nativeDirectColumnarLongSumNodes = directColumnarLongSumCounts.sum,
              nativeRowInputStatefulNodes = rowInputStatefulCounts.sum,
              rowToColumnarNodes = rowToColumnarNodes,
              columnarToRowNodes = columnarToRowNodes,
              sparkOwnedSourceNodes = sparkOwnedSourceNodes,
              nativePlanNodeShare = planNodeShare(
                nativeNodes,
                rowToColumnarNodes + columnarToRowNodes + sparkOwnedSourceNodes),
              maxBatchDurationMs = maxBatchDurationMs(progress),
              maxTriggerExecutionMs = maxDurationMs(progress, "triggerExecution"),
              maxAddBatchMs = maxDurationMs(progress, "addBatch"),
              maxQueryPlanningMs = maxDurationMs(progress, "queryPlanning"),
              maxGetBatchMs = maxDurationMs(progress, "getBatch"),
              maxLatestOffsetMs = maxDurationMs(progress, "latestOffset"),
              maxWalCommitMs = maxDurationMs(progress, "walCommit"),
              maxCommitOffsetsMs = maxDurationMs(progress, "commitOffsets"),
              avgProcessedRowsPerSecond = avgProcessedRowsPerSecond(progress),
              throughputSampleCount = progress.length
            )
            spark.catalog.dropTempView(queryName)
        }
    }
    assert(result != null)
    result
  }

  private val q12EventSchema = StructType(
    Seq(
      StructField("l_orderkey", LongType),
      StructField("l_linenumber", IntegerType),
      StructField("l_shipmode", StringType),
      StructField("l_shipdate", DateType),
      StructField("l_commitdate", DateType),
      StructField("l_receiptdate", DateType),
      StructField("o_orderpriority", StringType)
    ))

  private def writeQ12EventParquetBatch(sourcePath: String, rows: Seq[Q12EventRow]): Unit = {
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(rows.map {
          case (orderKey, lineNumber, shipMode, shipDate, commitDate, receiptDate, prio) =>
            Row(orderKey, lineNumber, shipMode, shipDate, commitDate, receiptDate, prio)
        }),
        q12EventSchema)
      .coalesce(1)
      .write
      .mode("append")
      .parquet(sourcePath)
  }

  // File-source variant of the high-cardinality orderkey long-sum update benchmark. Instead of a
  // row-based MemoryStream, batches are materialized as Parquet and consumed via a streaming file
  // source. With the native file-source gate + Velox cache this routes the scan through
  // FileSourceScanExecTransformer with rowToColumnar=0 (no source-side Row->Columnar bridge).
  private def runQ12FileSourceOrderKeyLongSumScenario(
      mode: String,
      expectNative: Boolean,
      firstBatchRows: Seq[Q12EventRow],
      secondBatchRows: Seq[Q12EventRow],
      scaleRows: Int): Q12Evidence = {
    val outputMode = "update"
    var result: Q12Evidence = null
    withTempDir {
      sourceDir =>
        withTempDir {
          checkpointDir =>
            val sourcePath = sourceDir.getCanonicalPath
            val checkpointPath = checkpointDir.getCanonicalPath
            val queryName =
              s"gluten_tpch_q12_filesrc_sum_${checkpointDir.getName}".replaceAll("\\W", "_")
            val progress = ArrayBuffer.empty[StreamingQueryProgress]
            val nativeNodeCounts = ArrayBuffer.empty[Int]
            val nativeStatefulCounts = ArrayBuffer.empty[Int]
            val directColumnarStatefulCounts = ArrayBuffer.empty[Int]
            val directColumnarCountCounts = ArrayBuffer.empty[Int]
            val directColumnarLongSumCounts = ArrayBuffer.empty[Int]
            val rowInputStatefulCounts = ArrayBuffer.empty[Int]
            val rowToColumnarCounts = ArrayBuffer.empty[Int]
            val columnarToRowCounts = ArrayBuffer.empty[Int]
            val nativeFileSourceScanCounts = ArrayBuffer.empty[Int]
            val seenProgressBatchIds = LinkedHashSet.empty[Long]

            val streamDf = spark.readStream
              .schema(q12EventSchema)
              .parquet(sourcePath)
              .where(
                $"l_shipmode".isin("MAIL", "SHIP") &&
                  $"l_commitdate" < $"l_receiptdate" &&
                  $"l_shipdate" < $"l_commitdate" &&
                  $"l_receiptdate" >= q12StartReceiptDate &&
                  $"l_receiptdate" < q12EndReceiptDate)
              .select($"l_orderkey", $"l_orderkey".as("orderkey_amount"))
              .groupBy($"l_orderkey")
              .agg(sum($"orderkey_amount").as("orderkey_sum"))

            def startQuery(): StreamingQuery = {
              streamDf.writeStream
                .format("memory")
                .queryName(queryName)
                .outputMode(outputMode)
                .option("checkpointLocation", checkpointPath)
                .start()
            }

            def collectEvidence(query: StreamingQuery): Unit = {
              val meaningfulProgress = query.recentProgress.filter {
                progress =>
                  progress.numInputRows > 0L ||
                  Option(progress.sink).exists(_.numOutputRows > 0L)
              }.filterNot {
                progress => seenProgressBatchIds.contains(progress.batchId)
              }
              if (meaningfulProgress.nonEmpty) {
                progress ++= meaningfulProgress
                seenProgressBatchIds ++= meaningfulProgress.map(_.batchId)
              } else {
                Option(query.lastProgress)
                  .filterNot(progress => seenProgressBatchIds.contains(progress.batchId))
                  .foreach {
                    lastProgress =>
                      progress += lastProgress
                      seenProgressBatchIds += lastProgress.batchId
                  }
              }
              val plan = executedPlan(query)
              nativeNodeCounts += nativeStatelessNodeCount(plan)
              nativeStatefulCounts += nativeStatefulNodeCount(plan)
              directColumnarStatefulCounts += nativeDirectColumnarStatefulNodeCount(plan)
              directColumnarCountCounts += nativeDirectColumnarCountNodeCount(plan)
              directColumnarLongSumCounts += nativeDirectColumnarLongSumNodeCount(plan)
              rowInputStatefulCounts += nativeRowInputStatefulNodeCount(plan)
              rowToColumnarCounts += rowToColumnarNodeCount(plan)
              columnarToRowCounts += columnarToRowNodeCount(plan)
              nativeFileSourceScanCounts += nativeFileSourceScanNodeCount(plan)
              if (expectNative) {
                assertNativeQ12FileSourceLongSumPlan(plan)
              } else {
                assertNoNativeQ12Plan(plan)
              }
            }

            val firstBatchExpected =
              expectedQ12LongSumRows(firstBatchRows, firstBatchRows, outputMode, groupByOrderKey = true)
            val secondBatchExpected = expectedQ12LongSumRows(
              firstBatchRows ++ secondBatchRows,
              secondBatchRows,
              outputMode,
              groupByOrderKey = true)

            val query = startQuery()
            val expectedRows =
              try {
                writeQ12EventParquetBatch(sourcePath, firstBatchRows)
                query.processAllAvailable()
                collectEvidence(query)
                assertMemoryRows(queryName, firstBatchExpected)
                assertCommitEpochs(checkpointDir, Set("0"))

                writeQ12EventParquetBatch(sourcePath, secondBatchRows)
                query.processAllAvailable()
                collectEvidence(query)
                firstBatchExpected ++ secondBatchExpected
              } finally {
                query.stop()
              }
            assertMemoryRows(queryName, expectedRows)
            assertCommitEpochs(checkpointDir, Set("0", "1"))

            val rows = spark.table(queryName).collect().toSeq
            val nativeNodes = nativeNodeCounts.sum + nativeStatefulCounts.sum
            val rowToColumnarNodes = rowToColumnarCounts.sum
            val columnarToRowNodes = columnarToRowCounts.sum
            val nativeFileSourceScanNodes = nativeFileSourceScanCounts.sum
            result = Q12Evidence(
              run = currentRunLabel,
              scenario = "tpch-q12-lite-filesource-orderkey-sum-update",
              scaleRows = scaleRows,
              mode = mode,
              status = "PASS",
              progressBatches = progress.length,
              progressInputRows = progress.map(_.numInputRows).sum,
              expectedSourceRows = firstBatchRows.size + secondBatchRows.size,
              outputRows = rows.length,
              outputChecksum = checksumRows(rows),
              commitEpochs = committedEpochs(checkpointDir).toSeq.sorted.mkString(","),
              nativePlanNodes = nativeNodes,
              nativeDirectColumnarStatefulNodes = directColumnarStatefulCounts.sum,
              nativeDirectColumnarCountNodes = directColumnarCountCounts.sum,
              nativeDirectColumnarLongSumNodes = directColumnarLongSumCounts.sum,
              nativeRowInputStatefulNodes = rowInputStatefulCounts.sum,
              rowToColumnarNodes = rowToColumnarNodes,
              columnarToRowNodes = columnarToRowNodes,
              sparkOwnedSourceNodes = nativeFileSourceScanNodes,
              nativePlanNodeShare = planNodeShare(
                nativeNodes,
                rowToColumnarNodes + columnarToRowNodes),
              maxBatchDurationMs = maxBatchDurationMs(progress),
              maxTriggerExecutionMs = maxDurationMs(progress, "triggerExecution"),
              maxAddBatchMs = maxDurationMs(progress, "addBatch"),
              maxQueryPlanningMs = maxDurationMs(progress, "queryPlanning"),
              maxGetBatchMs = maxDurationMs(progress, "getBatch"),
              maxLatestOffsetMs = maxDurationMs(progress, "latestOffset"),
              maxWalCommitMs = maxDurationMs(progress, "walCommit"),
              maxCommitOffsetsMs = maxDurationMs(progress, "commitOffsets"),
              avgProcessedRowsPerSecond = avgProcessedRowsPerSecond(progress),
              throughputSampleCount = progress.length
            )
            spark.catalog.dropTempView(queryName)
        }
    }
    assert(result != null)
    result
  }

  private def loadQ12EventRows(rowCount: Int): Seq[Q12EventRow] = {
    val root = tpchDataRoot
    val lineitem = spark.read.parquet(new File(root, "lineitem").getCanonicalPath)
    val orders = spark.read.parquet(new File(root, "orders").getCanonicalPath)
    val baseRows = lineitem
      .join(orders, lineitem("l_orderkey") === orders("o_orderkey"))
      .where(
        lineitem("l_shipmode").isin("MAIL", "SHIP") &&
          lineitem("l_commitdate") < lineitem("l_receiptdate") &&
          lineitem("l_shipdate") < lineitem("l_commitdate") &&
          lineitem("l_receiptdate") >= q12StartReceiptDate &&
          lineitem("l_receiptdate") < q12EndReceiptDate)
      .select(
        lineitem("l_orderkey").cast("long"),
        lineitem("l_linenumber").cast("int"),
        lineitem("l_shipmode").cast("string"),
        lineitem("l_shipdate"),
        lineitem("l_commitdate"),
        lineitem("l_receiptdate"),
        orders("o_orderpriority").cast("string")
      )
      .orderBy("l_orderkey", "l_linenumber")
      .collect()
      .map {
        row =>
          (
            row.getLong(0),
            row.getInt(1),
            row.getString(2),
            row.getDate(3),
            row.getDate(4),
            row.getDate(5),
            row.getString(6))
      }
      .toIndexedSeq
    assert(baseRows.nonEmpty, s"TPC-H Q12 fixture query produced no rows from $root")

    (0 until rowCount).map {
      index =>
        val base = baseRows(index % baseRows.size)
        val repeat = index / baseRows.size
        base.copy(_1 = base._1 + repeat.toLong * 100000000L, _2 = base._2 + repeat * 1000)
    }
  }

  private def expectedQ12OutputRows(rows: Seq[Q12EventRow]): Set[Row] = {
    rows
      .filter {
        case (_, _, shipMode, shipDate, commitDate, receiptDate, _) =>
          qualifiesForQ12(shipMode, shipDate, commitDate, receiptDate)
      }
      .groupBy {
        case (_, _, shipMode, _, _, _, orderPriority) => (shipMode, orderPriority)
      }
      .map {
        case ((shipMode, orderPriority), groupedRows) =>
          Row(shipMode, orderPriority, groupedRows.size.toLong)
      }
      .toSet
  }

  private def expectedQ12LongSumOutputRows(rows: Seq[Q12EventRow]): Set[Row] = {
    rows
      .filter {
        case (_, _, shipMode, shipDate, commitDate, receiptDate, _) =>
          qualifiesForQ12(shipMode, shipDate, commitDate, receiptDate)
      }
      .groupBy {
        case (_, _, shipMode, _, _, _, orderPriority) => (shipMode, orderPriority)
      }
      .map {
        case ((shipMode, orderPriority), groupedRows) =>
          Row(shipMode, orderPriority, groupedRows.map(_._1).sum)
      }
      .toSet
  }

  private def expectedQ12OrderKeyLongSumOutputRows(rows: Seq[Q12EventRow]): Set[Row] = {
    rows
      .filter {
        case (_, _, shipMode, shipDate, commitDate, receiptDate, _) =>
          qualifiesForQ12(shipMode, shipDate, commitDate, receiptDate)
      }
      .groupBy {
        case (orderKey, _, _, _, _, _, _) => orderKey
      }
      .map {
        case (orderKey, groupedRows) =>
          Row(orderKey, groupedRows.map(_._1).sum)
      }
      .toSet
  }

  private def expectedQ12LongSumRows(
      stateRows: Seq[Q12EventRow],
      updatedRows: Seq[Q12EventRow],
      outputMode: String,
      groupByOrderKey: Boolean = false): Set[Row] = {
    val currentOutputRows =
      if (groupByOrderKey) {
        expectedQ12OrderKeyLongSumOutputRows(stateRows)
      } else {
        expectedQ12LongSumOutputRows(stateRows)
      }
    outputMode match {
      case "complete" => currentOutputRows
      case "update" =>
        val updatedKeys = updatedRows
          .filter {
            case (_, _, shipMode, shipDate, commitDate, receiptDate, _) =>
              qualifiesForQ12(shipMode, shipDate, commitDate, receiptDate)
          }
          .map {
            case (orderKey, _, shipMode, _, _, _, orderPriority) =>
              if (groupByOrderKey) {
                orderKey
              } else {
                (shipMode, orderPriority)
              }
          }
          .toSet
        currentOutputRows.filter {
          row =>
            if (groupByOrderKey) {
              updatedKeys.contains(row.getLong(0))
            } else {
              updatedKeys.contains((row.getString(0), row.getString(1)))
            }
        }
      case other => fail(s"Unsupported TPC-H Q12 long-sum output mode: $other")
    }
  }

  private def q12LongSumScenarioName(outputMode: String): String = {
    outputMode match {
      case "complete" => "tpch-q12-lite-shipping-priority-orderkey-sum-complete-restart"
      case "update" => "tpch-q12-lite-shipping-priority-orderkey-sum-update-live"
      case other => fail(s"Unsupported TPC-H Q12 long-sum output mode: $other")
    }
  }

  private def q12OrderKeyLongSumScenarioName(outputMode: String): String = {
    outputMode match {
      case "update" => "tpch-q12-lite-orderkey-sum-update-live"
      case other => fail(s"Unsupported TPC-H Q12 orderkey long-sum output mode: $other")
    }
  }

  private def qualifiesForQ12(
      shipMode: String,
      shipDate: Date,
      commitDate: Date,
      receiptDate: Date): Boolean = {
    (shipMode == "MAIL" || shipMode == "SHIP") &&
    commitDate.before(receiptDate) &&
    shipDate.before(commitDate) &&
    !receiptDate.before(q12StartReceiptDate) &&
    receiptDate.before(q12EndReceiptDate)
  }

  private def assertQ12EvidenceParity(
      nativeEvidence: Q12Evidence,
      vanillaEvidence: Q12Evidence): Unit = {
    assert(
      nativeEvidence.outputChecksum == vanillaEvidence.outputChecksum,
      s"Native and vanilla TPC-H Q12 streaming output checksums diverged: " +
        s"native=${nativeEvidence.outputChecksum}, vanilla=${vanillaEvidence.outputChecksum}"
    )
    assert(
      nativeEvidence.outputRows == vanillaEvidence.outputRows,
      s"Native and vanilla TPC-H Q12 streaming row counts diverged: " +
        s"native=${nativeEvidence.outputRows}, vanilla=${vanillaEvidence.outputRows}"
    )
    assert(
      nativeEvidence.expectedSourceRows == vanillaEvidence.expectedSourceRows,
      s"Native and vanilla TPC-H Q12 source row counts diverged: " +
        s"native=${nativeEvidence.expectedSourceRows}, " +
        s"vanilla=${vanillaEvidence.expectedSourceRows}"
    )
    assert(
      nativeEvidence.progressInputRows == nativeEvidence.expectedSourceRows,
      s"Native TPC-H Q12 progress input rows should match deterministic source rows: " +
        s"progress=${nativeEvidence.progressInputRows}, " +
        s"expected=${nativeEvidence.expectedSourceRows}"
    )
    assert(
      vanillaEvidence.progressInputRows == vanillaEvidence.expectedSourceRows,
      s"Vanilla TPC-H Q12 progress input rows should match deterministic source rows: " +
        s"progress=${vanillaEvidence.progressInputRows}, " +
        s"expected=${vanillaEvidence.expectedSourceRows}"
    )
    assert(
      nativeEvidence.nativePlanNodes > 0,
      s"Expected native TPC-H Q12 run to contain native stateful nodes: $nativeEvidence")
    assert(
      vanillaEvidence.nativePlanNodes == 0,
      s"Expected vanilla TPC-H Q12 run to avoid native nodes: $vanillaEvidence")
  }

  private def assertNativeQ12Plan(plan: SparkPlan): Unit = {
    val nativeCountNodes = plan.collect {
      case exec: VeloxNativeStreamingCountExec => exec
    }
    assert(
      nativeCountNodes.nonEmpty,
      s"Expected native count aggregation in TPC-H Q12 stream:\n$plan")
    assert(
      nativeCountNodes.forall(_.directColumnarInputEnabled),
      "Expected all native count state inputs to be direct columnar in TPC-H Q12 stream:\n" +
        plan
    )
    assert(
      columnarToRowNodeCount(plan) == 0,
      s"Expected no explicit ColumnarToRow bridge in TPC-H Q12 native count stream:\n$plan")
    assert(
      nativeStatelessNodeCount(plan) > 0,
      s"Expected native stateless preprocessing before TPC-H Q12 native state:\n$plan")
    val sparkOwnedWrappers = plan.collect { case source: SparkOwnedMicroBatchScanExec => source }
    val rawMicroBatchScans = plan.collect { case scan: MicroBatchScanExec => scan }
    assert(
      sparkOwnedWrappers.nonEmpty || rawMicroBatchScans.nonEmpty,
      s"Expected Spark-owned micro-batch source boundary in TPC-H Q12 stream:\n$plan")
    sparkOwnedWrappers.foreach {
      source =>
        assert(
          source.scan.isInstanceOf[MicroBatchScanExec],
          s"Expected Spark-owned wrapper to retain Spark MicroBatchScanExec:\n$plan")
    }
  }

  private def assertNativeQ12LongSumPlan(plan: SparkPlan): Unit = {
    val nativeLongSumNodes = plan.collect {
      case exec: VeloxNativeStreamingLongSumExec => exec
    }
    assert(
      nativeLongSumNodes.nonEmpty,
      s"Expected native long-sum aggregation in TPC-H Q12 stream:\n$plan")
    assert(
      nativeLongSumNodes.forall(_.directColumnarInputEnabled),
      "Expected all native long-sum state inputs to be direct columnar in TPC-H Q12 " +
        s"stream:\n$plan"
    )
    assert(
      columnarToRowNodeCount(plan) == 0,
      s"Expected no explicit ColumnarToRow bridge in TPC-H Q12 native long-sum stream:\n$plan")
    assert(
      nativeStatelessNodeCount(plan) > 0,
      s"Expected native stateless preprocessing before TPC-H Q12 native long-sum state:\n$plan")
    val sparkOwnedWrappers = plan.collect { case source: SparkOwnedMicroBatchScanExec => source }
    val rawMicroBatchScans = plan.collect { case scan: MicroBatchScanExec => scan }
    assert(
      sparkOwnedWrappers.nonEmpty || rawMicroBatchScans.nonEmpty,
      s"Expected Spark-owned micro-batch source boundary in TPC-H Q12 long-sum stream:\n$plan")
    sparkOwnedWrappers.foreach {
      source =>
        assert(
          source.scan.isInstanceOf[MicroBatchScanExec],
          s"Expected Spark-owned wrapper to retain Spark MicroBatchScanExec:\n$plan")
    }
  }

  private def assertNativeQ12FileSourceLongSumPlan(plan: SparkPlan): Unit = {
    val nativeLongSumNodes = plan.collect {
      case exec: VeloxNativeStreamingLongSumExec => exec
    }
    assert(
      nativeLongSumNodes.nonEmpty,
      s"Expected native long-sum aggregation in TPC-H Q12 file-source stream:\n$plan")
    assert(
      nativeLongSumNodes.forall(_.directColumnarInputEnabled),
      "Expected all native long-sum state inputs to be direct columnar in TPC-H Q12 " +
        s"file-source stream:\n$plan"
    )
    assert(
      nativeStatelessNodeCount(plan) > 0,
      "Expected native stateless preprocessing before TPC-H Q12 native long-sum " +
        s"file-source state:\n$plan")
    // The native file-source path must own the scan with no source-side Row->Columnar bridge.
    assert(
      nativeFileSourceScanNodeCount(plan) > 0,
      s"Expected native FileSourceScanExecTransformer to own the file-source scan:\n$plan")
    assert(
      rowToColumnarNodeCount(plan) == 0,
      s"Expected no source-side RowToVeloxColumnar bridge in TPC-H Q12 native file-source " +
        s"stream:\n$plan")
    assert(
      plan.collect { case scan: FileSourceScanExec => scan }.isEmpty,
      s"Expected no vanilla FileSourceScanExec in TPC-H Q12 native file-source stream:\n$plan")
  }

  private def assertNoNativeQ12Plan(plan: SparkPlan): Unit = {
    assert(
      nativeStatelessNodeCount(plan) + nativeStatefulNodeCount(plan) == 0,
      s"Expected vanilla TPC-H Q12 benchmark to avoid native execution:\n$plan")
  }

  private def executedPlan(query: StreamingQuery): SparkPlan = {
    query
      .asInstanceOf[StreamingQueryWrapper]
      .streamingQuery
      .lastExecution
      .executedPlan
  }

  private def nativeStatelessNodeCount(plan: SparkPlan): Int = {
    plan.collect {
      case _: ProjectExecTransformer => 1
      case _: FilterExecTransformer => 1
    }.sum
  }

  private def nativeStatefulNodeCount(plan: SparkPlan): Int =
    plan.collect {
      case _: VeloxNativeStreamingCountExec => 1
      case _: VeloxNativeStreamingLongSumExec => 1
    }.sum

  private def nativeDirectColumnarStatefulNodeCount(plan: SparkPlan): Int =
    plan.collect {
      case exec: VeloxNativeStreamingCountExec if exec.directColumnarInputEnabled => 1
      case exec: VeloxNativeStreamingLongSumExec if exec.directColumnarInputEnabled => 1
    }.sum

  private def nativeDirectColumnarCountNodeCount(plan: SparkPlan): Int =
    plan.collect {
      case exec: VeloxNativeStreamingCountExec if exec.directColumnarInputEnabled => 1
    }.sum

  private def nativeDirectColumnarLongSumNodeCount(plan: SparkPlan): Int =
    plan.collect {
      case exec: VeloxNativeStreamingLongSumExec if exec.directColumnarInputEnabled => 1
    }.sum

  private def nativeRowInputStatefulNodeCount(plan: SparkPlan): Int =
    plan.collect {
      case exec: VeloxNativeStreamingCountExec if !exec.directColumnarInputEnabled => 1
      case exec: VeloxNativeStreamingLongSumExec if !exec.directColumnarInputEnabled => 1
    }.sum

  private def rowToColumnarNodeCount(plan: SparkPlan): Int =
    plan.collect { case _: RowToVeloxColumnarExec => 1 }.sum

  private def columnarToRowNodeCount(plan: SparkPlan): Int =
    plan.collect { case _: VeloxColumnarToRowExec => 1 }.sum

  private def sparkOwnedSourceNodeCount(plan: SparkPlan): Int =
    plan.collect {
      case _: SparkOwnedMicroBatchScanExec => 1
      case _: MicroBatchScanExec => 1
    }.sum

  private def nativeFileSourceScanNodeCount(plan: SparkPlan): Int =
    plan.collect { case _: FileSourceScanExecTransformer => 1 }.sum

  private def assertParquetRows(outputPath: String, expectedRows: Set[Row]): Unit = {
    assert(spark.read.parquet(outputPath).collect().toSet == expectedRows)
  }

  private def assertMemoryRows(queryName: String, expectedRows: Set[Row]): Unit = {
    assert(spark.table(queryName).collect().toSet == expectedRows)
  }

  private def assertCommitEpochs(checkpointDir: File, expectedEpochs: Set[String]): Unit = {
    assert(committedEpochs(checkpointDir) == expectedEpochs)
  }

  private def committedEpochs(checkpointDir: File): Set[String] = {
    val commitDir = new File(checkpointDir, "commits")
    Option(commitDir.list())
      .map(_.filterNot(_.startsWith(".")).toSet)
      .getOrElse(Set.empty[String])
  }

  private def checksumRows(rows: Seq[Row]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    rows
      .map(rowToStableString)
      .sorted
      .foreach {
        value =>
          digest.update(value.getBytes(StandardCharsets.UTF_8))
          digest.update('\n'.toByte)
      }
    digest.digest().map(byte => f"${byte & 0xff}%02x").mkString
  }

  private def rowToStableString(row: Row): String = {
    row.toSeq
      .map(value => Option(value).map(_.toString).getOrElse("<null>"))
      .mkString("[", ",", "]")
  }

  private def maxBatchDurationMs(progress: collection.Seq[StreamingQueryProgress]): Long = {
    progress
      .flatMap(_.durationMs.asScala.values.map(_.toLong))
      .foldLeft(0L)(math.max)
  }

  private def maxDurationMs(
      progress: collection.Seq[StreamingQueryProgress],
      durationKey: String): Long = {
    progress
      .flatMap(_.durationMs.asScala.get(durationKey).map(_.toLong))
      .foldLeft(0L)(math.max)
  }

  private def avgProcessedRowsPerSecond(
      progress: collection.Seq[StreamingQueryProgress]): String = {
    if (progress.isEmpty) {
      "0.0"
    } else {
      f"${progress.map(_.processedRowsPerSecond).sum / progress.length}%.2f"
    }
  }

  private def planNodeShare(nativeNodes: Int, nonNativeBoundaryNodes: Int): String = {
    val total = nativeNodes + nonNativeBoundaryNodes
    if (total == 0) {
      "0.0000"
    } else {
      f"${nativeNodes.toDouble / total.toDouble}%.4f"
    }
  }

  private def tpchScaleRows: Int = {
    val rowCount = sys.env
      .get("GLUTEN_NATIVE_STREAMING_TPCH_SCALE_ROWS")
      .filter(_.nonEmpty)
      .map {
        value =>
          assert(
            value.matches("[0-9]+"),
            s"GLUTEN_NATIVE_STREAMING_TPCH_SCALE_ROWS must be a non-negative integer: $value")
          value.toInt
      }
      .getOrElse(20000)
    assert(
      rowCount >= 2,
      s"GLUTEN_NATIVE_STREAMING_TPCH_SCALE_ROWS must be at least 2: $rowCount")
    rowCount
  }

  private def tpchReportPath: Option[String] =
    sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_COMPARISON_REPORT").filter(_.nonEmpty)

  private def currentRunLabel: String = {
    sys.env
      .get("GLUTEN_NATIVE_STREAMING_TPCH_CURRENT_RUN_LABEL")
      .orElse(sys.env.get("GLUTEN_NATIVE_STREAMING_TPCH_RUN_LABEL"))
      .filter(_.nonEmpty)
      .getOrElse("single")
  }

  private def tpchDataRoot: File = {
    val userDir = new File(sys.props.getOrElse("user.dir", ".")).getCanonicalFile
    val candidates = Seq(
      new File(userDir, "backends-velox/src/test/resources/tpch-data-parquet"),
      new File(userDir, "../backends-velox/src/test/resources/tpch-data-parquet"),
      new File(userDir, "../../backends-velox/src/test/resources/tpch-data-parquet"),
      new File(userDir, "gluten-core/src/test/resources/tpch-data"),
      new File(userDir, "../gluten-core/src/test/resources/tpch-data"),
      new File(userDir, "../../gluten-core/src/test/resources/tpch-data")
    )
    candidates.find(_.isDirectory).getOrElse {
      fail(
        s"Unable to locate checked-in TPC-H parquet fixtures from ${userDir.getCanonicalPath}. " +
          s"Tried: ${candidates.map(_.getPath).mkString(", ")}")
    }
  }

  private def appendQ12Evidence(reportPath: String, rows: Seq[Q12Evidence]): Unit = {
    val file = new File(reportPath)
    Option(file.getParentFile).foreach(_.mkdirs())
    val shouldWriteHeader = !file.exists() || file.length() == 0L
    val writer = new BufferedWriter(new FileWriter(file, true))
    try {
      if (shouldWriteHeader) {
        writer.write(Q12Evidence.Header)
        writer.newLine()
      }
      rows.foreach {
        row =>
          writer.write(row.toTsv)
          writer.newLine()
      }
    } finally {
      writer.close()
    }
  }

  private val q12StartReceiptDate = Date.valueOf("1994-01-01")
  private val q12EndReceiptDate = Date.valueOf("1995-01-01")

  private case class Q12Evidence(
      run: String,
      scenario: String,
      scaleRows: Int,
      mode: String,
      status: String,
      progressBatches: Int,
      progressInputRows: Long,
      expectedSourceRows: Long,
      outputRows: Long,
      outputChecksum: String,
      commitEpochs: String,
      nativePlanNodes: Int,
      nativeDirectColumnarStatefulNodes: Int,
      nativeDirectColumnarCountNodes: Int,
      nativeDirectColumnarLongSumNodes: Int,
      nativeRowInputStatefulNodes: Int,
      rowToColumnarNodes: Int,
      columnarToRowNodes: Int,
      sparkOwnedSourceNodes: Int,
      nativePlanNodeShare: String,
      maxBatchDurationMs: Long,
      maxTriggerExecutionMs: Long,
      maxAddBatchMs: Long,
      maxQueryPlanningMs: Long,
      maxGetBatchMs: Long,
      maxLatestOffsetMs: Long,
      maxWalCommitMs: Long,
      maxCommitOffsetsMs: Long,
      avgProcessedRowsPerSecond: String,
      throughputSampleCount: Int) {
    def toTsv: String = {
      Seq(
        run,
        scenario,
        scaleRows.toString,
        mode,
        status,
        progressBatches.toString,
        progressInputRows.toString,
        expectedSourceRows.toString,
        outputRows.toString,
        outputChecksum,
        commitEpochs,
        nativePlanNodes.toString,
        rowToColumnarNodes.toString,
        columnarToRowNodes.toString,
        sparkOwnedSourceNodes.toString,
        nativePlanNodeShare,
        maxBatchDurationMs.toString,
        maxTriggerExecutionMs.toString,
        maxAddBatchMs.toString,
        maxQueryPlanningMs.toString,
        maxGetBatchMs.toString,
        maxLatestOffsetMs.toString,
        maxWalCommitMs.toString,
        maxCommitOffsetsMs.toString,
        avgProcessedRowsPerSecond,
        throughputSampleCount.toString,
        nativeDirectColumnarStatefulNodes.toString,
        nativeDirectColumnarCountNodes.toString,
        nativeDirectColumnarLongSumNodes.toString,
        nativeRowInputStatefulNodes.toString
      ).mkString("\t")
    }
  }

  private object Q12Evidence {
    val Header: String =
      "run\tscenario\tscale_rows\tmode\tstatus\tprogress_batches\tprogress_input_rows\t" +
        "expected_source_rows\toutput_rows\toutput_checksum\tcommit_epochs\t" +
        "native_plan_nodes\trow_to_columnar_nodes\tcolumnar_to_row_nodes\t" +
        "spark_owned_source_nodes\tnative_plan_node_share\tmax_batch_duration_ms\t" +
        "max_trigger_execution_ms\tmax_add_batch_ms\tmax_query_planning_ms\t" +
        "max_get_batch_ms\tmax_latest_offset_ms\tmax_wal_commit_ms\tmax_commit_offsets_ms\t" +
        "avg_processed_rows_per_second\tthroughput_sample_count\t" +
        "native_direct_columnar_stateful_nodes\t" +
        "native_direct_columnar_count_nodes\t" +
        "native_direct_columnar_long_sum_nodes\tnative_row_input_stateful_nodes"
  }
}
