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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FilterExecTransformer, ProjectExecTransformer, RowToVeloxColumnarExec, SparkOwnedMicroBatchScanExec, VeloxColumnarToRowExec}

import org.apache.spark.sql.{GlutenStreamingSQLTestsTrait, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class GlutenVeloxNativeStreamingTpchBenchmarkSuite
  extends StreamTest
  with GlutenStreamingSQLTestsTrait {

  import testImplicits._

  testGluten("TPC-H Q6-style lineitem stream native-vs-vanilla correctness and timing") {
    val rowCount = tpchScaleRows
    val rows = generateLineitemRows(rowCount)
    val splitAt = math.max(1, rowCount / 2)
    val firstBatchRows = rows.take(splitAt)
    val secondBatchRows = rows.drop(splitAt)

    val nativeEvidence = withStatelessBridge {
      runQ6LineitemScenario(
        mode = "native",
        expectNative = true,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }
    val vanillaEvidence = withVanillaStreaming {
      runQ6LineitemScenario(
        mode = "vanilla",
        expectNative = false,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = rowCount)
    }

    assertQ6EvidenceParity(nativeEvidence, vanillaEvidence)
    tpchReportPath.foreach {
      reportPath =>
        appendQ6Evidence(
          reportPath,
          Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
    }
  }

  private def withStatelessBridge[T](testBody: => T): T = {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
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
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      testBody
    }
  }

  private type LineitemRow = (Long, Long, Long, Int, Long, Int, Int, Int)

  private def runQ6LineitemScenario(
      mode: String,
      expectNative: Boolean,
      firstBatchRows: Seq[LineitemRow],
      secondBatchRows: Seq[LineitemRow],
      scaleRows: Int): Q6Evidence = {
    var result: Q6Evidence = null
    withTempDir {
      checkpointDir =>
        withTempDir {
          outputDir =>
            val inputData = MemoryStream[LineitemRow]
            val checkpointPath = checkpointDir.getCanonicalPath
            val outputPath = outputDir.getCanonicalPath
            val progress = ArrayBuffer.empty[StreamingQueryProgress]
            val nativeNodeCounts = ArrayBuffer.empty[Int]
            val rowToColumnarCounts = ArrayBuffer.empty[Int]
            val columnarToRowCounts = ArrayBuffer.empty[Int]
            val sparkOwnedSourceCounts = ArrayBuffer.empty[Int]
            val q6Revenue = inputData
              .toDF()
              .toDF(
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_quantity",
                "l_extendedprice_cents",
                "l_discount_bp",
                "l_shipdate_days",
                "l_commitdate_days")
              .where(
                $"l_shipdate_days" >= q6StartShipDateDays &&
                  $"l_shipdate_days" < q6EndShipDateDays &&
                  $"l_discount_bp" >= q6MinDiscountBp &&
                  $"l_discount_bp" <= q6MaxDiscountBp &&
                  $"l_quantity" < q6MaxQuantity)
              .select(
                $"l_orderkey",
                $"l_partkey",
                $"l_suppkey",
                ($"l_extendedprice_cents" * $"l_discount_bp").as("revenue_cents_bp"),
                $"l_shipdate_days")

            def startQuery(): StreamingQuery = {
              q6Revenue.writeStream
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", checkpointPath)
                .option("path", outputPath)
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
              rowToColumnarCounts += rowToColumnarNodeCount(plan)
              columnarToRowCounts += columnarToRowNodeCount(plan)
              sparkOwnedSourceCounts += sparkOwnedSourceNodeCount(plan)
              if (expectNative) {
                assertSparkOwnedBridgeWithNativeStatelessIsland(plan)
              } else {
                assertNoNativeStatelessIsland(plan)
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

            assertParquetRows(outputPath, expectedQ6OutputRows(firstBatchRows))
            assertCommitEpochs(checkpointDir, Set("0"))

            val restarted = startQuery()
            try {
              inputData.addData(secondBatchRows: _*)
              restarted.processAllAvailable()
              collectEvidence(restarted)
            } finally {
              restarted.stop()
            }

            val expectedRows = expectedQ6OutputRows(firstBatchRows ++ secondBatchRows)
            assertParquetRows(outputPath, expectedRows)
            assertCommitEpochs(checkpointDir, Set("0", "1"))

            val rows = spark.read.parquet(outputPath).collect().toSeq
            val nativeNodes = nativeNodeCounts.sum
            val rowToColumnarNodes = rowToColumnarCounts.sum
            val columnarToRowNodes = columnarToRowCounts.sum
            val sparkOwnedSourceNodes = sparkOwnedSourceCounts.sum
            result = Q6Evidence(
              run = currentRunLabel,
              scenario = "tpch-q6-lineitem-stateless-restart",
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
              rowToColumnarNodes = rowToColumnarNodes,
              columnarToRowNodes = columnarToRowNodes,
              sparkOwnedSourceNodes = sparkOwnedSourceNodes,
              nativePlanNodeShare = planNodeShare(
                nativeNodes,
                rowToColumnarNodes + columnarToRowNodes + sparkOwnedSourceNodes),
              maxBatchDurationMs = maxBatchDurationMs(progress),
              avgProcessedRowsPerSecond = avgProcessedRowsPerSecond(progress)
            )
        }
    }
    assert(result != null)
    result
  }

  private def generateLineitemRows(rowCount: Int): Seq[LineitemRow] = {
    (0 until rowCount).map {
      index =>
        val orderKey = 1L + index.toLong
        val partKey = 1000L + (index % 100000).toLong
        val suppKey = 2000L + (index % 1000).toLong
        val quantity = 1 + (index % 50)
        val extendedPriceCents = 10000L + ((index * 37L) % 250000L)
        val discountBp =
          if (index % 11 == 0) {
            400
          } else if (index % 13 == 0) {
            800
          } else {
            500 + ((index % 3) * 100)
          }
        val shipDateDays =
          if (index % 17 == 0) {
            q6EndShipDateDays + (index % 31)
          } else {
            q6StartShipDateDays + (index % (q6EndShipDateDays - q6StartShipDateDays))
          }
        val commitDateDays = shipDateDays + 3 + (index % 7)
        (
          orderKey,
          partKey,
          suppKey,
          quantity,
          extendedPriceCents,
          discountBp,
          shipDateDays,
          commitDateDays)
    }
  }

  private def expectedQ6OutputRows(rows: Seq[LineitemRow]): Set[Row] = {
    rows.collect {
      case (
            orderKey,
            partKey,
            suppKey,
            quantity,
            extendedPriceCents,
            discountBp,
            shipDateDays,
            _) if qualifiesForQ6(quantity, discountBp, shipDateDays) =>
        Row(orderKey, partKey, suppKey, extendedPriceCents * discountBp, shipDateDays)
    }.toSet
  }

  private def qualifiesForQ6(quantity: Int, discountBp: Int, shipDateDays: Int): Boolean = {
    shipDateDays >= q6StartShipDateDays &&
    shipDateDays < q6EndShipDateDays &&
    discountBp >= q6MinDiscountBp &&
    discountBp <= q6MaxDiscountBp &&
    quantity < q6MaxQuantity
  }

  private def assertQ6EvidenceParity(
      nativeEvidence: Q6Evidence,
      vanillaEvidence: Q6Evidence): Unit = {
    assert(
      nativeEvidence.outputChecksum == vanillaEvidence.outputChecksum,
      s"Native and vanilla TPC-H Q6 streaming output checksums diverged: " +
        s"native=${nativeEvidence.outputChecksum}, vanilla=${vanillaEvidence.outputChecksum}"
    )
    assert(
      nativeEvidence.outputRows == vanillaEvidence.outputRows,
      s"Native and vanilla TPC-H Q6 streaming row counts diverged: " +
        s"native=${nativeEvidence.outputRows}, vanilla=${vanillaEvidence.outputRows}"
    )
    assert(
      nativeEvidence.expectedSourceRows == vanillaEvidence.expectedSourceRows,
      s"Native and vanilla TPC-H Q6 source row counts diverged: " +
        s"native=${nativeEvidence.expectedSourceRows}, " +
        s"vanilla=${vanillaEvidence.expectedSourceRows}"
    )
    assert(
      nativeEvidence.progressInputRows == nativeEvidence.expectedSourceRows,
      s"Native TPC-H Q6 progress input rows should match deterministic source rows: " +
        s"progress=${nativeEvidence.progressInputRows}, " +
        s"expected=${nativeEvidence.expectedSourceRows}"
    )
    assert(
      vanillaEvidence.progressInputRows == vanillaEvidence.expectedSourceRows,
      s"Vanilla TPC-H Q6 progress input rows should match deterministic source rows: " +
        s"progress=${vanillaEvidence.progressInputRows}, " +
        s"expected=${vanillaEvidence.expectedSourceRows}"
    )
    assert(
      nativeEvidence.nativePlanNodes > 0,
      s"Expected native TPC-H Q6 run to contain native project/filter nodes: $nativeEvidence")
    assert(
      vanillaEvidence.nativePlanNodes == 0,
      s"Expected vanilla TPC-H Q6 run to avoid native project/filter nodes: $vanillaEvidence")
  }

  private def assertSparkOwnedBridgeWithNativeStatelessIsland(plan: SparkPlan): Unit = {
    assert(
      plan.collect { case _: ProjectExecTransformer => true }.nonEmpty,
      s"Expected native project in TPC-H Q6 stateless island:\n$plan")
    assert(
      plan.collect { case _: FilterExecTransformer => true }.nonEmpty,
      s"Expected native filter in TPC-H Q6 stateless island:\n$plan")
    assert(
      plan.collect { case _: SparkOwnedMicroBatchScanExec => true }.nonEmpty,
      s"Expected Spark-owned source wrapper in TPC-H Q6 stateless island:\n$plan")
    assert(
      plan.collect { case source: SparkOwnedMicroBatchScanExec => source }
        .exists(_.scan.isInstanceOf[MicroBatchScanExec]),
      s"Expected Spark-owned wrapper to retain Spark MicroBatchScanExec:\n$plan"
    )
    assert(
      plan.collect { case _: RowToVeloxColumnarExec => true }.nonEmpty,
      s"Expected row-to-Velox bridge in TPC-H Q6 stateless island:\n$plan")
    assert(
      plan.collect { case _: VeloxColumnarToRowExec => true }.nonEmpty,
      s"Expected Velox-to-row bridge in TPC-H Q6 stateless island:\n$plan")
    assert(
      plan.collect {
        case native if native.getClass.getName.contains("MicroBatchScanExecTransformer") => native
      }.isEmpty,
      s"TPC-H Q6 benchmark source must stay Spark-owned " +
        "until native source ownership is complete:\n" +
        plan
    )
  }

  private def assertNoNativeStatelessIsland(plan: SparkPlan): Unit = {
    assert(
      nativeStatelessNodeCount(plan) == 0,
      s"Expected vanilla TPC-H Q6 benchmark to avoid native stateless island:\n$plan")
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

  private def rowToColumnarNodeCount(plan: SparkPlan): Int =
    plan.collect { case _: RowToVeloxColumnarExec => 1 }.sum

  private def columnarToRowNodeCount(plan: SparkPlan): Int =
    plan.collect { case _: VeloxColumnarToRowExec => 1 }.sum

  private def sparkOwnedSourceNodeCount(plan: SparkPlan): Int =
    plan.collect { case _: SparkOwnedMicroBatchScanExec => 1 }.sum

  private def assertParquetRows(outputPath: String, expectedRows: Set[Row]): Unit = {
    assert(spark.read.parquet(outputPath).collect().toSet == expectedRows)
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

  private def appendQ6Evidence(reportPath: String, rows: Seq[Q6Evidence]): Unit = {
    val file = new File(reportPath)
    Option(file.getParentFile).foreach(_.mkdirs())
    val shouldWriteHeader = !file.exists() || file.length() == 0L
    val writer = new BufferedWriter(new FileWriter(file, true))
    try {
      if (shouldWriteHeader) {
        writer.write(Q6Evidence.Header)
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

  private val q6StartShipDateDays = 8766
  private val q6EndShipDateDays = 9131
  private val q6MinDiscountBp = 500
  private val q6MaxDiscountBp = 700
  private val q6MaxQuantity = 24

  private case class Q6Evidence(
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
      rowToColumnarNodes: Int,
      columnarToRowNodes: Int,
      sparkOwnedSourceNodes: Int,
      nativePlanNodeShare: String,
      maxBatchDurationMs: Long,
      avgProcessedRowsPerSecond: String) {
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
        avgProcessedRowsPerSecond
      ).mkString("\t")
    }
  }

  private object Q6Evidence {
    val Header: String =
      "run\tscenario\tscale_rows\tmode\tstatus\tprogress_batches\tprogress_input_rows\t" +
        "expected_source_rows\toutput_rows\toutput_checksum\tcommit_epochs\t" +
        "native_plan_nodes\trow_to_columnar_nodes\tcolumnar_to_row_nodes\t" +
        "spark_owned_source_nodes\tnative_plan_node_share\tmax_batch_duration_ms\t" +
        "avg_processed_rows_per_second"
  }
}
