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
import org.apache.gluten.execution.{FilterExecTransformer, ProjectExecTransformer, RowToVeloxColumnarExec, SparkOwnedMicroBatchScanExec, VeloxColumnarToRowExec}
import org.apache.gluten.execution.streaming.state.{VeloxNativeStateStoreJniWrapper, VeloxNativeStateStoreProvider}

import org.apache.spark.sql.{GlutenStreamingSQLTestsTrait, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.execution.streaming.operators.stateful.{VeloxNativeStreamingCountExec, VeloxNativeStreamingDeduplicateExec}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.Timestamp
import java.time.Instant

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class GlutenVeloxNativeStreamingShowcaseSuite
  extends StreamTest
  with GlutenStreamingSQLTestsTrait {

  import testImplicits._

  override def afterAll(): Unit = {
    try {
      captureThreadSnapshotIfRequested()
    } finally {
      super.afterAll()
    }
  }

  testGluten("showcase fraud-risk stateless native island writes parquet across restart") {
    val nativeEvidence = runNativeFraudRiskStatelessScenario()

    assert(
      nativeEvidence.progressInputRows == nativeEvidence.expectedSourceRows,
      s"Spark progress input rows should remain visible through the Spark-owned native bridge: " +
        s"progress=${nativeEvidence.progressInputRows}, " +
        s"expected=${nativeEvidence.expectedSourceRows}"
    )

    pairedEvidenceReportPath.foreach {
      reportPath =>
        scaledComparisonBatches match {
          case Some((firstBatchRows, secondBatchRows)) =>
            val scaledNativeEvidence = runNativeFraudRiskStatelessScenario(
              scenario = "fraud-risk-stateless-scaled",
              firstBatchRows = firstBatchRows,
              secondBatchRows = secondBatchRows,
              scaleRows = firstBatchRows.size + secondBatchRows.size
            )
            val scaledVanillaEvidence = runVanillaFraudRiskStatelessScenario(
              scenario = "fraud-risk-stateless-scaled",
              firstBatchRows = firstBatchRows,
              secondBatchRows = secondBatchRows,
              scaleRows = firstBatchRows.size + secondBatchRows.size
            )

            assertFraudRiskEvidenceParity(
              scaledNativeEvidence,
              scaledVanillaEvidence,
              "scaled fraud-risk")

            appendFraudRiskEvidence(
              reportPath,
              Seq(
                scaledNativeEvidence.copy(status = "PASS"),
                scaledVanillaEvidence.copy(status = "PASS")))

          case None =>
            val vanillaEvidence = runVanillaFraudRiskStatelessScenario()
            assertFraudRiskEvidenceParity(nativeEvidence, vanillaEvidence, "fraud-risk")

            appendFraudRiskEvidence(
              reportPath,
              Seq(nativeEvidence.copy(status = "PASS"), vanillaEvidence.copy(status = "PASS")))
        }
    }
  }

  testGluten("showcase alert idempotency uses native dedupe with watermark") {
    withNativeStatefulDeduplicate {
      val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
      val baselineIterators = VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles()
      val inputData = MemoryStream[(Timestamp, String)]
      val alerts = inputData
        .toDF()
        .toDF("eventTime", "alertId")
        .withWatermark("eventTime", "10 seconds")
        .dropDuplicatesWithinWatermark("alertId")
        .select("eventTime", "alertId")

      testStream(alerts, OutputMode.Append())(
        AddData(
          inputData,
          ts("2026-01-01 00:00:01") -> "a-1",
          ts("2026-01-01 00:00:02") -> "a-1",
          ts("2026-01-01 00:00:03") -> "b-1"),
        CheckAnswer(
          Row(ts("2026-01-01 00:00:01"), "a-1"),
          Row(ts("2026-01-01 00:00:03"), "b-1")),
        Execute {
          query =>
            assertNativeDeduplicatePlan(query.lastExecution.executedPlan, withinWatermark = true)
            assertNativeHandleCounts(baselineStores, baselineIterators)
        },
        AddData(
          inputData,
          ts("2026-01-01 00:00:04") -> "a-1",
          ts("2026-01-01 00:00:25") -> "c-1"),
        CheckAnswer(
          Row(ts("2026-01-01 00:00:01"), "a-1"),
          Row(ts("2026-01-01 00:00:03"), "b-1"),
          Row(ts("2026-01-01 00:00:25"), "c-1")),
        Execute {
          query =>
            assertNativeDeduplicatePlan(query.lastExecution.executedPlan, withinWatermark = true)
            val metrics = nativeStateProgressMetrics(query)
            assert(metrics("veloxNativeStateStoreNumKeys") >= 1L)
        }
      )
    }
  }

  testGluten("showcase account velocity uses native grouped count across restart") {
    withNativeStatefulAggregation {
      val inputData = MemoryStream[(Long, Long)]
      val accountVelocity = inputData
        .toDF()
        .toDF("accountId", "eventId")
        .groupBy("accountId")
        .count()

      testStream(accountVelocity, OutputMode.Update())(
        AddData(inputData, 7L -> 1001L, 7L -> 1002L, 8L -> 1003L),
        CheckNewAnswer(Row(7L, 2L), Row(8L, 1L)),
        Execute {
          query =>
            assertNativeCountAggregationPlan(query.lastExecution.executedPlan)
            val metrics = nativeStateProgressMetrics(query)
            assert(metrics("veloxNativeStateStoreNumKeys") == 2L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        },
        StopStream,
        StartStream(),
        AddData(inputData, 7L -> 1004L, 9L -> 1005L),
        CheckNewAnswer(Row(7L, 3L), Row(9L, 1L)),
        Execute {
          query =>
            assertNativeCountAggregationPlan(query.lastExecution.executedPlan)
            val metrics = nativeStateProgressMetrics(query)
            assert(metrics("veloxNativeStateStoreNumKeys") == 3L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  private def withStatelessBridge(testBody: => Unit): Unit = {
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

  private def withNativeStatefulDeduplicate(testBody: => Unit): Unit = {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[VeloxNativeStateStoreProvider].getName,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATEFUL_DEDUPLICATE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      testBody
    }
  }

  private def withNativeStatefulAggregation(testBody: => Unit): Unit = {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[VeloxNativeStateStoreProvider].getName,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATEFUL_AGGREGATION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      testBody
    }
  }

  private def withVanillaStreaming(testBody: => Unit): Unit = {
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

  private type FraudRiskInputRow = (Long, Int, Long)

  private def runNativeFraudRiskStatelessScenario(
      scenario: String = "fraud-risk-stateless-restart",
      firstBatchRows: Seq[FraudRiskInputRow] = defaultFraudRiskFirstBatchRows,
      secondBatchRows: Seq[FraudRiskInputRow] =
        defaultFraudRiskSecondBatchRows,
      scaleRows: Int = 0): FraudRiskEvidence = {
    var result: FraudRiskEvidence = null
    withStatelessBridge {
      result = runFraudRiskStatelessScenario(
        mode = "native",
        expectNative = true,
        scenario = scenario,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = scaleRows)
    }
    result
  }

  private def runVanillaFraudRiskStatelessScenario(
      scenario: String = "fraud-risk-stateless-restart",
      firstBatchRows: Seq[FraudRiskInputRow] = defaultFraudRiskFirstBatchRows,
      secondBatchRows: Seq[FraudRiskInputRow] =
        defaultFraudRiskSecondBatchRows,
      scaleRows: Int = 0): FraudRiskEvidence = {
    var result: FraudRiskEvidence = null
    withVanillaStreaming {
      result = runFraudRiskStatelessScenario(
        mode = "vanilla",
        expectNative = false,
        scenario = scenario,
        firstBatchRows = firstBatchRows,
        secondBatchRows = secondBatchRows,
        scaleRows = scaleRows)
    }
    result
  }

  private def runFraudRiskStatelessScenario(
      mode: String,
      expectNative: Boolean,
      scenario: String,
      firstBatchRows: Seq[FraudRiskInputRow],
      secondBatchRows: Seq[FraudRiskInputRow],
      scaleRows: Int): FraudRiskEvidence = {
    var result: FraudRiskEvidence = null
    withTempDir {
      checkpointDir =>
        withTempDir {
          outputDir =>
            val inputData = MemoryStream[(Long, Int, Long)]
            val checkpointPath = checkpointDir.getCanonicalPath
            val outputPath = outputDir.getCanonicalPath
            val progress = ArrayBuffer.empty[StreamingQueryProgress]
            val nativeNodeCounts = ArrayBuffer.empty[Int]
            val riskyTransactions = inputData
              .toDF()
              .toDF("accountId", "merchantId", "amountCents")
              .where($"amountCents" >= 10000L)
              .select($"accountId", ($"amountCents" + 100L).as("riskScore"))

            def startQuery(): StreamingQuery = {
              riskyTransactions.writeStream
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
              if (expectNative) {
                assertSparkOwnedBridgeWithNativeStatelessIsland(query)
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

            assertParquetRows(outputPath, expectedFraudRiskOutputRows(firstBatchRows))
            assertCommitEpochs(checkpointDir, Set("0"))

            val restarted = startQuery()
            try {
              inputData.addData(secondBatchRows: _*)
              restarted.processAllAvailable()
              collectEvidence(restarted)
            } finally {
              restarted.stop()
            }

            val expectedRows = expectedFraudRiskOutputRows(firstBatchRows ++ secondBatchRows)
            assertParquetRows(outputPath, expectedRows)
            assertCommitEpochs(checkpointDir, Set("0", "1"))

            val rows = spark.read.parquet(outputPath).collect().toSeq
            result = FraudRiskEvidence(
              run = currentEvidenceRunLabel,
              scenario = scenario,
              scaleRows = scaleRows,
              mode = mode,
              status = "PASS",
              progressBatches = progress.length,
              progressInputRows = progress.map(_.numInputRows).sum,
              expectedSourceRows = firstBatchRows.size + secondBatchRows.size,
              outputRows = rows.length,
              outputChecksum = checksumRows(rows),
              commitEpochs = committedEpochs(checkpointDir).toSeq.sorted.mkString(","),
              nativePlanNodes = nativeNodeCounts.sum,
              maxBatchDurationMs = maxBatchDurationMs(progress),
              avgProcessedRowsPerSecond = avgProcessedRowsPerSecond(progress)
            )
        }
    }
    assert(result != null)
    result
  }

  private def defaultFraudRiskFirstBatchRows: Seq[FraudRiskInputRow] = {
    Seq((7L, 101, 15000L), (8L, 102, 2000L), (7L, 103, 20000L))
  }

  private def defaultFraudRiskSecondBatchRows: Seq[FraudRiskInputRow] = {
    Seq((9L, 104, 50000L), (8L, 105, 9000L))
  }

  private def scaledComparisonBatches: Option[(Seq[FraudRiskInputRow], Seq[FraudRiskInputRow])] = {
    val rowCount = sys.env
      .get("GLUTEN_NATIVE_STREAMING_USE_CASE_SCALE_ROWS")
      .filter(_.nonEmpty)
      .map {
        value =>
          assert(
            value.matches("[0-9]+"),
            s"GLUTEN_NATIVE_STREAMING_USE_CASE_SCALE_ROWS must be a non-negative integer: $value")
          value.toInt
      }
      .getOrElse(0)

    if (rowCount == 0) {
      None
    } else {
      assert(
        rowCount >= 2,
        s"GLUTEN_NATIVE_STREAMING_USE_CASE_SCALE_ROWS must be 0 to disable or at least 2: " +
          rowCount)
      val rows = (0 until rowCount).map {
        index =>
          val amountCents = if (index % 3 == 0) 5000L else 10000L + index.toLong
          (1000000L + index.toLong, 100 + (index % 97), amountCents)
      }
      val splitAt = math.max(1, rowCount / 2)
      Some((rows.take(splitAt), rows.drop(splitAt)))
    }
  }

  private def expectedFraudRiskOutputRows(rows: Seq[FraudRiskInputRow]): Set[Row] = {
    rows.collect {
      case (accountId, _, amountCents) if amountCents >= 10000L =>
        Row(accountId, amountCents + 100L)
    }.toSet
  }

  private def assertFraudRiskEvidenceParity(
      nativeEvidence: FraudRiskEvidence,
      vanillaEvidence: FraudRiskEvidence,
      label: String): Unit = {
    assert(
      nativeEvidence.outputChecksum == vanillaEvidence.outputChecksum,
      s"Native and vanilla $label output checksums diverged: " +
        s"native=${nativeEvidence.outputChecksum}, vanilla=${vanillaEvidence.outputChecksum}"
    )
    assert(
      nativeEvidence.outputRows == vanillaEvidence.outputRows,
      s"Native and vanilla $label output row counts diverged: " +
        s"native=${nativeEvidence.outputRows}, vanilla=${vanillaEvidence.outputRows}"
    )
    assert(
      nativeEvidence.expectedSourceRows == vanillaEvidence.expectedSourceRows,
      s"Native and vanilla $label expected source row counts diverged: " +
        s"native=${nativeEvidence.expectedSourceRows}, " +
        s"vanilla=${vanillaEvidence.expectedSourceRows}"
    )
    assert(
      nativeEvidence.progressInputRows == nativeEvidence.expectedSourceRows,
      s"Native $label progress input rows should match deterministic source rows: " +
        s"progress=${nativeEvidence.progressInputRows}, " +
        s"expected=${nativeEvidence.expectedSourceRows}"
    )
    assert(
      vanillaEvidence.progressInputRows == vanillaEvidence.expectedSourceRows,
      s"Vanilla $label progress input rows should match deterministic source rows: " +
        s"progress=${vanillaEvidence.progressInputRows}, " +
        s"expected=${vanillaEvidence.expectedSourceRows}"
    )
  }

  private def assertSparkOwnedBridgeWithNativeStatelessIsland(query: StreamingQuery): Unit = {
    val plan = executedPlan(query)

    assert(
      plan.collect { case _: ProjectExecTransformer => true }.nonEmpty,
      s"Expected native project in showcase stateless island:\n$plan")
    assert(
      plan.collect { case _: FilterExecTransformer => true }.nonEmpty,
      s"Expected native filter in showcase stateless island:\n$plan")
    assert(
      plan.collect { case _: SparkOwnedMicroBatchScanExec => true }.nonEmpty,
      s"Expected Spark-owned source wrapper in showcase stateless island:\n$plan")
    assert(
      plan.collect { case source: SparkOwnedMicroBatchScanExec => source }
        .exists(_.scan.isInstanceOf[MicroBatchScanExec]),
      s"Expected Spark-owned wrapper to retain Spark MicroBatchScanExec:\n$plan"
    )
    assert(
      plan.collect { case _: RowToVeloxColumnarExec => true }.nonEmpty,
      s"Expected row-to-Velox bridge in showcase stateless island:\n$plan")
    assert(
      plan.collect { case _: VeloxColumnarToRowExec => true }.nonEmpty,
      s"Expected Velox-to-row bridge in showcase stateless island:\n$plan")
    assert(
      plan.collect {
        case native if native.getClass.getName.contains("MicroBatchScanExecTransformer") => native
      }.isEmpty,
      s"Showcase source must stay Spark-owned until native source ownership is complete:\n$plan"
    )
  }

  private def assertNoNativeStatelessIsland(plan: SparkPlan): Unit = {
    assert(
      nativeStatelessNodeCount(plan) == 0,
      s"Expected vanilla showcase run to avoid native stateless island:\n$plan")
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
      case _: RowToVeloxColumnarExec => 1
      case _: VeloxColumnarToRowExec => 1
    }.sum
  }

  private def assertNativeDeduplicatePlan(plan: SparkPlan, withinWatermark: Boolean): Unit = {
    val nativeDedupeNodes = plan.collect {
      case exec: VeloxNativeStreamingDeduplicateExec => exec
    }
    assert(
      nativeDedupeNodes.exists(_.withinWatermark == withinWatermark),
      s"Expected Velox native streaming deduplicate exec with withinWatermark=$withinWatermark:\n" +
        plan
    )
  }

  private def assertNativeCountAggregationPlan(plan: SparkPlan): Unit = {
    val nativeCountNodes = plan.collect {
      case exec: VeloxNativeStreamingCountExec => exec
    }
    assert(
      nativeCountNodes.nonEmpty,
      s"Expected Velox native streaming count aggregation exec:\n$plan"
    )
  }

  private def nativeStateProgressMetrics(query: StreamingQuery): Map[String, Long] = {
    val progress = query.lastProgress
    assert(progress != null)
    val stateOperator = progress.stateOperators.headOption.getOrElse {
      fail(s"Expected StateStore progress metrics for ${query.name}")
    }
    stateOperator.customMetrics.asScala.map { case (name, value) => name -> value.toLong }.toMap
  }

  private def assertNativeHandleCounts(expectedStores: Long, expectedIterators: Long): Unit = {
    assert(VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles() == expectedStores)
    assert(VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles() == expectedIterators)
  }

  private def assertParquetRows(outputPath: String, expectedRows: Set[Row]): Unit = {
    assert(spark.read.parquet(outputPath).collect().toSet == expectedRows)
  }

  private def assertCommitEpochs(checkpointDir: java.io.File, expectedEpochs: Set[String]): Unit = {
    assert(committedEpochs(checkpointDir) == expectedEpochs)
  }

  private def committedEpochs(checkpointDir: java.io.File): Set[String] = {
    val commitDir = new java.io.File(checkpointDir, "commits")
    Option(commitDir.list())
      .map(_.filterNot(_.startsWith(".")).toSet)
      .getOrElse(Set.empty[String])
  }

  private def ts(value: String): Timestamp = Timestamp.valueOf(value)

  private def pairedEvidenceReportPath: Option[String] = {
    sys.env.get("GLUTEN_NATIVE_STREAMING_USE_CASE_COMPARISON_REPORT").filter(_.nonEmpty)
  }

  private def currentEvidenceRunLabel: String = {
    sys.env
      .get("GLUTEN_NATIVE_STREAMING_USE_CASE_CURRENT_RUN_LABEL")
      .orElse(sys.env.get("GLUTEN_NATIVE_STREAMING_USE_CASE_RUN_LABEL"))
      .filter(_.nonEmpty)
      .getOrElse("single")
  }

  private def captureThreadSnapshotIfRequested(): Unit = {
    sys.env
      .get("GLUTEN_NATIVE_STREAMING_USE_CASE_THREAD_SNAPSHOT_PATH")
      .filter(_.nonEmpty)
      .foreach {
        path =>
          val file = new File(path)
          Option(file.getParentFile).foreach(_.mkdirs())
          val writer = new BufferedWriter(new FileWriter(file, false))
          try {
            writer.write(s"Captured at ${Instant.now()}")
            writer.newLine()
            writer.write("Full thread dump (in-JVM before Spark suite teardown)")
            writer.newLine()
            writer.write(s"Suite: ${getClass.getName}")
            writer.newLine()
            writer.newLine()

            Thread.getAllStackTraces.asScala.toSeq
              .sortBy {
                case (thread, _) => (thread.getName, thread.getId)
              }
              .foreach {
                case (thread, stackTrace) =>
                  writer.write(
                    s""""${thread.getName}" #${thread.getId} prio=${thread.getPriority} """ +
                      s"daemon=${thread.isDaemon} java.lang.Thread.State: ${thread.getState}")
                  writer.newLine()
                  stackTrace.foreach {
                    frame =>
                      writer.write(s"\tat $frame")
                      writer.newLine()
                  }
                  writer.newLine()
              }
          } finally {
            writer.close()
          }
      }
  }

  private def appendFraudRiskEvidence(reportPath: String, rows: Seq[FraudRiskEvidence]): Unit = {
    val file = new File(reportPath)
    Option(file.getParentFile).foreach(_.mkdirs())
    val shouldWriteHeader = !file.exists() || file.length() == 0L
    val writer = new BufferedWriter(new FileWriter(file, true))
    try {
      if (shouldWriteHeader) {
        writer.write(FraudRiskEvidence.Header)
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

  private case class FraudRiskEvidence(
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
        maxBatchDurationMs.toString,
        avgProcessedRowsPerSecond
      ).mkString("\t")
    }
  }

  private object FraudRiskEvidence {
    val Header: String =
      "run\tscenario\tscale_rows\tmode\tstatus\tprogress_batches\tprogress_input_rows\t" +
        "expected_source_rows\toutput_rows\toutput_checksum\tcommit_epochs\t" +
        "native_plan_nodes\tmax_batch_duration_ms\t" +
        "avg_processed_rows_per_second"
  }
}
