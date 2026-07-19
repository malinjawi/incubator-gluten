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
import org.apache.gluten.execution.{FilterExecTransformer, GlutenPlan, ProjectExecTransformer, RowToVeloxColumnarExec, SparkOwnedMicroBatchScanExec, VeloxColumnarToRowExec}
import org.apache.gluten.execution.streaming.state.VeloxNativeStateStoreProvider
import org.apache.gluten.extension.columnar.validator.StreamingPlanSupport

import org.apache.spark.sql.{GlutenStreamingSQLTestsTrait, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.execution.streaming.operators.stateful.{BaseStreamingDeduplicateExec, StateStoreRestoreExec, StateStoreSaveExec, VeloxNativeStreamingCountExec, VeloxNativeStreamingDeduplicateExec}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf

class GlutenStreamingPlanVisibilitySuite extends StreamTest with GlutenStreamingSQLTestsTrait {

  import testImplicits._

  testGluten("streaming fallback trait disables native execution by default") {
    val disabledNativeConfs = Seq(
      "spark.gluten.sql.columnar.batchscan",
      "spark.gluten.sql.columnar.filescan",
      "spark.gluten.sql.columnar.project",
      "spark.gluten.sql.columnar.filter",
      "spark.gluten.sql.columnar.sort",
      "spark.gluten.sql.columnar.window",
      "spark.gluten.sql.columnar.union",
      "spark.gluten.sql.columnar.expand",
      "spark.gluten.sql.columnar.generate",
      "spark.gluten.sql.columnar.coalesce",
      "spark.gluten.sql.columnar.range",
      "spark.gluten.sql.columnar.shuffle",
      "spark.gluten.sql.columnar.hashagg",
      "spark.gluten.sql.columnar.shuffledHashJoin",
      "spark.gluten.sql.columnar.sortMergeJoin",
      "spark.gluten.sql.columnar.broadcastExchange",
      "spark.gluten.sql.columnar.broadcastJoin",
      "spark.gluten.sql.columnar.appendData",
      "spark.gluten.sql.columnar.writeToDataSourceV2",
      "spark.gluten.sql.native.writer.enabled"
    )

    disabledNativeConfs.foreach {
      key =>
        assert(
          spark.conf.get(key) == "false",
          s"$key should stay disabled for broad streaming compatibility suites")
    }

    Seq(
      "spark.gluten.sql.columnar.query.fallback.threshold",
      "spark.gluten.sql.columnar.wholeStage.fallback.threshold",
      "spark.gluten.sql.columnar.fallback.expressions.threshold"
    ).foreach {
      key =>
        assert(
          spark.conf.get(key) == "0",
          s"$key should force fallback for broad streaming compatibility suites")
    }

    assert(spark.conf.get("spark.gluten.sql.columnar.fallback.preferColumnar") == "false")
    assert(spark.conf.get("spark.gluten.expression.blacklist") == "collect_list,collect_set")

    Seq(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_STATEFUL_DEDUPLICATE_ENABLED.key,
      GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key
    ).foreach {
      key =>
        assert(
          spark.conf.get(key) == "false",
          s"$key should remain disabled until native streaming support is explicitly guarded")
    }
  }

  testGluten("stateless micro-batch plan remains vanilla under streaming fallback trait") {
    val inputData = MemoryStream[Int]

    val df = inputData.toDF().where($"value" > 1).select(($"value" + 1).as("value"))

    testStream(df)(
      AddData(inputData, 1, 2, 3),
      CheckAnswer(Row(3), Row(4)),
      Execute {
        qe => assertNoNativePlan(qe.lastExecution.executedPlan)
      }
    )
  }

  testGluten("stateless micro-batch stays vanilla until streaming source and sink are native") {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val inputData = MemoryStream[Int]

      val df = inputData.toDF().where($"value" > 1).select(($"value" + 1).as("value"))

      testStream(df)(
        AddData(inputData, 1, 2, 3),
        CheckAnswer(Row(3), Row(4)),
        Execute {
          qe => assertNoNativePlan(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("non-Kafka micro-batch source stays vanilla when Kafka source gate is enabled") {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val inputData = MemoryStream[Int]

      val df = inputData.toDF().where($"value" > 1).select(($"value" + 1).as("value"))

      testStream(df)(
        AddData(inputData, 1, 2, 3),
        CheckAnswer(Row(3), Row(4)),
        Execute {
          qe => assertNoNativePlan(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("Spark source and sink bridges stay vanilla until bridge execution is hardened") {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val inputData = MemoryStream[Int]

      val df = inputData.toDF().where($"value" > 1).select(($"value" + 1).as("value"))

      testStream(df)(
        AddData(inputData, 1, 2, 3),
        CheckAnswer(Row(3), Row(4)),
        Execute {
          qe => assertNoNativePlan(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("full native mode refuses Spark source and sink bridge POC") {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val inputData = MemoryStream[Int]

      val df = inputData.toDF().where($"value" > 1).select(($"value" + 1).as("value"))

      testStream(df)(
        AddData(inputData, 1, 2, 3),
        CheckAnswer(Row(3), Row(4)),
        Execute {
          qe => assertNoNativePlan(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("Spark source and sink bridge POC executes native stateless micro-batch island") {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val inputData = MemoryStream[Int]

      val df = inputData.toDF().where($"value" > 1).select(($"value" + 1).as("value"))

      testStream(df)(
        AddData(inputData, 1, 2, 3),
        CheckAnswer(Row(3), Row(4)),
        Execute {
          qe => assertSparkOwnedBridgeWithNativeStatelessIsland(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("Spark source and sink bridge POC survives multiple batches and restart") {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val inputData = MemoryStream[Int]

      val df = inputData.toDF().where($"value" > 1).select(($"value" + 1).as("value"))

      testStream(df)(
        AddData(inputData, 1, 2, 3),
        CheckAnswer(Row(3), Row(4)),
        Execute {
          qe => assertSparkOwnedBridgeWithNativeStatelessIsland(qe.lastExecution.executedPlan)
        },
        AddData(inputData, -1, 0, 1),
        CheckAnswer(Row(3), Row(4)),
        Execute {
          qe => assertSparkOwnedBridgeWithNativeStatelessIsland(qe.lastExecution.executedPlan)
        },
        StopStream,
        StartStream(),
        AddData(inputData, 4, 5),
        CheckAnswer(Row(3), Row(4), Row(5), Row(6)),
        Execute {
          qe => assertSparkOwnedBridgeWithNativeStatelessIsland(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("Spark source and sink bridge POC writes parquet sink across checkpoint restart") {
    withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      withTempDir {
        checkpointDir =>
          withTempDir {
            outputDir =>
              val inputData = MemoryStream[Int]
              val outputPath = outputDir.getCanonicalPath
              val checkpointPath = checkpointDir.getCanonicalPath
              val df = inputData.toDF().where($"value" > 1).select(($"value" + 1).as("value"))

              def startQuery(): StreamingQuery = {
                df.writeStream
                  .format("parquet")
                  .option("checkpointLocation", checkpointPath)
                  .option("path", outputPath)
                  .outputMode("append")
                  .start()
              }

              val query1 = startQuery()
              try {
                inputData.addData(1, 2, 3)
                query1.processAllAvailable()
                assertSparkOwnedBridgeWithNativeStatelessIsland(query1)
              } finally {
                query1.stop()
              }
              assertParquetRows(outputPath, Set(Row(3), Row(4)))
              assertCommitEpochs(checkpointDir, Set("0"))

              val query2 = startQuery()
              try {
                inputData.addData(4, 5)
                query2.processAllAvailable()
                assertSparkOwnedBridgeWithNativeStatelessIsland(query2)
              } finally {
                query2.stop()
              }
              assertParquetRows(outputPath, Set(Row(3), Row(4), Row(5), Row(6)))
              assertCommitEpochs(checkpointDir, Set("0", "1"))
          }
      }
    }
  }

  testGluten("native StateStore provider keeps aggregation physical operators Spark-owned") {
    withNativeStateStoreProvider {
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().groupBy("value").count()

      testStream(df, OutputMode.Complete())(
        AddData(inputData, 1, 1, 2),
        CheckAnswer(Row(1, 2L), Row(2, 1L)),
        Execute {
          qe => assertSparkOwnedAggregationState(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("native StateStore provider keeps deduplicate physical operator Spark-owned") {
    withNativeStateStoreProvider {
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().dropDuplicates("value")

      testStream(df, OutputMode.Append())(
        AddData(inputData, 1, 1, 2),
        CheckAnswer(Row(1), Row(2)),
        Execute {
          qe => assertSparkOwnedDeduplicateState(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("native aggregation gate does not enable native deduplicate") {
    withNativeStatefulOperatorGates(deduplicate = false, aggregation = true) {
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().dropDuplicates("value")

      testStream(df, OutputMode.Append())(
        AddData(inputData, 1, 1, 2),
        CheckAnswer(Row(1), Row(2)),
        Execute {
          qe =>
            val plan = qe.lastExecution.executedPlan
            assertSparkOwnedDeduplicateState(plan)
            assert(
              plan.collect { case _: VeloxNativeStreamingDeduplicateExec => true }.isEmpty,
              s"Native aggregation gate should not enable native deduplicate:\n$plan")
        }
      )
    }
  }

  testGluten("native deduplicate gate does not enable native aggregation") {
    withNativeStatefulOperatorGates(deduplicate = true, aggregation = false) {
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().groupBy("value").count()

      testStream(df, OutputMode.Complete())(
        AddData(inputData, 1, 1, 2),
        CheckAnswer(Row(1, 2L), Row(2, 1L)),
        Execute {
          qe =>
            val plan = qe.lastExecution.executedPlan
            assertSparkOwnedAggregationState(plan)
            assert(
              plan.collect { case _: VeloxNativeStreamingCountExec => true }.isEmpty,
              s"Native deduplicate gate should not enable native count aggregation:\n$plan")
        }
      )
    }
  }

  private def assertNoNativePlan(plan: SparkPlan): Unit = {
    val nativeNodes = plan.collect { case glutenPlan: GlutenPlan => glutenPlan }
    assert(
      nativeNodes.isEmpty,
      s"Expected streaming compatibility plan to remain vanilla, but found native nodes: " +
        s"${nativeNodes.mkString(", ")}\n$plan")
  }

  private def assertSparkOwnedAggregationState(plan: SparkPlan): Unit = {
    val restoreNodes = plan.collect { case restore: StateStoreRestoreExec => restore }
    assert(
      restoreNodes.nonEmpty,
      s"Expected Spark StateStoreRestoreExec with native provider-backed state:\n$plan")

    val saveNodes = plan.collect { case save: StateStoreSaveExec => save }
    assert(
      saveNodes.nonEmpty,
      s"Expected Spark StateStoreSaveExec with native provider-backed state:\n$plan")

    assertNoNativeStatefulPlan(plan)
  }

  private def assertSparkOwnedDeduplicateState(plan: SparkPlan): Unit = {
    val dedupeNodes = plan.collect { case dedupe: BaseStreamingDeduplicateExec => dedupe }
    assert(
      dedupeNodes.nonEmpty,
      s"Expected Spark streaming deduplicate exec with native provider-backed state:\n$plan")

    assertNoNativeStatefulPlan(plan)
  }

  private def assertNoNativeStatefulPlan(plan: SparkPlan): Unit = {
    val nativeStatefulNodes = plan.collect {
      case glutenPlan: GlutenPlan
          if StreamingPlanSupport.isStatefulStreamingClassName(glutenPlan.getClass.getName) =>
        glutenPlan
    }
    assert(
      nativeStatefulNodes.isEmpty,
      s"Expected native StateStore provider to leave stateful Spark physical operators in place, " +
        s"but found native stateful nodes: ${nativeStatefulNodes.mkString(", ")}\n$plan"
    )
  }

  private def withNativeStateStoreProvider(testBody: => Unit): Unit = {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[VeloxNativeStateStoreProvider].getName,
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      testBody
    }
  }

  private def withNativeStatefulOperatorGates(
      deduplicate: Boolean,
      aggregation: Boolean)(testBody: => Unit): Unit = {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[VeloxNativeStateStoreProvider].getName,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATEFUL_DEDUPLICATE_ENABLED.key -> deduplicate.toString,
      GlutenConfig.NATIVE_STREAMING_STATEFUL_AGGREGATION_ENABLED.key -> aggregation.toString,
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

  private def assertSparkOwnedBridgeWithNativeStatelessIsland(query: StreamingQuery): Unit = {
    val plan = query
      .asInstanceOf[StreamingQueryWrapper]
      .streamingQuery
      .lastExecution
      .executedPlan
    assertSparkOwnedBridgeWithNativeStatelessIsland(plan)
  }

  private def assertSparkOwnedBridgeWithNativeStatelessIsland(plan: SparkPlan): Unit = {
    val nativeStatelessNodes = plan.collect {
      case project: ProjectExecTransformer => project
      case filter: FilterExecTransformer => filter
    }
    assert(
      nativeStatelessNodes.nonEmpty,
      s"Expected native stateless operators inside streaming micro-batch bridge:\n$plan")

    val sparkSources = plan.collect { case source: SparkOwnedMicroBatchScanExec => source }
    assert(
      sparkSources.nonEmpty,
      s"Expected Spark-owned streaming source wrapper to remain in the plan:\n$plan")
    assert(
      sparkSources.exists(_.scan.isInstanceOf[MicroBatchScanExec]),
      s"Expected Spark-owned wrapper to retain the Spark MicroBatchScanExec:\n$plan")

    val rowToVeloxNodes = plan.collect { case rowToVelox: RowToVeloxColumnarExec => rowToVelox }
    assert(
      rowToVeloxNodes.nonEmpty,
      s"Expected row-to-Velox bridge above the Spark-owned streaming source:\n$plan")

    val veloxToRowNodes = plan.collect { case veloxToRow: VeloxColumnarToRowExec => veloxToRow }
    assert(
      veloxToRowNodes.nonEmpty,
      s"Expected Velox-to-row bridge before the Spark-owned streaming sink:\n$plan")

    val nativeSourceNodes = plan.collect {
      case native if native.getClass.getName.contains("MicroBatchScanExecTransformer") => native
    }
    assert(
      nativeSourceNodes.isEmpty,
      s"Expected streaming source to stay Spark-owned, but found: " +
        s"${nativeSourceNodes.mkString(", ")}\n$plan")

    val nativeSinkNodes = plan.collect {
      case native if native.getClass.getName.contains("WriteToDataSourceV2ExecTransformer") =>
        native
      case native if native.getClass.getName.contains("AppendDataExecTransformer") => native
    }
    assert(
      nativeSinkNodes.isEmpty,
      s"Expected streaming sink to stay Spark-owned, but found: " +
        s"${nativeSinkNodes.mkString(", ")}\n$plan")
  }

  private def assertParquetRows(outputPath: String, expectedRows: Set[Row]): Unit = {
    assert(spark.read.parquet(outputPath).collect().toSet == expectedRows)
  }

  private def assertCommitEpochs(checkpointDir: java.io.File, expectedEpochs: Set[String]): Unit = {
    val commitDir = new java.io.File(checkpointDir, "commits")
    val committedEpochs = Option(commitDir.list())
      .map(_.filterNot(_.startsWith(".")).toSet)
      .getOrElse(Set.empty[String])
    assert(committedEpochs == expectedEpochs)
  }
}
