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
import org.apache.gluten.execution.VeloxColumnarToRowExec
import org.apache.gluten.execution.streaming.state.{FailingVeloxNativeStateStoreProvider, VeloxNativeStateStoreJniWrapper, VeloxNativeStateStoreProvider}

import org.apache.spark.sql.{Encoders, GlutenStreamingSQLTestsTrait, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.operators.stateful.{StreamingDeduplicateWithinWatermarkExec, VeloxNativeStreamingCountExec, VeloxNativeStreamingDeduplicateExec, VeloxNativeStreamingLongSumExec}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.{expr, session_window, window}
import org.apache.spark.sql.internal.SQLConf

import java.nio.file.{Files, Path => NioPath}
import java.sql.Timestamp
import java.util.Properties

import scala.jdk.CollectionConverters._
import scala.util.Try

class GlutenVeloxNativeStatefulStreamingSuite extends StreamTest with GlutenStreamingSQLTestsTrait {

  import testImplicits._

  testGluten("production Velox native StateStore provider runs aggregation with restart") {
    withNativeStateStoreProvider {
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().groupBy("value").count()

      testStream(df, OutputMode.Complete())(
        AddData(inputData, 1, 1, 2),
        CheckAnswer(Row(1, 2L), Row(2, 1L)),
        StopStream,
        StartStream(),
        AddData(inputData, 1, 2, 3),
        CheckAnswer(Row(1, 3L), Row(2, 2L), Row(3, 1L))
      )
    }
  }

  testGluten("production Velox native count aggregation physical exec runs with restart") {
    withNativeStatefulAggregation {
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().groupBy("value").count()

      testStream(df, OutputMode.Complete())(
        AddData(inputData, 1, 1, 2),
        CheckAnswer(Row(1, 2L), Row(2, 1L)),
        Execute {
          qe => assertNativeCountAggregationPlan(qe.lastExecution.executedPlan)
        },
        StopStream,
        StartStream(),
        AddData(inputData, 1, 2, 2, 3),
        CheckAnswer(Row(1, 3L), Row(2, 3L), Row(3, 1L)),
        Execute {
          qe =>
            assertNativeCountAggregationPlan(qe.lastExecution.executedPlan)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") == 3L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  testGluten("production Velox native count aggregation physical exec runs in update mode") {
    withNativeStatefulAggregation {
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().groupBy("value").count()

      testStream(df, OutputMode.Update())(
        AddData(inputData, 1, 1, 2),
        CheckNewAnswer(Row(1, 2L), Row(2, 1L)),
        Execute {
          qe =>
            assertNativeCountAggregationPlan(
              qe.lastExecution.executedPlan,
              expectedDirectColumnarInput = Some(false),
              expectNoColumnarToRow = true)
        },
        StopStream,
        StartStream(),
        AddData(inputData, 2, 3),
        CheckNewAnswer(Row(2, 2L), Row(3, 1L)),
        Execute {
          qe =>
            assertNativeCountAggregationPlan(
              qe.lastExecution.executedPlan,
              expectedDirectColumnarInput = Some(false),
              expectNoColumnarToRow = true)
        },
        AddData(inputData, 1, 3, 3),
        CheckNewAnswer(Row(1, 3L), Row(3, 3L)),
        Execute {
          qe =>
            assertNativeCountAggregationPlan(
              qe.lastExecution.executedPlan,
              expectedDirectColumnarInput = Some(false),
              expectNoColumnarToRow = true)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") == 3L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  testGluten("production Velox native count aggregation runs append watermark window count") {
    withNativeStatefulAggregation {
      val inputData = MemoryStream[(Timestamp, String)]
      val df = inputData
        .toDF()
        .toDF("eventTime", "key")
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "10 seconds"), $"key")
        .count()
        .select($"key", $"count")

      testStream(df, OutputMode.Append())(
        AddData(
          inputData,
          ts("2026-01-01 00:00:01") -> "a",
          ts("2026-01-01 00:00:02") -> "a"),
        CheckAnswer(),
        Execute {
          qe => assertNativeCountAggregationPlan(qe.lastExecution.executedPlan)
        },
        AddData(inputData, ts("2026-01-01 00:00:25") -> "b"),
        CheckAnswer(Row("a", 2L)),
        Execute {
          qe => assertNativeCountAggregationPlan(qe.lastExecution.executedPlan)
        },
        AddData(inputData, ts("2026-01-01 00:00:45") -> "c"),
        CheckAnswer(Row("a", 2L), Row("b", 1L)),
        Execute {
          qe =>
            assertNativeCountAggregationPlan(qe.lastExecution.executedPlan)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") == 1L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  testGluten(
    "production Velox native count aggregation skips non-window append watermark rewrite") {
    withNativeStatefulAggregation {
      val inputData = MemoryStream[(Timestamp, String)]
      val df = inputData
        .toDF()
        .toDF("eventTime", "key")
        .withWatermark("eventTime", "10 seconds")
        .groupBy($"eventTime", $"key")
        .count()

      testStream(df, OutputMode.Append())(
        AddData(
          inputData,
          ts("2026-01-01 00:00:01") -> "a",
          ts("2026-01-01 00:00:01") -> "a"),
        CheckAnswer(),
        Execute {
          qe => assertNoNativeCountAggregationPlan(qe.lastExecution.executedPlan)
        },
        AddData(inputData, ts("2026-01-01 00:00:25") -> "b"),
        CheckAnswer(Row(ts("2026-01-01 00:00:01"), "a", 2L)),
        Execute {
          qe => assertNoNativeCountAggregationPlan(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("production Velox native long sum aggregation physical exec runs with restart") {
    withNativeStatefulAggregation {
      val inputData = MemoryStream[(Int, Long)]
      val df = inputData
        .toDF()
        .toDF("key", "amount")
        .groupBy("key")
        .agg(expr("sum(amount)").as("total"))

      testStream(df, OutputMode.Complete())(
        AddData(inputData, 1 -> 10L, 1 -> 2L, 2 -> -3L),
        CheckAnswer(Row(1, 12L), Row(2, -3L)),
        Execute {
          qe => assertNativeLongSumAggregationPlan(qe.lastExecution.executedPlan)
        },
        StopStream,
        StartStream(),
        AddData(inputData, 1 -> -1L, 2 -> 4L, 3 -> 5L),
        CheckAnswer(Row(1, 11L), Row(2, 1L), Row(3, 5L)),
        Execute {
          qe =>
            assertNativeLongSumAggregationPlan(qe.lastExecution.executedPlan)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") == 3L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  testGluten("production Velox native long sum aggregation uses direct columnar filtered input") {
    withNativeStatefulAggregationAndStatelessPreprocessing {
      val inputData = MemoryStream[(Int, Long)]
      val df = inputData
        .toDF()
        .toDF("key", "amount")
        .where($"amount" >= -100L)
        .groupBy($"key")
        .agg(expr("sum(amount)").as("total"))

      testStream(df, OutputMode.Complete())(
        AddData(inputData, 1 -> 10L, 1 -> 2L, 2 -> -3L),
        CheckAnswer(Row(1, 12L), Row(2, -3L)),
        Execute {
          qe =>
            assertNativeLongSumAggregationPlan(
              qe.lastExecution.executedPlan,
              expectedDirectColumnarInput = Some(true),
              expectedDirectFixedWidthUpdateRows = Some(true),
              expectNoColumnarToRow = true)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") == 2L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  testGluten("production Velox native long sum update uses direct columnar filtered input") {
    withNativeStatefulAggregationAndStatelessPreprocessing {
      val inputData = MemoryStream[(Long, Long)]
      val df = inputData
        .toDF()
        .toDF("key", "amount")
        .where($"amount" >= -100L)
        .groupBy($"key")
        .agg(expr("sum(amount)").as("total"))

      testStream(df, OutputMode.Update())(
        AddData(inputData, 1L -> 10L, 1L -> 2L, 2L -> -3L, 3L -> 0L),
        CheckNewAnswer(Row(1L, 12L), Row(2L, -3L), Row(3L, 0L)),
        Execute {
          qe =>
            assertNativeLongSumAggregationPlan(
              qe.lastExecution.executedPlan,
              expectedDirectColumnarInput = Some(true),
              expectedDirectFixedWidthUpdateRows = Some(true),
              expectedDirectTypedInt64UpdateRows = Some(true),
              expectNoColumnarToRow = true)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") == 3L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        },
        AddData(inputData, 1L -> -1L, 2L -> 4L, 3L -> 5L, 4L -> 0L),
        CheckNewAnswer(Row(1L, 11L), Row(2L, 1L), Row(3L, 5L), Row(4L, 0L)),
        Execute {
          qe =>
            assertNativeLongSumAggregationPlan(
              qe.lastExecution.executedPlan,
              expectedDirectColumnarInput = Some(true),
              expectedDirectTypedInt64UpdateRows = Some(true),
              expectNoColumnarToRow = true)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") == 4L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  testGluten("production Velox native long sum aggregation skips nullable sums") {
    withNativeStatefulAggregation {
      val inputData = MemoryStream[(Int, java.lang.Long)]
      val df = inputData
        .toDF()
        .toDF("key", "amount")
        .groupBy("key")
        .agg(expr("sum(amount)").as("total"))

      testStream(df, OutputMode.Complete())(
        AddData(
          inputData,
          1 -> java.lang.Long.valueOf(10L),
          1 -> null.asInstanceOf[java.lang.Long],
          2 -> null.asInstanceOf[java.lang.Long]),
        CheckAnswer(Row(1, 10L), Row(2, null)),
        Execute {
          qe => assertNoNativeLongSumAggregationPlan(qe.lastExecution.executedPlan)
        }
      )
    }
  }

  testGluten("production Velox native count aggregation physical exec writes partitioned state") {
    withNativeStatefulAggregationShufflePartitions(4) {
      withTempDir {
        checkpointDir =>
          val inputData = MemoryStream[Int]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_count_partitioned_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData.toDF().groupBy("value").count()
          var expected = Map.empty[Int, Long]

          val query = df.writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("complete")
            .option("checkpointLocation", checkpointPath)
            .start()

          try {
            val firstBatch = Seq(0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3)
            inputData.addData(firstBatch: _*)
            query.processAllAvailable()
            expected = incrementCounts(expected, firstBatch)
            assert(spark.table(queryName).collect().toSet == intCountsToRows(expected))
            assertNativeCountAggregationPlan(
              query.asInstanceOf[StreamingQueryWrapper]
                .streamingQuery
                .lastExecution
                .executedPlan)

            val secondBatch = Seq(0, 4, 8, 9)
            inputData.addData(secondBatch: _*)
            query.processAllAvailable()
            expected = incrementCounts(expected, secondBatch)
            assert(spark.table(queryName).collect().toSet == intCountsToRows(expected))
            assertNativeCountAggregationPlan(
              query.asInstanceOf[StreamingQueryWrapper]
                .streamingQuery
                .lastExecution
                .executedPlan)
          } finally {
            query.stop()
            spark.catalog.dropTempView(queryName)
          }

          val metadataRows = spark.read
            .format("state-metadata")
            .option("path", checkpointPath)
            .load()
            .select("operatorId", "stateStoreName", "numPartitions", "minBatchId", "maxBatchId")
            .collect()

          assert(
            metadataRows.exists {
              case Row(0L, "default", 4, 0L, 1L) => true
              case _ => false
            },
            s"Expected 4-partition native state metadata, got ${metadataRows.mkString(", ")}"
          )

          val nativeStateRows = spark.read
            .format("statestore")
            .option("path", checkpointPath)
            .load()
            .selectExpr("key.value as value", "value.count as count", "partition_id")
            .collect()
            .toSeq

          val nativeStateCounts = nativeStateRows
            .map(row => Row(row.getInt(0), row.getLong(1)))
            .toSet
          assert(nativeStateCounts == intCountsToRows(expected))
          val statePartitions = nativeStateRows.map(_.getInt(2)).toSet
          assert(
            statePartitions.size > 1 && statePartitions.forall(id => id >= 0 && id < 4),
            s"Expected native state across multiple partitions in [0, 4), got $statePartitions")
      }
    }
  }

  testGluten("production Velox native count aggregation uses direct columnar filtered input") {
    withNativeStatefulAggregationAndStatelessPreprocessing {
      val inputData = MemoryStream[Long]
      val df = inputData.toDF().where($"value" >= 0).groupBy($"value").count()

      testStream(df, OutputMode.Update())(
        AddData(inputData, 1L, 1L, 2L),
        CheckNewAnswer(Row(1L, 2L), Row(2L, 1L)),
        Execute {
          qe =>
            assertNativeCountAggregationPlan(
              qe.lastExecution.executedPlan,
              expectedDirectColumnarInput = Some(true),
              expectedDirectTypedInt64StateInput = Some(true),
              expectedDirectTypedInt64UpdateRows = Some(true),
              expectNoColumnarToRow = true)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") == 2L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  testGluten("production Velox native count complete uses typed columnar state input") {
    withNativeStatefulAggregationAndStatelessPreprocessing {
      val inputData = MemoryStream[Long]
      val df = inputData.toDF().where($"value" >= 0).groupBy($"value").count()

      testStream(df, OutputMode.Complete())(
        AddData(inputData, 1L, 1L, 2L),
        CheckAnswer(Row(1L, 2L), Row(2L, 1L)),
        Execute {
          qe =>
            assertNativeCountAggregationPlan(
              qe.lastExecution.executedPlan,
              expectedDirectColumnarInput = Some(true),
              expectedDirectTypedInt64StateInput = Some(true),
              expectedDirectTypedInt64UpdateRows = Some(false),
              expectNoColumnarToRow = true)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") == 2L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  testGluten("production Velox native StateStore provider runs deduplicate with restart") {
    withNativeStateStoreProvider {
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().dropDuplicates("value")

      testStream(df, OutputMode.Append())(
        AddData(inputData, 1, 1, 2),
        CheckAnswer(Row(1), Row(2)),
        StopStream,
        StartStream(),
        AddData(inputData, 1, 2, 3),
        CheckAnswer(Row(1), Row(2), Row(3))
      )
    }
  }

  testGluten("production Velox native deduplicate physical exec runs with restart") {
    withNativeStatefulDeduplicate {
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().dropDuplicates("value")

      testStream(df, OutputMode.Append())(
        AddData(inputData, 1, 1, 2),
        CheckAnswer(Row(1), Row(2)),
        Execute {
          qe => assertNativeDeduplicatePlan(qe.lastExecution.executedPlan, withinWatermark = false)
        },
        StopStream,
        StartStream(),
        AddData(inputData, 1, 2, 3),
        CheckAnswer(Row(1), Row(2), Row(3)),
        Execute {
          qe => assertNativeDeduplicatePlan(qe.lastExecution.executedPlan, withinWatermark = false)
        }
      )
    }
  }

  testGluten(
    "production Velox native StateStore provider runs deduplicate within watermark with restart") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          withTempDir {
            outputDir =>
              val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
              val baselineIterators = VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles()
              val inputData = MemoryStream[(Timestamp, String)]
              val checkpointPath = checkpointDir.getCanonicalPath
              val outputPath = outputDir.getCanonicalPath
              val df = inputData
                .toDF()
                .toDF("eventTime", "key")
                .withWatermark("eventTime", "10 seconds")
                .dropDuplicatesWithinWatermark("key")
                .select("eventTime", "key")

              var firstQuery: StreamingQuery = null
              try {
                firstQuery = startParquetQuery(df, outputPath, checkpointPath)
                inputData.addData(
                  ts("2026-01-01 00:00:01") -> "a",
                  ts("2026-01-01 00:00:02") -> "a",
                  ts("2026-01-01 00:00:03") -> "b")
                firstQuery.processAllAvailable()
                assertDeduplicateWithinWatermarkPlan(firstQuery)
                assert(
                  outputEventRows(outputPath) ==
                    Set(
                      Row(ts("2026-01-01 00:00:01"), "a"),
                      Row(ts("2026-01-01 00:00:03"), "b")))
              } finally {
                if (firstQuery != null) {
                  firstQuery.stop()
                }
                assertNativeHandleCounts(baselineStores, baselineIterators)
              }

              var restarted: StreamingQuery = null
              try {
                restarted = startParquetQuery(df, outputPath, checkpointPath)

                inputData.addData(
                  ts("2026-01-01 00:00:04") -> "a",
                  ts("2026-01-01 00:00:25") -> "c")
                restarted.processAllAvailable()
                assertDeduplicateWithinWatermarkPlan(restarted)
                assert(
                  outputEventRows(outputPath) ==
                    Set(
                      Row(ts("2026-01-01 00:00:01"), "a"),
                      Row(ts("2026-01-01 00:00:03"), "b"),
                      Row(ts("2026-01-01 00:00:25"), "c")))

                inputData.addData(ts("2026-01-01 00:00:50") -> "d")
                restarted.processAllAvailable()
                assert(
                  outputEventRows(outputPath) ==
                    Set(
                      Row(ts("2026-01-01 00:00:01"), "a"),
                      Row(ts("2026-01-01 00:00:03"), "b"),
                      Row(ts("2026-01-01 00:00:25"), "c"),
                      Row(ts("2026-01-01 00:00:50"), "d")))

                inputData.addData(ts("2026-01-01 00:00:04") -> "a")
                restarted.processAllAvailable()
                assert(
                  outputEventRows(outputPath) ==
                    Set(
                      Row(ts("2026-01-01 00:00:01"), "a"),
                      Row(ts("2026-01-01 00:00:03"), "b"),
                      Row(ts("2026-01-01 00:00:25"), "c"),
                      Row(ts("2026-01-01 00:00:50"), "d")))

                inputData.addData(ts("2026-01-01 00:01:05") -> "a")
                restarted.processAllAvailable()
                assert(
                  outputEventRows(outputPath) ==
                    Set(
                      Row(ts("2026-01-01 00:00:01"), "a"),
                      Row(ts("2026-01-01 00:00:03"), "b"),
                      Row(ts("2026-01-01 00:00:25"), "c"),
                      Row(ts("2026-01-01 00:00:50"), "d"),
                      Row(ts("2026-01-01 00:01:05"), "a")
                    ))
              } finally {
                if (restarted != null) {
                  restarted.stop()
                }
                assertNativeHandleCounts(baselineStores, baselineIterators)
              }

              val nativeStateKeys = spark.read
                .format("statestore")
                .option("path", checkpointPath)
                .load()
                .selectExpr("key.key as key")
                .collect()
                .map(_.getString(0))
                .toSet

              assert(nativeStateKeys.contains("a"))
              assert(nativeStateKeys.contains("d"))
              assert(!nativeStateKeys.contains("b"))
          }
      }
    }
  }

  testGluten("production Velox native deduplicate-with-watermark physical exec runs") {
    withNativeStatefulDeduplicate {
      val inputData = MemoryStream[(Timestamp, String)]
      val df = inputData
        .toDF()
        .toDF("eventTime", "key")
        .withWatermark("eventTime", "10 seconds")
        .dropDuplicatesWithinWatermark("key")
        .select("eventTime", "key")

      testStream(df, OutputMode.Append())(
        AddData(
          inputData,
          ts("2026-01-01 00:00:01") -> "a",
          ts("2026-01-01 00:00:02") -> "a",
          ts("2026-01-01 00:00:03") -> "b"),
        CheckAnswer(
          Row(ts("2026-01-01 00:00:01"), "a"),
          Row(ts("2026-01-01 00:00:03"), "b")),
        Execute {
          qe => assertNativeDeduplicatePlan(qe.lastExecution.executedPlan, withinWatermark = true)
        },
        AddData(
          inputData,
          ts("2026-01-01 00:00:04") -> "a",
          ts("2026-01-01 00:00:25") -> "c"),
        CheckAnswer(
          Row(ts("2026-01-01 00:00:01"), "a"),
          Row(ts("2026-01-01 00:00:03"), "b"),
          Row(ts("2026-01-01 00:00:25"), "c")),
        Execute {
          qe =>
            assertNativeDeduplicatePlan(qe.lastExecution.executedPlan, withinWatermark = true)
            val metrics = nativeStateProgressMetrics(qe)
            assert(metrics("veloxNativeStateStoreNumKeys") >= 1L)
            assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
        }
      )
    }
  }

  testGluten(
    "production Velox native StateStore provider runs watermark aggregation with restart") {
    withNativeStateStoreProvider {
      val inputData = MemoryStream[(Timestamp, String)]
      val df = inputData
        .toDF()
        .toDF("eventTime", "key")
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "10 seconds"), $"key")
        .count()
        .select($"key", $"count")

      testStream(df, OutputMode.Append())(
        AddData(
          inputData,
          ts("2026-01-01 00:00:01") -> "a",
          ts("2026-01-01 00:00:02") -> "a"),
        CheckAnswer(),
        AddData(inputData, ts("2026-01-01 00:00:25") -> "b"),
        CheckAnswer(Row("a", 2L)),
        StopStream,
        StartStream(),
        AddData(inputData, ts("2026-01-01 00:00:45") -> "c"),
        CheckAnswer(Row("a", 2L), Row("b", 1L))
      )
    }
  }

  testGluten("production Velox native StateStore provider is readable by state data source") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val inputData = MemoryStream[Int]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName = s"gluten_native_state_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData.toDF().groupBy("value").count()

          val query = df.writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("complete")
            .option("checkpointLocation", checkpointPath)
            .start()

          try {
            inputData.addData(1, 1, 2)
            query.processAllAvailable()
            inputData.addData(1, 3)
            query.processAllAvailable()

            assert(
              spark.table(queryName).collect().toSet ==
                Set(Row(1, 3L), Row(2, 1L), Row(3, 1L)))
          } finally {
            query.stop()
          }

          val metadataRows = spark.read
            .format("state-metadata")
            .option("path", checkpointPath)
            .load()
            .select("operatorId", "stateStoreName", "numPartitions", "minBatchId", "maxBatchId")
            .collect()

          assert(
            metadataRows.exists {
              case Row(0L, "default", 1, 0L, 1L) => true
              case _ => false
            },
            s"Expected native state metadata for operator 0, got ${metadataRows.mkString(", ")}"
          )

          val nativeStateRows = spark.read
            .format("statestore")
            .option("path", checkpointPath)
            .load()
            .selectExpr("key.value as value", "value.count as count", "partition_id")
            .collect()
            .toSet

          assert(nativeStateRows == Set(Row(1, 3L, 0), Row(2, 1L, 0), Row(3, 1L, 0)))
          spark.catalog.dropTempView(queryName)
      }
    }
  }

  testGluten("production Velox native count aggregation evicts append watermark state") {
    withNativeStatefulAggregation {
      withTempDir {
        checkpointDir =>
          val inputData = MemoryStream[(Timestamp, String)]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_watermark_eviction_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData
            .toDF()
            .toDF("eventTime", "key")
            .withWatermark("eventTime", "10 seconds")
            .groupBy(window($"eventTime", "10 seconds"), $"key")
            .count()

          val query = df.writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .option("checkpointLocation", checkpointPath)
            .start()

          try {
            inputData.addData(
              ts("2026-01-01 00:00:01") -> "a",
              ts("2026-01-01 00:00:02") -> "a")
            query.processAllAvailable()
            assertNativeCountAggregationPlan(
              query.asInstanceOf[StreamingQueryWrapper]
                .streamingQuery
                .lastExecution
                .executedPlan)
            assert(spark.table(queryName).collect().isEmpty)

            inputData.addData(ts("2026-01-01 00:00:25") -> "b")
            query.processAllAvailable()
            assertNativeCountAggregationPlan(
              query.asInstanceOf[StreamingQueryWrapper]
                .streamingQuery
                .lastExecution
                .executedPlan)
            assert(outputKeyCounts(queryName) == Set(Row("a", 2L)))

            inputData.addData(ts("2026-01-01 00:00:45") -> "c")
            query.processAllAvailable()
            assertNativeCountAggregationPlan(
              query.asInstanceOf[StreamingQueryWrapper]
                .streamingQuery
                .lastExecution
                .executedPlan)
            assert(outputKeyCounts(queryName) == Set(Row("a", 2L), Row("b", 1L)))

            inputData.addData(ts("2026-01-01 00:00:03") -> "late-a")
            query.processAllAvailable()
            assertNativeCountAggregationPlan(
              query.asInstanceOf[StreamingQueryWrapper]
                .streamingQuery
                .lastExecution
                .executedPlan)
            assert(outputKeyCounts(queryName) == Set(Row("a", 2L), Row("b", 1L)))
          } finally {
            query.stop()
          }

          val nativeStateRows = spark.read
            .format("statestore")
            .option("path", checkpointPath)
            .load()
            .selectExpr(
              "key.window.start as start",
              "key.window.end as end",
              "key.key as key",
              "value.count as count",
              "partition_id")
            .collect()
            .toSet

          assert(
            nativeStateRows ==
              Set(Row(ts("2026-01-01 00:00:40"), ts("2026-01-01 00:00:50"), "c", 1L, 0)))
          spark.catalog.dropTempView(queryName)
      }
    }
  }

  testGluten("production Velox native StateStore provider survives repeated micro-batch commits") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val inputData = MemoryStream[Int]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_lifecycle_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData.toDF().groupBy("value").count()
          var expected = Map.empty[Int, Long]

          val query = df.writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("complete")
            .option("checkpointLocation", checkpointPath)
            .start()

          try {
            for (batchId <- 0 until 10) {
              val values = Seq(batchId % 3, batchId % 3, (batchId + 1) % 3)
              inputData.addData(values: _*)
              query.processAllAvailable()
              expected = incrementCounts(expected, values)

              assert(
                spark.table(queryName).collect().toSet == intCountsToRows(expected),
                s"Unexpected output after native state commit batch $batchId")
            }
          } finally {
            query.stop()
          }

          val nativeStateRows = spark.read
            .format("statestore")
            .option("path", checkpointPath)
            .load()
            .selectExpr("key.value as value", "value.count as count")
            .collect()
            .toSet

          assert(nativeStateRows == intCountsToRows(expected))
          spark.catalog.dropTempView(queryName)
      }
    }
  }

  testGluten("production Velox native StateStore provider survives repeated checkpoint restarts") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val inputData = MemoryStream[Int]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_restart_lifecycle_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData.toDF().groupBy("value").count()
          var expected = Map.empty[Int, Long]

          def startQuery(): StreamingQuery = {
            df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .start()
          }

          for (batchId <- 0 until 6) {
            val values = Seq(batchId % 4, batchId % 4, (batchId + 1) % 4)
            var query: StreamingQuery = null
            try {
              query = startQuery()
              inputData.addData(values: _*)
              query.processAllAvailable()
              expected = incrementCounts(expected, values)

              assert(
                spark.table(queryName).collect().toSet == intCountsToRows(expected),
                s"Unexpected output after native state checkpoint restart batch $batchId")
            } finally {
              if (query != null) {
                query.stop()
              }
              spark.catalog.dropTempView(queryName)
            }
          }

          val nativeStateRows = spark.read
            .format("statestore")
            .option("path", checkpointPath)
            .load()
            .selectExpr("key.value as value", "value.count as count")
            .collect()
            .toSet

          assert(nativeStateRows == intCountsToRows(expected))
      }
    }
  }

  testGluten("production Velox native StateStore provider survives bounded lifecycle soak") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
          val baselineIterators = VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles()
          val inputData = MemoryStream[Int]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_lifecycle_soak_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData.toDF().groupBy("value").count()
          var expected = Map.empty[Int, Long]
          var observedMaxMemoryBytes = 0L
          val soakCycles = positiveIntTestParam(
            "gluten.native.streaming.state.lifecycleSoakCycles",
            "GLUTEN_NATIVE_STREAMING_STATE_LIFECYCLE_SOAK_CYCLES",
            8)
          val batchesPerCycle = positiveIntTestParam(
            "gluten.native.streaming.state.lifecycleSoakBatchesPerCycle",
            "GLUTEN_NATIVE_STREAMING_STATE_LIFECYCLE_SOAK_BATCHES_PER_CYCLE",
            2)

          def startQuery(): StreamingQuery = {
            df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .start()
          }

          for (cycle <- 0 until soakCycles) {
            var query: StreamingQuery = null
            try {
              query = startQuery()
              for (localBatch <- 0 until batchesPerCycle) {
                val batchId = cycle * batchesPerCycle + localBatch
                val values = Seq(batchId % 12, batchId % 12, (batchId + 1) % 12, (batchId + 5) % 12)
                inputData.addData(values: _*)
                query.processAllAvailable()
                expected = incrementCounts(expected, values)

                assert(
                  spark.table(queryName).collect().toSet == intCountsToRows(expected),
                  s"Unexpected output after native state lifecycle soak batch $batchId")

                val metrics = nativeStateProgressMetrics(query)
                assert(metrics("veloxNativeStateStoreNumKeys") == expected.size)
                assert(metrics("veloxNativeStateStoreMemoryBytes") > 0L)
                observedMaxMemoryBytes =
                  math.max(observedMaxMemoryBytes, metrics("veloxNativeStateStoreMemoryBytes"))
                assertNativeHandleCounts(baselineStores, baselineIterators)
              }
            } finally {
              if (query != null) {
                query.stop()
              }
              spark.catalog.dropTempView(queryName)
            }
            assertNativeHandleCounts(baselineStores, baselineIterators)
            assertNoNativeTempFiles(checkpointDir)
          }

          assert(observedMaxMemoryBytes < 128L * 1024L * 1024L)

          val nativeStateRows = spark.read
            .format("statestore")
            .option("path", checkpointPath)
            .load()
            .selectExpr("key.value as value", "value.count as count")
            .collect()
            .toSet

          assert(nativeStateRows == intCountsToRows(expected))
      }
    }
  }

  testGluten("production Velox native StateStore provider replays missing commit log batch") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val inputData = MemoryStream[Int]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_replay_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData.toDF().groupBy("value").count()
          val firstBatchRows = Set(Row(1, 2L), Row(2, 1L))
          val secondBatchRows = Set(Row(1, 3L), Row(2, 1L), Row(3, 1L))
          var query: StreamingQuery = null

          try {
            query = df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .start()

            inputData.addData(1, 1, 2)
            query.processAllAvailable()
            assert(spark.table(queryName).collect().toSet == firstBatchRows)
          } finally {
            if (query != null) {
              query.stop()
            }
            spark.catalog.dropTempView(queryName)
          }

          val commitLog = checkpointDir.toPath.resolve("commits").resolve("0")
          val checksum = commitLog.resolveSibling(s".${commitLog.getFileName}.crc")
          Files.deleteIfExists(checksum)
          assert(Files.deleteIfExists(commitLog), s"Expected Spark commit log $commitLog")

          var restarted: StreamingQuery = null
          try {
            restarted = df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .start()

            restarted.processAllAvailable()
            assert(spark.table(queryName).collect().toSet == firstBatchRows)

            inputData.addData(1, 3)
            restarted.processAllAvailable()
            assert(spark.table(queryName).collect().toSet == secondBatchRows)
          } finally {
            if (restarted != null) {
              restarted.stop()
            }
            spark.catalog.dropTempView(queryName)
          }

          val nativeStateRows = spark.read
            .format("statestore")
            .option("path", checkpointPath)
            .load()
            .selectExpr("key.value as value", "value.count as count")
            .collect()
            .toSet

          assert(nativeStateRows == secondBatchRows)
      }
    }
  }

  testGluten("production Velox native StateStore provider recovers after commit failure") {
    withFailingNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
          val baselineIterators = VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles()
          val inputData = MemoryStream[Int]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_commit_failure_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData.toDF().groupBy("value").count()
          val expectedRows = Set(Row(1, 2L), Row(2, 1L))

          FailingVeloxNativeStateStoreProvider.failOnce()
          var failedQuery: StreamingQuery = null
          val error = intercept[StreamingQueryException] {
            failedQuery = df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .start()
            inputData.addData(1, 1, 2)
            failedQuery.processAllAvailable()
          }
          try {
            assert(causeMessages(error).contains("injected Velox native StateStore commit failure"))
          } finally {
            FailingVeloxNativeStateStoreProvider.reset()
            if (failedQuery != null) {
              failedQuery.stop()
            }
            spark.catalog.dropTempView(queryName)
          }
          assertNativeHandleCounts(baselineStores, baselineIterators)
          assertNoNativeTempFiles(checkpointDir)
          assertNativeSnapshotFiles(checkpointDir, Set.empty)

          var restarted: StreamingQuery = null
          try {
            restarted = df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .start()
            restarted.processAllAvailable()

            assert(spark.table(queryName).collect().toSet == expectedRows)
          } finally {
            if (restarted != null) {
              restarted.stop()
            }
            spark.catalog.dropTempView(queryName)
          }
          assertNativeHandleCounts(baselineStores, baselineIterators)
          assertNoNativeTempFiles(checkpointDir)
          assertNativeSnapshotFiles(checkpointDir, Set("00000000000000000001.snapshot"))

          val nativeStateRows = spark.read
            .format("statestore")
            .option("path", checkpointPath)
            .load()
            .selectExpr("key.value as value", "value.count as count")
            .collect()
            .toSet

          assert(nativeStateRows == expectedRows)
      }
    }
  }

  testGluten("production Velox native StateStore provider replays after post-commit failure") {
    withFailingNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
          val baselineIterators = VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles()
          val inputData = MemoryStream[Int]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_post_commit_failure_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData.toDF().groupBy("value").count()
          val firstBatchRows = Set(Row(1, 2L), Row(2, 1L))
          val secondBatchRows = Set(Row(1, 2L), Row(2, 2L), Row(3, 1L))

          FailingVeloxNativeStateStoreProvider.failAfterNativeCommitOnce()
          var failedQuery: StreamingQuery = null
          val error = intercept[StreamingQueryException] {
            failedQuery = df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .start()
            inputData.addData(1, 1, 2)
            failedQuery.processAllAvailable()
          }
          try {
            assert(
              causeMessages(error)
                .contains("injected Velox native StateStore post-commit failure"))
          } finally {
            FailingVeloxNativeStateStoreProvider.reset()
            if (failedQuery != null) {
              failedQuery.stop()
            }
            spark.catalog.dropTempView(queryName)
          }
          assertNativeHandleCounts(baselineStores, baselineIterators)
          assertNoNativeTempFiles(checkpointDir)
          assertNativeSnapshotFiles(checkpointDir, Set("00000000000000000001.snapshot"))
          val commitLog = checkpointDir.toPath.resolve("commits").resolve("0")
          assert(!Files.exists(commitLog), s"Unexpected Spark commit log after failure: $commitLog")

          var restarted: StreamingQuery = null
          try {
            restarted = df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .start()
            restarted.processAllAvailable()

            assert(spark.table(queryName).collect().toSet == firstBatchRows)

            inputData.addData(2, 3)
            restarted.processAllAvailable()
            assert(spark.table(queryName).collect().toSet == secondBatchRows)
          } finally {
            if (restarted != null) {
              restarted.stop()
            }
            spark.catalog.dropTempView(queryName)
          }
          assertNativeHandleCounts(baselineStores, baselineIterators)
          assertNoNativeTempFiles(checkpointDir)
          assertNativeSnapshotFiles(
            checkpointDir,
            Set("00000000000000000001.snapshot", "00000000000000000002.snapshot"))

          val nativeStateRows = spark.read
            .format("statestore")
            .option("path", checkpointPath)
            .load()
            .selectExpr("key.value as value", "value.count as count")
            .collect()
            .toSet

          assert(nativeStateRows == secondBatchRows)
      }
    }
  }

  testGluten(
    "production Velox native StateStore provider rejects checkpoint schema mismatch on restart") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val inputData = MemoryStream[Int]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_schema_mismatch_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData.toDF().groupBy("value").count()

          val query = df.writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("complete")
            .option("checkpointLocation", checkpointPath)
            .start()

          try {
            inputData.addData(1, 1, 2)
            query.processAllAvailable()
            assert(spark.table(queryName).collect().toSet == Set(Row(1, 2L), Row(2, 1L)))
          } finally {
            query.stop()
            spark.catalog.dropTempView(queryName)
          }

          corruptNativeStateValueSchema(checkpointDir)

          var restarted: StreamingQuery = null
          val error = intercept[StreamingQueryException] {
            restarted = df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("complete")
              .option("checkpointLocation", checkpointPath)
              .start()
            inputData.addData(3)
            restarted.processAllAvailable()
          }
          try {
            val messages = causeMessages(error)
            assert(messages.contains("Velox native StateStore metadata mismatch"))
            assert(messages.contains("valueSchemaJson"))
          } finally {
            if (restarted != null) {
              restarted.stop()
            }
            spark.catalog.dropTempView(queryName)
          }
      }
    }
  }

  testGluten("production Velox native StateStore provider rejects stream-stream join state") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val leftInput = MemoryStream[(Int, Timestamp)]
          val rightInput = MemoryStream[(Int, Timestamp)]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_stream_join_${checkpointDir.getName.replaceAll("\\W", "_")}"

          val left = leftInput
            .toDF()
            .toDF("id", "leftTime")
            .withWatermark("leftTime", "10 seconds")
            .as("left")
          val right = rightInput
            .toDF()
            .toDF("id", "rightTime")
            .withWatermark("rightTime", "10 seconds")
            .as("right")
          val joined = left
            .join(
              right,
              expr(
                "left.id = right.id AND rightTime >= leftTime AND " +
                  "rightTime <= leftTime + interval 5 seconds"))
            .selectExpr("left.id as id")

          val query = joined.writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .option("checkpointLocation", checkpointPath)
            .start()

          try {
            leftInput.addData(1 -> ts("2026-01-01 00:00:01"))
            rightInput.addData(1 -> ts("2026-01-01 00:00:03"))

            val error = intercept[StreamingQueryException] {
              query.processAllAvailable()
            }
            val messages = causeMessages(error)

            assert(messages.contains("Velox native StateStore supports only default single-value"))
            val unsupportedStateShape = Seq(
              "multiple values per key",
              "key state encoder",
              "column family",
              "stream-stream join state store").exists(messages.contains)
            assert(unsupportedStateShape, messages)
          } finally {
            query.stop()
            spark.catalog.dropTempView(queryName)
          }
      }
    }
  }

  testGluten("production Velox native StateStore provider rejects query-level list state") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val inputData = MemoryStream[String]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_list_state_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val outputEncoder = Encoders.tuple(Encoders.STRING, Encoders.STRING)
          val df = inputData
            .toDS()
            .groupByKey(identity)
            .transformWithState(
              new NativeListStateProcessor,
              TimeMode.None(),
              OutputMode.Append(),
              outputEncoder)
            .toDF("key", "values")

          var query: StreamingQuery = null
          val error = intercept[StreamingQueryException] {
            query = df.writeStream
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .option("checkpointLocation", checkpointPath)
              .start()
            inputData.addData("a", "a")
            query.processAllAvailable()
          }
          try {
            val messages = causeMessages(error)
            assert(messages.contains("Velox native StateStore supports only default single-value"))
            assert(
              messages.contains("column families") ||
                messages.contains("multiple values per key") ||
                messages.contains("putList"),
              messages)
          } finally {
            if (query != null) {
              query.stop()
            }
            spark.catalog.dropTempView(queryName)
          }
      }
    }
  }

  testGluten("production Velox native StateStore provider rejects session-window prefix state") {
    withNativeStateStoreProvider {
      withTempDir {
        checkpointDir =>
          val inputData = MemoryStream[(Timestamp, String)]
          val checkpointPath = checkpointDir.getCanonicalPath
          val queryName =
            s"gluten_native_session_window_${checkpointDir.getName.replaceAll("\\W", "_")}"
          val df = inputData
            .toDF()
            .toDF("eventTime", "key")
            .groupBy($"key", session_window($"eventTime", "10 seconds"))
            .count()

          val query = df.writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("complete")
            .option("checkpointLocation", checkpointPath)
            .start()

          try {
            inputData.addData(ts("2026-01-01 00:00:01") -> "a")

            val error = intercept[StreamingQueryException] {
              query.processAllAvailable()
            }
            val messages = causeMessages(error)

            assert(messages.contains("Velox native StateStore supports only default single-value"))
            assert(messages.contains("key state encoder PrefixKeyScanStateEncoderSpec"))
          } finally {
            query.stop()
            spark.catalog.dropTempView(queryName)
          }
      }
    }
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
    withNativeStatefulAggregationShufflePartitions(1)(testBody)
  }

  private def withNativeStatefulAggregationAndStatelessPreprocessing(testBody: => Unit): Unit = {
    withNativeStatefulAggregation {
      withSQLConf(
        GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
        GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "true",
        GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "true",
        GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "-1",
        GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "-1",
        GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "10000"
      ) {
        testBody
      }
    }
  }

  private def withNativeStatefulAggregationShufflePartitions(
      shufflePartitions: Int)(testBody: => Unit): Unit = {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[VeloxNativeStateStoreProvider].getName,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATEFUL_AGGREGATION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> shufflePartitions.toString,
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      testBody
    }
  }

  private def withFailingNativeStateStoreProvider(testBody: => Unit): Unit = {
    FailingVeloxNativeStateStoreProvider.reset()
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[FailingVeloxNativeStateStoreProvider].getName,
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      try {
        testBody
      } finally {
        FailingVeloxNativeStateStoreProvider.reset()
      }
    }
  }

  private def ts(value: String): Timestamp = Timestamp.valueOf(value)

  private def outputKeyCounts(queryName: String): Set[Row] = {
    spark.table(queryName).select("key", "count").collect().toSet
  }

  private def outputEventRows(outputPath: String): Set[Row] = {
    spark.read.parquet(outputPath).select("eventTime", "key").collect().toSet
  }

  private def startParquetQuery(
      df: org.apache.spark.sql.DataFrame,
      outputPath: String,
      checkpointPath: String): StreamingQuery = {
    df.writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointPath)
      .start()
  }

  private def assertDeduplicateWithinWatermarkPlan(query: StreamingQuery): Unit = {
    val plan = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
    assert(
      plan.collect { case _: StreamingDeduplicateWithinWatermarkExec => true }.nonEmpty,
      s"Expected Spark StreamingDeduplicateWithinWatermarkExec backed by native state:\n$plan"
    )
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

  private def assertNativeCountAggregationPlan(
      plan: SparkPlan,
      expectedDirectColumnarInput: Option[Boolean] = None,
      expectedDirectTypedInt64StateInput: Option[Boolean] = None,
      expectedDirectTypedInt64UpdateRows: Option[Boolean] = None,
      expectNoColumnarToRow: Boolean = false): Unit = {
    val nativeCountNodes = plan.collect {
      case exec: VeloxNativeStreamingCountExec => exec
    }
    assert(
      nativeCountNodes.nonEmpty,
      s"Expected Velox native streaming count aggregation exec:\n$plan"
    )
    expectedDirectColumnarInput.foreach {
      expected =>
        val message = "Expected Velox native streaming count aggregation " +
          s"directColumnarInputEnabled=$expected:\n" +
          plan
        assert(
          nativeCountNodes.exists(_.directColumnarInputEnabled == expected),
          message
        )
    }
    expectedDirectTypedInt64StateInput.foreach {
      expected =>
        val message = "Expected Velox native streaming count aggregation " +
          s"directTypedInt64StateInputEnabled=$expected:\n" +
          plan
        assert(
          nativeCountNodes.exists(_.directTypedInt64StateInputEnabled == expected),
          message
        )
    }
    expectedDirectTypedInt64UpdateRows.foreach {
      expected =>
        val message = "Expected Velox native streaming count aggregation " +
          s"directTypedInt64StateUpdateRowsEnabled=$expected:\n" +
          plan
        assert(
          nativeCountNodes.exists(_.directTypedInt64StateUpdateRowsEnabled == expected),
          message
        )
    }
    if (expectNoColumnarToRow) {
      val columnarToRowNodes = plan.collect {
        case exec: VeloxColumnarToRowExec => exec
      }
      assert(
        columnarToRowNodes.isEmpty,
        s"Expected Velox native streaming count aggregation without C2R bridge:\n$plan")
    }
  }

  private def assertNoNativeCountAggregationPlan(plan: SparkPlan): Unit = {
    val nativeCountNodes = plan.collect {
      case exec: VeloxNativeStreamingCountExec => exec
    }
    assert(
      nativeCountNodes.isEmpty,
      s"Expected Spark-owned aggregation for unsupported native count shape:\n$plan"
    )
  }

  private def assertNativeLongSumAggregationPlan(
      plan: SparkPlan,
      expectedDirectColumnarInput: Option[Boolean] = None,
      expectedDirectFixedWidthUpdateRows: Option[Boolean] = None,
      expectedDirectTypedInt64UpdateRows: Option[Boolean] = None,
      expectNoColumnarToRow: Boolean = false): Unit = {
    val nativeLongSumNodes = plan.collect {
      case exec: VeloxNativeStreamingLongSumExec => exec
    }
    assert(
      nativeLongSumNodes.nonEmpty,
      s"Expected Velox native streaming long sum aggregation exec:\n$plan"
    )
    expectedDirectColumnarInput.foreach {
      expected =>
        val message = "Expected Velox native streaming long sum aggregation " +
          s"directColumnarInputEnabled=$expected:\n" +
          plan
        assert(
          nativeLongSumNodes.exists(_.directColumnarInputEnabled == expected),
          message
        )
    }
    expectedDirectFixedWidthUpdateRows.foreach {
      expected =>
        val message = "Expected Velox native streaming long sum aggregation " +
          s"directFixedWidthUpdateRowsEnabled=$expected:\n" +
          plan
        assert(
          nativeLongSumNodes.exists(_.directFixedWidthUpdateRowsEnabled == expected),
          message
        )
    }
    expectedDirectTypedInt64UpdateRows.foreach {
      expected =>
        val message = "Expected Velox native streaming long sum aggregation " +
          s"directTypedInt64StateUpdateRowsEnabled=$expected:\n" +
          plan
        assert(
          nativeLongSumNodes.exists(_.directTypedInt64StateUpdateRowsEnabled == expected),
          message
        )
    }
    if (expectNoColumnarToRow) {
      val columnarToRowNodes = plan.collect {
        case exec: VeloxColumnarToRowExec => exec
      }
      assert(
        columnarToRowNodes.isEmpty,
        s"Expected Velox native streaming long sum aggregation without C2R bridge:\n$plan")
    }
  }

  private def assertNoNativeLongSumAggregationPlan(plan: SparkPlan): Unit = {
    val nativeLongSumNodes = plan.collect {
      case exec: VeloxNativeStreamingLongSumExec => exec
    }
    assert(
      nativeLongSumNodes.isEmpty,
      s"Expected Spark-owned aggregation for unsupported native long sum shape:\n$plan"
    )
  }

  private def assertNativeHandleCounts(expectedStores: Long, expectedIterators: Long): Unit = {
    assert(VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles() == expectedStores)
    assert(VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles() == expectedIterators)
  }

  private def positiveIntTestParam(property: String, env: String, defaultValue: Int): Int = {
    sys.props.get(property).orElse(sys.env.get(env)).map {
      value =>
        Try(value.toInt)
          .filter(_ > 0)
          .getOrElse(fail(s"$property/$env must be a positive integer, got '$value'"))
    }.getOrElse(defaultValue)
  }

  private def nativeStateProgressMetrics(query: StreamingQuery): Map[String, Long] = {
    val progress = query.lastProgress
    assert(progress != null)
    val stateOperator = progress.stateOperators.headOption.getOrElse {
      fail(s"Expected StateStore progress metrics for ${query.name}")
    }
    stateOperator.customMetrics.asScala.map { case (name, value) => name -> value.toLong }.toMap
  }

  private def incrementCounts(current: Map[Int, Long], values: Seq[Int]): Map[Int, Long] = {
    values.foldLeft(current) {
      case (counts, value) => counts.updated(value, counts.getOrElse(value, 0L) + 1L)
    }
  }

  private def intCountsToRows(counts: Map[Int, Long]): Set[Row] = {
    counts.map { case (value, count) => Row(value, count) }.toSet
  }

  private def corruptNativeStateValueSchema(checkpointDir: java.io.File): Unit = {
    val metadataPath = nativeStateMetadataPath(checkpointDir)
    val hadoopPath = new org.apache.hadoop.fs.Path(metadataPath.toUri)
    val fileSystem = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
    val metadata = new Properties()
    val input = fileSystem.open(hadoopPath)
    try {
      metadata.load(input)
    } finally {
      input.close()
    }

    metadata.setProperty("valueSchemaJson", "changed-native-state-schema")
    val output = fileSystem.create(hadoopPath, true)
    try {
      metadata.store(output, "Corrupted by GlutenVeloxNativeStatefulStreamingSuite")
      output.hflush()
    } finally {
      output.close()
    }
  }

  private def assertNoNativeTempFiles(checkpointDir: java.io.File): Unit = {
    val tempFiles = nativeCheckpointFiles(checkpointDir)
      .filter(_.getFileName.toString.endsWith(".tmp"))
    assert(tempFiles.isEmpty, s"Unexpected Velox native StateStore temp files: $tempFiles")
  }

  private def assertNativeSnapshotFiles(
      checkpointDir: java.io.File,
      expected: Set[String]): Unit = {
    val snapshots = nativeCheckpointFiles(checkpointDir)
      .map(_.getFileName.toString)
      .filter(_.endsWith(".snapshot"))
      .toSet
    assert(snapshots == expected)
  }

  private def nativeCheckpointFiles(checkpointDir: java.io.File): Seq[NioPath] = {
    val stream = Files.walk(checkpointDir.toPath)
    try {
      stream.iterator().asScala.filter(Files.isRegularFile(_)).toSeq
    } finally {
      stream.close()
    }
  }

  private def nativeStateMetadataPath(checkpointDir: java.io.File): NioPath = {
    val stream = Files.walk(checkpointDir.toPath)
    try {
      val matches = stream
        .iterator()
        .asScala
        .filter(_.getFileName.toString == "_metadata.properties")
        .toSeq
      assert(
        matches.size == 1,
        s"Expected exactly one Velox native StateStore metadata file, found " +
          s"${matches.mkString(", ")}")
      matches.head
    } finally {
      stream.close()
    }
  }

  private def causeMessages(error: Throwable): String = {
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(error => Option(error.getMessage))
      .mkString("\n")
  }
}

private class NativeListStateProcessor extends StatefulProcessor[String, String, (String, String)] {
  private var values: ListState[String] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    values = getHandle.getListState("values", Encoders.STRING, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    values.put(rows.toArray)
    Iterator.single(key -> values.get().mkString(","))
  }
}
