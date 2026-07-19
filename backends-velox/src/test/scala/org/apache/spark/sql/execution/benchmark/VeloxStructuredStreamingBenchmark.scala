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
package org.apache.spark.sql.execution.benchmark

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.Trigger

import java.io.File

/**
 * Benchmark to measure present-day Structured Streaming behavior with Gluten enabled but native
 * streaming fenced off. To run this benchmark:
 * {{{
 *   bin/spark-submit --class <this class> \
 *     --conf spark.plugins=org.apache.gluten.GlutenPlugin \
 *     --conf spark.memory.offHeap.enabled=true \
 *     --conf spark.memory.offHeap.size=1g \
 *     --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
 *     --jars <spark core test jar> <sql core test jar>
 * }}}
 *
 * This is not a native streaming speed benchmark yet. It measures the fallback compatibility tax
 * while streaming source, sink, and state boundaries remain Spark-owned.
 */
object VeloxStructuredStreamingBenchmark extends SqlBasedBenchmark {
  private val rowsPerBatch =
    spark.sparkContext.conf.getLong(
      "spark.gluten.benchmark.streaming.rowsPerBatch",
      2 * 1000 * 1000)
  private val restartCount =
    spark.sparkContext.conf.getInt("spark.gluten.benchmark.streaming.restarts", 5)
  private val numPartitions =
    spark.sparkContext.conf.getInt("spark.gluten.benchmark.streaming.partitions", 8)
  private val totalRows = rowsPerBatch * restartCount

  private def runAvailableNowRestartLoop(root: File): Unit = {
    (0 until restartCount).foreach {
      restart =>
        val checkpointDir = new File(root, s"checkpoint-$restart").getCanonicalPath
        val input =
          spark
            .readStream
            .format("rate-micro-batch")
            .option("rowsPerBatch", rowsPerBatch)
            .option("numPartitions", numPartitions)
            .load()
            .where("value % 3 <> 0")
            .selectExpr(
              "CAST(value AS BIGINT) AS id",
              "CAST((value * 13) % 1024 AS INT) AS bucket",
              "timestamp")

        val query =
          input
            .writeStream
            .format("noop")
            .option("checkpointLocation", checkpointDir)
            .trigger(Trigger.AvailableNow())
            .start()

        try {
          query.awaitTermination()
        } finally {
          query.stop()
        }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmark =
      new Benchmark(
        "Velox Structured Streaming fallback compatibility",
        totalRows,
        output = output)

    benchmark.addCase("vanilla Spark structured streaming", 3) {
      _ =>
        withTempPath {
          root =>
            withSQLConf(
              GlutenConfig.GLUTEN_ENABLED.key -> "false",
              SQLConf.ANSI_ENABLED.key -> "false") {
              runAvailableNowRestartLoop(root)
            }
        }
    }

    benchmark.addCase("Gluten structured streaming compatibility fallback", 3) {
      _ =>
        withTempPath {
          root =>
            withSQLConf(
              GlutenConfig.GLUTEN_ENABLED.key -> "true",
              GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "false",
              SQLConf.ANSI_ENABLED.key -> "false") {
              runAvailableNowRestartLoop(root)
            }
        }
    }

    benchmark.run()
  }
}
