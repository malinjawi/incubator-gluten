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
package org.apache.gluten.execution

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.SparkPlan

/**
 * Benchmark native Delta deletion-vector read scans.
 *
 * Arguments:
 *   - rows
 *   - files
 *   - deleteModulo
 *   - iterations
 *   - warmups
 *   - minExecutionSpeedup, optional. Use 2.0 to fail unless scan execution is at least 2x Spark.
 *   - minTotalSpeedup, optional. Use 2.0 to fail unless planning + execution is at least 2x Spark.
 *   - scanOnly, optional. Defaults to true to isolate scan offload.
 *   - printPlans, optional. Defaults to false.
 *   - mode, optional. Either materialize or count. Defaults to materialize.
 *   - tableKind, optional. Either parquet, delta, or deltaDv. Defaults to deltaDv.
 */
object DeltaDeletionVectorReadBenchmark extends SqlBasedBenchmark {
  private val MaterializeMode = "materialize"
  private val CountMode = "count"
  private val ParquetTable = "parquet"
  private val DeltaTable = "delta"
  private val DeltaDeletionVectorTable = "deltaDv"

  private case class BenchmarkConfig(
      rows: Long = 5 * 1000 * 1000L,
      files: Int = 16,
      deleteModulo: Int = 10,
      iterations: Int = 5,
      warmups: Int = 2,
      minExecutionSpeedup: Double = 0.0,
      minTotalSpeedup: Double = 0.0,
      scanOnly: Boolean = true,
      printPlans: Boolean = false,
      mode: String = MaterializeMode,
      tableKind: String = DeltaDeletionVectorTable)

  private case class Timing(
      planningMs: Double,
      executionMs: Double,
      rowCount: Long,
      nativeScanCount: Int,
      supportsColumnar: Boolean,
      columnarChildExecutionMs: Option[Double] = None) {
    def totalMs: Double = planningMs + executionMs
  }

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("DeltaDeletionVectorReadBenchmark")
      .setIfMissing("spark.master", "local[4]")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set(GlutenConfig.FALLBACK_REPORTER_ENABLED.key, "false")
      .set("spark.memory.offHeap.enabled", "true")
      .setIfMissing("spark.memory.offHeap.size", "4g")
      .setIfMissing("spark.driver.memory", "4g")
      .setIfMissing("spark.executor.memory", "4g")
      .setIfMissing("spark.sql.files.maxPartitionBytes", "1g")
      .setIfMissing("spark.sql.files.openCostInBytes", "1073741824")

    SparkSession.builder.config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val conf = parseArgs(mainArgs)
    require(conf.rows > 0, "rows must be positive")
    require(conf.files > 0, "files must be positive")
    require(conf.deleteModulo > 1, "deleteModulo must be greater than 1")
    require(conf.iterations > 0, "iterations must be positive")
    require(conf.warmups >= 0, "warmups must be non-negative")
    require(conf.minExecutionSpeedup >= 0, "minExecutionSpeedup must be non-negative")
    require(conf.minTotalSpeedup >= 0, "minTotalSpeedup must be non-negative")
    require(
      Set(MaterializeMode, CountMode).contains(conf.mode),
      s"mode must be $MaterializeMode or $CountMode")
    require(
      Set(ParquetTable, DeltaTable, DeltaDeletionVectorTable).contains(conf.tableKind),
      s"tableKind must be $ParquetTable, $DeltaTable, or $DeltaDeletionVectorTable")

    withTempPath {
      dir =>
        val path = dir.getCanonicalPath
        prepareTable(path, conf)
        val expectedRows =
          if (conf.tableKind == DeltaDeletionVectorTable) {
            conf.rows - (((conf.rows - 1) / conf.deleteModulo) + 1)
          } else {
            conf.rows
          }

        val sparkTimings = runVariant("Spark", glutenEnabled = false, path, expectedRows, conf)
        val glutenName = if (conf.scanOnly) "Gluten scan-only" else "Gluten native"
        val glutenTimings = runVariant(glutenName, glutenEnabled = true, path, expectedRows, conf)

        printSummary(conf, sparkTimings, glutenTimings)
    }
  }

  private def parseArgs(args: Array[String]): BenchmarkConfig = {
    BenchmarkConfig(
      rows = args.lift(0).map(_.toLong).getOrElse(5 * 1000 * 1000L),
      files = args.lift(1).map(_.toInt).getOrElse(16),
      deleteModulo = args.lift(2).map(_.toInt).getOrElse(10),
      iterations = args.lift(3).map(_.toInt).getOrElse(5),
      warmups = args.lift(4).map(_.toInt).getOrElse(2),
      minExecutionSpeedup = args.lift(5).map(_.toDouble).getOrElse(0.0),
      minTotalSpeedup = args.lift(6).map(_.toDouble).getOrElse(0.0),
      scanOnly = args.lift(7).forall(_.toBoolean),
      printPlans = args.lift(8).exists(_.toBoolean),
      mode = args.lift(9).getOrElse(MaterializeMode),
      tableKind = args.lift(10).getOrElse(DeltaDeletionVectorTable))
  }

  private def prepareTable(path: String, conf: BenchmarkConfig): Unit = {
    withBenchmarkSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      spark
        .range(conf.rows)
        .repartition(conf.files)
        .selectExpr(
          "id",
          "cast(id % 1000 as int) as bucket",
          "cast(id % 100 as int) as payload_int",
          "concat('payload-', cast(id as string)) as payload_string")
        .write
        .format(if (conf.tableKind == ParquetTable) "parquet" else "delta")
        .save(path)

      if (conf.tableKind == DeltaDeletionVectorTable) {
        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$path` WHERE id % ${conf.deleteModulo} = 0")
      }
    }
  }

  private def runVariant(
      name: String,
      glutenEnabled: Boolean,
      path: String,
      expectedRows: Long,
      conf: BenchmarkConfig): Seq[Timing] = {
    withBenchmarkSQLConf(
      GlutenConfig.GLUTEN_ENABLED.key -> glutenEnabled.toString,
      "spark.gluten.sql.columnar.scanOnly" -> conf.scanOnly.toString,
      "spark.gluten.delta.dv.benchmark.tableKind" -> conf.tableKind) {
      (0 until conf.warmups).foreach {
        _ =>
          val timing = runOnce(path, conf.mode, printPlan = conf.printPlans)
          require(timing.rowCount == expectedRows, s"$name warmup returned ${timing.rowCount}")
      }

      (0 until conf.iterations).map {
        _ =>
          val timing = runOnce(path, conf.mode, printPlan = conf.printPlans)
          require(timing.rowCount == expectedRows, s"$name returned ${timing.rowCount}")
          if (glutenEnabled) {
            require(timing.nativeScanCount > 0, s"$name did not use DeltaScanTransformer")
          }
          timing
      }
    }
  }

  private def withBenchmarkSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val previous = pairs.map { case (key, _) => key -> spark.conf.getOption(key) }
    try {
      pairs.foreach { case (key, value) => spark.conf.set(key, value) }
      f
    } finally {
      previous.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  private def runOnce(path: String, mode: String, printPlan: Boolean): Timing = {
    val format =
      if (spark.conf.get("spark.gluten.delta.dv.benchmark.tableKind") == ParquetTable) {
        "parquet"
      } else {
        "delta"
      }
    val baseDf = spark.read
      .format(format)
      .load(path)
      .where("id >= 0")
      .selectExpr("id", "bucket", "payload_int", "payload_string")
    val df =
      if (mode == CountMode) {
        baseDf.selectExpr("count(1) as row_count")
      } else {
        baseDf
      }

    if (printPlan) {
      RuleExecutor.resetMetrics()
    }
    val planningStart = System.nanoTime()
    val plan = df.queryExecution.executedPlan
    val planningMs = elapsedMs(planningStart)
    val nativeScanCount = plan.collect {
      case _: DeltaScanTransformer => true
      case _: FileSourceScanExecTransformerBase => true
    }.size
    if (printPlan) {
      println()
      println(plan)
      println()
      println(RuleExecutor.dumpTimeSpent())
    }

    val executionStart = System.nanoTime()
    val rowCount = executeAndCountRows(plan, countResult = mode == CountMode)
    val executionMs = elapsedMs(executionStart)
    val columnarChildExecutionMs =
      executeColumnarChild(plan, rowCount, countResult = mode == CountMode)

    Timing(
      planningMs,
      executionMs,
      rowCount,
      nativeScanCount,
      plan.supportsColumnar,
      columnarChildExecutionMs)
  }

  private def executeColumnarChild(
      plan: SparkPlan,
      expectedRows: Long,
      countResult: Boolean): Option[Double] = {
    if (countResult) {
      return None
    }
    plan match {
      case c2r: ColumnarToRowExecBase if c2r.child.supportsColumnar =>
        val start = System.nanoTime()
        val rowCount = executeAndCountRows(c2r.child, countResult)
        require(
          rowCount == expectedRows,
          s"Columnar child returned $rowCount rows, expected $expectedRows")
        Some(elapsedMs(start))
      case _ =>
        None
    }
  }

  private def executeAndCountRows(plan: SparkPlan, countResult: Boolean): Long = {
    if (plan.supportsColumnar) {
      plan
        .executeColumnar()
        .mapPartitions {
          batches =>
            var rows = 0L
            batches.foreach {
              batch =>
                if (countResult) {
                  var rowId = 0
                  while (rowId < batch.numRows()) {
                    rows += batch.column(0).getLong(rowId)
                    rowId += 1
                  }
                } else {
                  rows += batch.numRows().toLong
                }
                batch.close()
            }
            Iterator.single(rows)
        }
        .collect()
        .sum
    } else if (countResult) {
      plan.execute().map(_.getLong(0)).collect().sum
    } else {
      plan.execute().count()
    }
  }

  private def elapsedMs(startNs: Long): Double = {
    (System.nanoTime() - startNs).toDouble / 1000.0 / 1000.0
  }

  private def median(values: Seq[Double]): Double = {
    val sorted = values.sorted
    val middle = sorted.length / 2
    if (sorted.length % 2 == 0) {
      (sorted(middle - 1) + sorted(middle)) / 2.0
    } else {
      sorted(middle)
    }
  }

  private def printSummary(
      conf: BenchmarkConfig,
      sparkTimings: Seq[Timing],
      glutenTimings: Seq[Timing]): Unit = {
    val sparkPlanning = median(sparkTimings.map(_.planningMs))
    val sparkExecution = median(sparkTimings.map(_.executionMs))
    val sparkTotal = median(sparkTimings.map(_.totalMs))
    val glutenPlanning = median(glutenTimings.map(_.planningMs))
    val glutenExecution = median(glutenTimings.map(_.executionMs))
    val glutenTotal = median(glutenTimings.map(_.totalMs))
    val glutenColumnarChild =
      glutenTimings.flatMap(_.columnarChildExecutionMs) match {
        case values if values.nonEmpty => Some(median(values))
        case _ => None
      }
    val planningSpeedup = sparkPlanning / glutenPlanning
    val executionSpeedup = sparkExecution / glutenExecution
    val totalSpeedup = sparkTotal / glutenTotal
    val columnarChildSpeedup = glutenColumnarChild.map(sparkExecution / _)

    println()
    println("Delta deletion vector read benchmark")
    println(
      s"rows=${conf.rows}, files=${conf.files}, deleteModulo=${conf.deleteModulo}, " +
        s"iterations=${conf.iterations}, warmups=${conf.warmups}, " +
        f"minExecutionSpeedup=${conf.minExecutionSpeedup}%.2f, " +
        f"minTotalSpeedup=${conf.minTotalSpeedup}%.2f, " +
        s"scanOnly=${conf.scanOnly}, printPlans=${conf.printPlans}, mode=${conf.mode}, " +
        s"tableKind=${conf.tableKind}")
    println(
      f"Spark median: planning=${sparkPlanning}%.1f ms, " +
        f"execution=${sparkExecution}%.1f ms, total=${sparkTotal}%.1f ms, " +
        s"columnar=${sparkTimings.last.supportsColumnar}")
    println(
      f"Gluten median: planning=${glutenPlanning}%.1f ms, " +
        f"execution=${glutenExecution}%.1f ms, total=${glutenTotal}%.1f ms, " +
        s"nativeScans=${glutenTimings.last.nativeScanCount}, " +
        s"columnar=${glutenTimings.last.supportsColumnar}")
    glutenColumnarChild.zip(columnarChildSpeedup).foreach {
      case (childExecution, childSpeedup) =>
        println(
          f"Gluten native columnar child median: execution=${childExecution}%.1f ms, " +
            f"speedupVsSparkExecution=${childSpeedup}%.2fx")
    }
    println(
      f"Speedup: planning=${planningSpeedup}%.2fx, " +
        f"execution=${executionSpeedup}%.2fx, " +
        f"total=${totalSpeedup}%.2fx")

    if (conf.minExecutionSpeedup > 0) {
      require(
        executionSpeedup >= conf.minExecutionSpeedup,
        f"Execution speedup ${executionSpeedup}%.2fx is below " +
          f"required ${conf.minExecutionSpeedup}%.2fx")
    }
    if (conf.minTotalSpeedup > 0) {
      require(
        totalSpeedup >= conf.minTotalSpeedup,
        f"Total speedup ${totalSpeedup}%.2fx is below required ${conf.minTotalSpeedup}%.2fx")
    }
  }
}
