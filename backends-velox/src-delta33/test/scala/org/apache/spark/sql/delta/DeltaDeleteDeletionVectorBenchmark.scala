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
package org.apache.spark.sql.delta

import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}
import org.apache.gluten.execution.{DeltaScanTransformer, HashAggregateExecBaseTransformer, HashAggregateExecTransformer}
import org.apache.gluten.extension.DeltaDeletionVectorDmlUtils
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.{FileSourceScanExec, GlutenExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.Utils

import io.delta.sql.DeltaSparkSessionExtension

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Focused benchmark for Delta DELETE with persistent deletion vectors.
 *
 * Usage:
 * {{{
 *   org.apache.spark.sql.delta.DeltaDeleteDeletionVectorBenchmark \
 *     [rows] [files] [iterations] [deleteMode] [executionMode] [deleteShape]
 * }}}
 *
 * Delete modes: create, update, all. Execution modes: spark, gluten-jvm-bitmap,
 * gluten-native-bitmap, gluten, all. Delete shapes: sparse1, mod10, dense50, uniformhot,
 * fileskewhot, allshapes.
 */
object DeltaDeleteDeletionVectorBenchmark extends BenchmarkBase {
  private val EnableNativeDmlRowIndexScan =
    "spark.gluten.sql.delta.enableNativeDmlRowIndexScan"

  private case class BenchmarkConf(
      rowCount: Long = 1000 * 1000,
      files: Int = 8,
      iterations: Int = 3,
      deleteMode: String = "all",
      executionMode: String = "spark",
      deleteShape: String = "mod10")

  private case class DeleteShape(
      label: String,
      layout: String,
      createPredicate: String,
      updateSetupPredicate: String,
      updateMeasuredPredicate: String)

  private case class ExpectedStats(
      deletedRows: Long,
      finalRows: Long,
      finalIdSum: BigInt)

  private case class ExecutionMode(
      label: String,
      withGlutenPlugin: Boolean,
      deleteConfs: DeleteConfs)

  private case class DeleteConfs(
      glutenEnabled: Boolean,
      nativeWriteEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean,
      scanOnly: Boolean)

  private case class DeleteResult(
      activeFiles: Long,
      filesWithDvs: Long,
      dvCardinality: Long,
      dvPayloadBytes: Long,
      finalRows: Long,
      finalIdSum: BigInt,
      deleteMs: Long,
      validationMs: Long,
      planSummary: PlanSummary)

  private case class PlanSummary(
      deletePlans: Int,
      glutenDeleteCommands: Int,
      deltaScanTransformers: Int,
      nativeHashAggregateTransformers: Int,
      bitmapAggregatorMentions: Int,
      nativeBitmapAggregatePlans: Int,
      sparkBitmapAggregatePlans: Int,
      dmlRowIndexFallbackScans: Int,
      fallbackReasons: Seq[String],
      bitmapAggregatePlanDiagnostics: Seq[String],
      bitmapAggregateFallbackDiagnostics: Seq[String],
      bitmapAggregateValidationDiagnostics: Seq[String])

  private var sparkSession: SparkSession = _
  private var benchmarkRoot: File = _

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val conf = parseArgs(mainArgs)
    val shapes = deleteShapes(conf.deleteShape, conf.files)
    executionModes(conf.executionMode).foreach {
      mode =>
        sparkSession = createSparkSession(conf, mode)
        benchmarkRoot = Utils.createTempDir(
          namePrefix = s"delta-delete-dv-benchmark-${mode.label}")
        try {
          shapes.foreach {
            shape =>
              conf.deleteMode match {
                case "create" =>
                  runDeleteBenchmark(
                    name = "Delta DELETE creates deletion vectors",
                    conf = conf,
                    mode = mode,
                    existingDv = false,
                    shape = shape,
                    measuredPredicate = shape.createPredicate
                  )
                case "update" =>
                  runDeleteBenchmark(
                    name = "Delta DELETE updates existing deletion vectors",
                    conf = conf,
                    mode = mode,
                    existingDv = true,
                    shape = shape,
                    measuredPredicate = shape.updateMeasuredPredicate
                  )
                case "all" =>
                  runDeleteBenchmark(
                    name = "Delta DELETE creates deletion vectors",
                    conf = conf,
                    mode = mode,
                    existingDv = false,
                    shape = shape,
                    measuredPredicate = shape.createPredicate
                  )
                  runDeleteBenchmark(
                    name = "Delta DELETE updates existing deletion vectors",
                    conf = conf,
                    mode = mode,
                    existingDv = true,
                    shape = shape,
                    measuredPredicate = shape.updateMeasuredPredicate
                  )
                case other =>
                  throw new IllegalArgumentException(
                    s"Unknown delete mode '$other'. Expected create, update, or all.")
              }
          }
        } finally {
          stopSpark()
          if (benchmarkRoot != null) {
            Utils.deleteRecursively(benchmarkRoot)
            benchmarkRoot = null
          }
        }
    }
  }

  override def afterAll(): Unit = {
    stopSpark()
    if (benchmarkRoot != null) {
      Utils.deleteRecursively(benchmarkRoot)
      benchmarkRoot = null
    }
  }

  private def spark: SparkSession = sparkSession

  private def parseArgs(args: Array[String]): BenchmarkConf = {
    val defaults = BenchmarkConf()
    BenchmarkConf(
      rowCount = args.headOption.map(_.toLong).getOrElse(defaults.rowCount),
      files = args.lift(1).map(_.toInt).getOrElse(defaults.files),
      iterations = args.lift(2).map(_.toInt).getOrElse(defaults.iterations),
      deleteMode = args.lift(3).map(_.toLowerCase(Locale.ROOT)).getOrElse(defaults.deleteMode),
      executionMode =
        args.lift(4).map(_.toLowerCase(Locale.ROOT)).getOrElse(defaults.executionMode),
      deleteShape = args.lift(5).map(_.toLowerCase(Locale.ROOT)).getOrElse(defaults.deleteShape)
    )
  }

  private def deleteShapes(shape: String, files: Int): Seq[DeleteShape] = {
    shape match {
      case "allshapes" | "all-shapes" | "matrix" =>
        Seq("sparse1", "mod10", "dense50", "uniformhot", "fileskewhot")
          .map(deleteShape(_, files))
      case other => Seq(deleteShape(other, files))
    }
  }

  private def deleteShape(shape: String, files: Int): DeleteShape = {
    val hotFiles = math.max(1, files / 32)
    val uniformDivisor = math.max(4, (math.max(files, 1) / hotFiles) * 2)
    val hotFilePredicate = s"file_group < $hotFiles"
    shape match {
      case "sparse1" =>
        DeleteShape(
          label = "sparse1",
          layout = "uniform-id-modulo",
          createPredicate = predicate(100, Seq(0)),
          updateSetupPredicate = predicate(100, Seq(99)),
          updateMeasuredPredicate = predicate(100, Seq(1))
        )
      case "mod10" | "uniform10" =>
        DeleteShape(
          label = "mod10",
          layout = "uniform-id-modulo",
          createPredicate = predicate(10, Seq(0)),
          updateSetupPredicate = predicate(10, Seq(9)),
          updateMeasuredPredicate = predicate(10, Seq(1))
        )
      case "dense50" =>
        DeleteShape(
          label = "dense50",
          layout = "uniform-id-modulo",
          createPredicate = predicate(4, Seq(0, 2)),
          updateSetupPredicate = predicate(4, Seq(3)),
          updateMeasuredPredicate = predicate(4, Seq(0, 2))
        )
      case "uniformhot" =>
        DeleteShape(
          label = s"uniformhot${hotFiles}of${math.max(files, 1)}",
          layout = "uniform-id-modulo-matched-to-file-skew-density",
          createPredicate = predicate(uniformDivisor, Seq(0)),
          updateSetupPredicate = predicate(uniformDivisor, Seq(uniformDivisor - 1)),
          updateMeasuredPredicate = predicate(uniformDivisor, Seq(1))
        )
      case "fileskewhot" =>
        DeleteShape(
          label = s"fileskewhot${hotFiles}of${math.max(files, 1)}",
          layout = "file-group-skew",
          createPredicate = s"$hotFilePredicate AND id % 2 = 0",
          updateSetupPredicate = s"$hotFilePredicate AND id % 4 = 3",
          updateMeasuredPredicate = s"$hotFilePredicate AND id % 2 = 0"
        )
      case other =>
        throw new IllegalArgumentException(
          s"Unknown delete shape '$other'. Expected sparse1, mod10, dense50, " +
            "uniformhot, fileskewhot, or allshapes.")
    }
  }

  private def executionModes(mode: String): Seq[ExecutionMode] = {
    val sparkOnly = ExecutionMode(
      label = "spark",
      withGlutenPlugin = false,
      deleteConfs = DeleteConfs(
        glutenEnabled = false,
        nativeWriteEnabled = false,
        nativeDmlRowIndexScanEnabled = false,
        scanOnly = false)
    )
    val glutenJvmBitmap = ExecutionMode(
      label = "gluten-jvm-bitmap",
      withGlutenPlugin = true,
      deleteConfs = DeleteConfs(
        glutenEnabled = true,
        nativeWriteEnabled = true,
        nativeDmlRowIndexScanEnabled = true,
        scanOnly = true)
    )
    val glutenNativeBitmap = ExecutionMode(
      label = "gluten-native-bitmap",
      withGlutenPlugin = true,
      deleteConfs = DeleteConfs(
        glutenEnabled = true,
        nativeWriteEnabled = true,
        nativeDmlRowIndexScanEnabled = true,
        scanOnly = false)
    )
    mode match {
      case "spark" => Seq(sparkOnly)
      case "gluten-jvm-bitmap" => Seq(glutenJvmBitmap)
      case "gluten-native-bitmap" | "gluten" => Seq(glutenNativeBitmap)
      case "all" => Seq(sparkOnly, glutenJvmBitmap, glutenNativeBitmap)
      case other =>
        throw new IllegalArgumentException(
          s"Unknown execution mode '$other'. Expected spark, gluten-jvm-bitmap, " +
            "gluten-native-bitmap, gluten, or all.")
    }
  }

  private def createSparkSession(conf: BenchmarkConf, mode: ExecutionMode): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(s"DeltaDeleteDeletionVectorBenchmark-${mode.label}")
      .setIfMissing("spark.master", "local[4]")
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.default.parallelism", conf.files.toString)
      .set("spark.sql.shuffle.partitions", conf.files.toString)
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(GlutenConfig.FALLBACK_REPORTER_ENABLED.key, "false")
      .set("spark.gluten.enabled", mode.deleteConfs.glutenEnabled.toString)
      .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, mode.deleteConfs.nativeWriteEnabled.toString)
      .set(EnableNativeDmlRowIndexScan, mode.deleteConfs.nativeDmlRowIndexScanEnabled.toString)
      .set(GlutenConfig.COLUMNAR_SCAN_ONLY_ENABLED.key, mode.deleteConfs.scanOnly.toString)
      .set(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key, "true")
      .set(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey,
        "true")
      .set(DeltaSQLConf.DELTA_COLLECT_STATS.key, "false")

    if (mode.withGlutenPlugin) {
      sparkConf
        .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "4g")
    }

    SparkSession.builder.config(sparkConf).getOrCreate()
  }

  private def runDeleteBenchmark(
      name: String,
      conf: BenchmarkConf,
      mode: ExecutionMode,
      existingDv: Boolean,
      shape: DeleteShape,
      measuredPredicate: String): Unit = {
    val paths = prepareTables(s"$name-${mode.label}-${shape.label}", conf, existingDv, shape)
    val expectedStats = expectedStatsAfterDelete(paths.head, measuredPredicate)
    val benchmark = new Benchmark(
      name =
        s"$name ${mode.label} ${shape.label} (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase(s"${mode.label} DELETE DV", conf.iterations) {
      iteration =>
        val result = runDelete(
          paths(iteration),
          measuredPredicate,
          mode.deleteConfs)
        validateDeleteResult(
          result,
          existingDv,
          expectedStats.finalRows,
          expectedStats.finalIdSum)
        validateBitmapPlanShape(result, mode.label)
        printIterationResult(
          iteration,
          mode.label,
          if (existingDv) "update" else "create",
          shape,
          measuredPredicate,
          conf.rowCount,
          expectedStats.deletedRows,
          result)
    }

    benchmark.run()
  }

  private def prepareTables(
      prefix: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      shape: DeleteShape): IndexedSeq[String] = {
    (0 until conf.iterations).map {
      iteration =>
        val path = new File(
          benchmarkRoot,
          s"${sanitize(prefix)}-$iteration").getCanonicalPath
        writeTable(path, conf)
        if (existingDv) {
          val setupExpected = expectedStatsAfterDelete(path, shape.updateSetupPredicate)
          val result = runDelete(
            path,
            shape.updateSetupPredicate,
            DeleteConfs(
              glutenEnabled = false,
              nativeWriteEnabled = false,
              nativeDmlRowIndexScanEnabled = false,
              scanOnly = false)
          )
          validateDeleteResult(
            result,
            existingDv = false,
            expectedFinalRows = setupExpected.finalRows,
            expectedFinalIdSum = setupExpected.finalIdSum
          )
        }
        path
    }
  }

  private def writeTable(path: String, conf: BenchmarkConf): Unit = {
    val fileCount = math.max(conf.files, 1)
    val rowCount = math.max(conf.rowCount, 1L)
    spark
      .range(0L, conf.rowCount, 1L, fileCount)
      .selectExpr(
        "id",
        s"cast(least($fileCount - 1, cast(id * $fileCount / $rowCount as int)) as int) " +
          "as file_group",
        s"cast(id % $fileCount as int) as part",
        "cast(id % 1000 as int) as payload"
      )
      .write
      .format("delta")
      .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "true")
      .mode("overwrite")
      .save(path)
  }

  private def runDelete(
      path: String,
      predicate: String,
      confs: DeleteConfs): DeleteResult = {
    withConfs(
      "spark.gluten.enabled" -> confs.glutenEnabled.toString,
      VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> confs.nativeWriteEnabled.toString,
      EnableNativeDmlRowIndexScan -> confs.nativeDmlRowIndexScanEnabled.toString,
      GlutenConfig.COLUMNAR_SCAN_ONLY_ENABLED.key -> confs.scanOnly.toString,
      DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true"
    ) {
      val deleteStartNs = System.nanoTime()
      val executedPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
        spark.sql(s"DELETE FROM delta.`$path` WHERE $predicate").collect()
      }.map(_.executedPlan)
      val deleteMs = elapsedMs(deleteStartNs)
      collectDeleteResult(
        path,
        deleteMs,
        summarizePlans(
          executedPlans,
          validateNativeBitmapAggregates = confs.glutenEnabled && !confs.scanOnly))
    }
  }

  private def collectDeleteResult(
      path: String,
      deleteMs: Long,
      planSummary: PlanSummary): DeleteResult = {
    val validationStartNs = System.nanoTime()
    val files = DeltaLog.forTable(spark, path).update().allFiles.collect()
    val filesWithDvs = files.filter(_.deletionVector != null)
    val finalStats = spark.read
      .format("delta")
      .load(path)
      .selectExpr(
        "count(*) as final_rows",
        "coalesce(sum(cast(id as decimal(38,0))), cast(0 as decimal(38,0))) as final_id_sum")
      .head()
    DeleteResult(
      activeFiles = files.length,
      filesWithDvs = filesWithDvs.length,
      dvCardinality = filesWithDvs.map(_.deletionVector.cardinality).sum,
      dvPayloadBytes = filesWithDvs.map(_.deletionVector.sizeInBytes).sum,
      finalRows = finalStats.getLong(0),
      finalIdSum = BigInt(finalStats.getDecimal(1).toBigInteger),
      deleteMs = deleteMs,
      validationMs = elapsedMs(validationStartNs),
      planSummary = planSummary
    )
  }

  private def expectedStatsAfterDelete(path: String, predicate: String): ExpectedStats = {
    val stats = spark.read
      .format("delta")
      .load(path)
      .selectExpr(
        s"sum(case when $predicate then 1L else 0L end) as deleted_rows",
        s"sum(case when NOT ($predicate) then 1L else 0L end) as final_rows",
        s"coalesce(sum(case when NOT ($predicate) then cast(id as decimal(38,0)) " +
          "else cast(0 as decimal(38,0)) end), cast(0 as decimal(38,0))) as final_id_sum"
      )
      .head()
    ExpectedStats(
      deletedRows = Option(stats.get(0)).map(_.asInstanceOf[Number].longValue()).getOrElse(0L),
      finalRows = Option(stats.get(1)).map(_.asInstanceOf[Number].longValue()).getOrElse(0L),
      finalIdSum = BigInt(stats.getDecimal(2).toBigInteger)
    )
  }

  private def summarizePlans(
      executedPlans: Seq[SparkPlan],
      validateNativeBitmapAggregates: Boolean): PlanSummary = {
    val planNodes = executedPlans.flatMap(collectSparkPlanNodes)
    val fileScans = planNodes.collect { case scan: FileSourceScanExec => scan }
    val planTexts = executedPlans.map(_.treeString.toLowerCase(Locale.ROOT))
    val bitmapAggregatorMentions = planTexts.map(countBitmapAggregatorMentions).sum
    val nativeBitmapAggregatePlans = executedPlans.count {
      plan =>
        val planText = plan.treeString.toLowerCase(Locale.ROOT)
        containsBitmapAggregator(planText) &&
        planText.contains("hashaggregatetransformer")
    }
    val sparkBitmapAggregatePlans = executedPlans.count {
      plan =>
        val planText = plan.treeString.toLowerCase(Locale.ROOT)
        containsBitmapAggregator(planText) &&
        !planText.contains("hashaggregatetransformer")
    }
    val fallbackReasons =
      planNodes.flatMap(plan => FallbackTags.getOption(plan).map(_.reason())).distinct.sorted
    val dmlFallbackScans = fileScans.count {
      scan =>
        DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(scan) &&
        FallbackTags
          .getOption(scan)
          .exists(_.reason().contains("fallback Delta DV DML row-index scan"))
    }
    val bitmapAggregatePlanDiagnostics = executedPlans.zipWithIndex.collect {
      case (plan, index) if containsBitmapAggregator(plan.treeString.toLowerCase(Locale.ROOT)) =>
        s"bitmap aggregate plan #$index:\n${plan.treeString}"
    }
    val bitmapAggregateFallbackDiagnostics = executedPlans.zipWithIndex.flatMap {
      case (plan, index) if containsBitmapAggregator(plan.treeString.toLowerCase(Locale.ROOT)) =>
        collectBitmapAggregateFallbackDiagnostics(plan).map {
          diagnostic => s"bitmap aggregate fallback #$index: $diagnostic"
        }
      case _ => Seq.empty
    }
    val bitmapAggregateValidationDiagnostics =
      if (validateNativeBitmapAggregates) {
        planNodes.collect {
          case aggregate: ObjectHashAggregateExec
              if containsBitmapAggregator(aggregate.treeString.toLowerCase(Locale.ROOT)) =>
            validateBitmapAggregateTransformer(aggregate)
        }.distinct.sorted
      } else {
        Seq.empty
      }
    PlanSummary(
      deletePlans = executedPlans.length,
      glutenDeleteCommands = planNodes.count(_.nodeName.contains("GlutenDeleteCommand")),
      deltaScanTransformers = planNodes.count(_.isInstanceOf[DeltaScanTransformer]),
      nativeHashAggregateTransformers =
        planNodes.count(_.isInstanceOf[HashAggregateExecTransformer]),
      bitmapAggregatorMentions = bitmapAggregatorMentions,
      nativeBitmapAggregatePlans = nativeBitmapAggregatePlans,
      sparkBitmapAggregatePlans = sparkBitmapAggregatePlans,
      dmlRowIndexFallbackScans = dmlFallbackScans,
      fallbackReasons = fallbackReasons,
      bitmapAggregatePlanDiagnostics = bitmapAggregatePlanDiagnostics,
      bitmapAggregateFallbackDiagnostics = bitmapAggregateFallbackDiagnostics,
      bitmapAggregateValidationDiagnostics = bitmapAggregateValidationDiagnostics
    )
  }

  private def validateBitmapAggregateTransformer(aggregate: ObjectHashAggregateExec): String = {
    Try {
      val validation = HashAggregateExecBaseTransformer.from(aggregate).doValidate()
      if (validation.ok()) {
        s"${aggregate.nodeName} ${aggregate.aggregateExpressions.mkString("[", ", ", "]")} -> passed"
      } else {
        s"${aggregate.nodeName} ${aggregate.aggregateExpressions.mkString("[", ", ", "]")} -> " +
          validation.reason()
      }
    }.recover {
      case e =>
        s"${aggregate.nodeName} ${aggregate.aggregateExpressions.mkString("[", ", ", "]")} -> " +
          s"validation threw ${e.getClass.getName}: ${Option(e.getMessage).getOrElse("")}"
    }.get
  }

  private def collectSparkPlanNodes(plan: SparkPlan): Seq[SparkPlan] = {
    val directNodes = plan.collect { case node: SparkPlan => node }
    directNodes ++ directNodes.collect {
      case adaptive: AdaptiveSparkPlanExec =>
        Seq(adaptive.executedPlan, adaptive.initialPlan).flatMap {
          child =>
            if (child eq adaptive) {
              Seq.empty
            } else {
              collectSparkPlanNodes(child)
            }
        }
    }.flatten
  }

  private def collectBitmapAggregateFallbackDiagnostics(plan: SparkPlan): Seq[String] = {
    Try {
      val (_, fallbackInfo) = GlutenExplainUtils.processPlan[SparkPlan](plan, _ => ())
      fallbackInfo.toSeq.collect {
        case (node, reason)
            if node.toLowerCase(Locale.ROOT).contains("aggregate") ||
              reason.toLowerCase(Locale.ROOT).contains("aggregate") =>
          s"$node -> $reason"
      }.sorted
    }.getOrElse(Seq.empty)
  }

  private def containsBitmapAggregator(planText: String): Boolean =
    planText.contains("bitmapaggregator") || planText.contains("bitmap_aggregator")

  private def countBitmapAggregatorMentions(planText: String): Int =
    countOccurrences(planText, "bitmapaggregator") +
      countOccurrences(planText, "bitmap_aggregator")

  private def countOccurrences(text: String, token: String): Int = {
    Iterator
      .iterate(text.indexOf(token))(index => text.indexOf(token, index + token.length))
      .takeWhile(_ >= 0)
      .size
  }

  private def validateDeleteResult(
      result: DeleteResult,
      existingDv: Boolean,
      expectedFinalRows: Long,
      expectedFinalIdSum: BigInt): Unit = {
    require(result.filesWithDvs > 0, s"Expected deletion vectors, got $result")
    require(result.dvCardinality > 0, s"Expected deleted-row cardinality, got $result")
    require(
      result.finalRows == expectedFinalRows,
      s"Expected $expectedFinalRows final rows, got $result")
    require(
      result.finalIdSum == expectedFinalIdSum,
      s"Expected final id sum $expectedFinalIdSum, got $result")
    if (existingDv) {
      require(
        result.dvCardinality > result.filesWithDvs,
        s"Expected existing-DV update to retain non-trivial cardinality, got $result")
    }
  }

  private def validateBitmapPlanShape(result: DeleteResult, label: String): Unit = {
    val summary = result.planSummary
    label match {
      case "spark" =>
        require(summary.nativeBitmapAggregatePlans == 0, s"Unexpected native bitmap plan: $result")
      case "gluten-jvm-bitmap" =>
        require(
          summary.sparkBitmapAggregatePlans > 0,
          s"Expected Spark bitmap aggregation for $label, got $result")
        require(
          summary.nativeBitmapAggregatePlans == 0,
          s"Expected no native bitmap aggregation for $label, got $result")
      case "gluten-native-bitmap" =>
        require(
          summary.nativeHashAggregateTransformers > 0 && summary.nativeBitmapAggregatePlans > 0,
          s"Expected native bitmap aggregation for $label, got $result")
      case _ =>
    }
  }

  private def printIterationResult(
      iteration: Int,
      label: String,
      deleteMode: String,
      shape: DeleteShape,
      measuredPredicate: String,
      rowCount: Long,
      expectedDeletedRows: Long,
      result: DeleteResult): Unit = {
    val deleteDensityPct = percent(expectedDeletedRows, rowCount)
    val dvCardinalityPct = percent(result.dvCardinality, rowCount)
    val touchedFilePct = percent(result.filesWithDvs, result.activeFiles)
    val payloadBytesPerDeletedRow = ratio(result.dvPayloadBytes, expectedDeletedRows)
    val payloadBytesPerDvRow = ratio(result.dvPayloadBytes, result.dvCardinality)
    writeOutputLine(
      s"$label result: iteration=$iteration, " +
        s"deleteMode=$deleteMode, " +
        s"activeFiles=${result.activeFiles}, " +
        s"deleteShape=${shape.label}, " +
        s"deleteLayout=${shape.layout}, " +
        s"deletePredicate=$measuredPredicate, " +
        s"expectedDeletedRows=$expectedDeletedRows, " +
        s"deleteDensityPct=$deleteDensityPct, " +
        s"touchedFiles=${result.filesWithDvs}, " +
        s"touchedFilePct=$touchedFilePct, " +
        s"filesWithDvs=${result.filesWithDvs}, " +
        s"dvCardinality=${result.dvCardinality}, " +
        s"dvCardinalityPct=$dvCardinalityPct, " +
        s"dvPayloadBytes=${result.dvPayloadBytes}, " +
        s"payloadBytesPerDeletedRow=$payloadBytesPerDeletedRow, " +
        s"payloadBytesPerDvRow=$payloadBytesPerDvRow, " +
        s"finalRows=${result.finalRows}, " +
        s"finalIdSum=${result.finalIdSum}, " +
        s"deleteMs=${result.deleteMs}, " +
        s"validationMs=${result.validationMs}, " +
        s"deletePlans=${result.planSummary.deletePlans}, " +
        s"glutenDeleteCommands=${result.planSummary.glutenDeleteCommands}, " +
        s"deltaScanTransformers=${result.planSummary.deltaScanTransformers}, " +
        s"nativeHashAggregateTransformers=" +
        s"${result.planSummary.nativeHashAggregateTransformers}, " +
        s"bitmapAggregatorMentions=${result.planSummary.bitmapAggregatorMentions}, " +
        s"nativeBitmapAggregatePlans=${result.planSummary.nativeBitmapAggregatePlans}, " +
        s"sparkBitmapAggregatePlans=${result.planSummary.sparkBitmapAggregatePlans}, " +
        s"dmlRowIndexFallbackScans=${result.planSummary.dmlRowIndexFallbackScans}, " +
        s"fallbackReasons=${result.planSummary.fallbackReasons.mkString("[", "; ", "]")}")
  }

  private def ratio(numerator: Long, denominator: Long): String = {
    if (denominator == 0L) {
      "n/a"
    } else {
      f"${numerator.toDouble / denominator.toDouble}%.4f"
    }
  }

  private def percent(numerator: Long, denominator: Long): String = {
    if (denominator == 0L) {
      "n/a"
    } else {
      f"${numerator.toDouble * 100.0 / denominator.toDouble}%.4f"
    }
  }

  private def writeOutputLine(line: String): Unit = {
    output match {
      case Some(out) =>
        out.write((line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8))
        out.flush()
      case None =>
        println(line)
    }
  }

  private def withConfs[T](confs: (String, String)*)(f: => T): T = {
    val previous = confs.map {
      case (key, _) => key -> Try(spark.conf.get(key)).toOption
    }
    try {
      confs.foreach { case (key, value) => spark.conf.set(key, value) }
      f
    } finally {
      previous.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  private def predicate(divisor: Int, mods: Seq[Int]): String = {
    val distinctMods = mods.distinct.sorted
    require(distinctMods.nonEmpty, "Expected at least one delete modulo")
    if (distinctMods.length == 1) {
      s"id % $divisor = ${distinctMods.head}"
    } else {
      s"id % $divisor IN (${distinctMods.mkString(", ")})"
    }
  }

  private def stopSpark(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
      sparkSession = null
    }
  }

  private def sanitize(name: String): String =
    name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-")

  private def elapsedMs(startNs: Long): Long =
    (System.nanoTime() - startNs) / (1000L * 1000L)
}
