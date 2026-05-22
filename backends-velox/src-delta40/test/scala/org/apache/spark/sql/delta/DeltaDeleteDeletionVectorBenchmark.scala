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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.{DeletionVectorBitmapGenerator, GlutenDeltaDeleteTiming}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormat.{FILE_PATH, METADATA_NAME}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.Utils

import io.delta.sql.DeltaSparkSessionExtension

import java.io.File
import java.util.Locale
import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

/**
 * Benchmark native Delta DELETE with deletion vectors.
 *
 * To run from a built checkout, use a Spark 4 / Scala 2.13 / Velox / Delta test classpath and run:
 * {{{
 *   org.apache.spark.sql.delta.DeltaDeleteDeletionVectorBenchmark [rows] [files] [iterations] [mode]
 * }}}
 *
 * Modes: full, best, crc, scanheavy, phase, diagnose, timed, partitioned, commit, variance,
 * plainvariance, explain, deleteexplain, all. Defaults: 1,000,000 rows, 8 files, 3 iterations.
 */
object DeltaDeleteDeletionVectorBenchmark extends BenchmarkBase {
  private val EnableNativeDmlRowIndexScan =
    "spark.gluten.sql.delta.enableNativeDmlRowIndexScan"
  private val EnableNativeColumnarBitmapMerge =
    "spark.gluten.sql.delta.delete.dv.enableNativeColumnarBitmapMerge"
  private val EnableNativeBitmapAggregation =
    "spark.gluten.sql.delta.delete.dv.enableNativeBitmapAggregation"
  private val EnablePlainParquetTargetScan =
    "spark.gluten.sql.delta.delete.dv.enablePlainParquetTargetScan"
  private val DriverMergeMaxFiles =
    "spark.gluten.sql.delta.delete.dv.driverMergeMaxFiles"
  private val PlainParquetDriverBitmapScanGlutenEnabled =
    "spark.gluten.sql.delta.delete.dv.plainParquetDriverBitmapScan.glutenEnabled"
  private val DriverColumnarBitmapMergeEnabled =
    "spark.gluten.sql.delta.delete.dv.driverColumnarBitmapMerge.enabled"
  private val SkipAllFilesInCrcForDvDelete =
    "spark.gluten.sql.delta.delete.dv.checksum.skipAllFilesInCrc"
  private val SkipChecksumForDvDelete =
    "spark.gluten.sql.delta.delete.dv.checksum.skipWrite"

  private case class DiagnosticConf(name: String, confs: Seq[(String, String)])

  private case class BenchmarkConf(
      rowCount: Long = 1000 * 1000,
      files: Int = 8,
      iterations: Int = 3,
      mode: String = "full")

  private var sparkSession: SparkSession = _
  private var benchmarkRoot: File = _

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val conf = parseArgs(mainArgs)
    sparkSession = createSparkSession(conf)
    benchmarkRoot = Utils.createTempDir(namePrefix = "delta-delete-dv-benchmark")

    if (conf.mode == "explain") {
      explainPhasePlans(conf)
    }

    if (conf.mode == "deleteexplain") {
      explainDeletePlans(conf)
    }

    if (conf.mode == "phase" || conf.mode == "all") {
      runPhaseBenchmark(
        name = "Delta DELETE DV phases while creating deletion vectors",
        conf = conf,
        existingDv = false,
        measuredPredicate = "id % 10 = 0")

      runPhaseBenchmark(
        name = "Delta DELETE DV phases while updating existing deletion vectors",
        conf = conf,
        existingDv = true,
        measuredPredicate = "id % 10 = 1")
    }

    if (conf.mode == "diagnose" || conf.mode == "all") {
      runScanPlanningBenchmark(
        name = "Delta DELETE DV scan planning while creating deletion vectors",
        conf = conf,
        existingDv = false,
        measuredPredicate = "id % 10 = 0")

      runScanPlanningBenchmark(
        name = "Delta DELETE DV scan planning while updating existing deletion vectors",
        conf = conf,
        existingDv = true,
        measuredPredicate = "id % 10 = 1")
    }

    if (conf.mode == "timed") {
      runTimedDeleteBenchmark(
        name = "Delta DELETE DV internal timing",
        conf = conf,
        existingDv = false,
        measuredPredicate = "id % 10 = 0")

      runTimedDeleteBenchmark(
        name = "Delta DELETE DV internal timing with existing deletion vectors",
        conf = conf,
        existingDv = true,
        measuredPredicate = "id % 10 = 1")
    }

    if (conf.mode == "partitioned" || conf.mode == "all") {
      runPartitionedTimedDeleteBenchmark(
        name = "Delta DELETE DV partitioned production path timing",
        conf = conf,
        existingDv = false,
        measuredPredicate = "part = 0 AND id % 10 = 4")

      runPartitionedTimedDeleteBenchmark(
        name = "Delta DELETE DV partitioned production path timing with existing deletion vectors",
        conf = conf,
        existingDv = true,
        measuredPredicate = "part = 0 AND id % 10 = 4"
      )
    }

    if (conf.mode == "commit" || conf.mode == "all") {
      runCommitPostCommitBenchmark(
        name = "Delta DELETE DV commit/post-commit sensitivity",
        conf = conf,
        existingDv = false,
        measuredPredicate = "id % 10 = 0")

      runCommitPostCommitBenchmark(
        name = "Delta DELETE DV commit/post-commit sensitivity with existing deletion vectors",
        conf = conf,
        existingDv = true,
        measuredPredicate = "id % 10 = 1")
    }

    if (conf.mode == "variance" || conf.mode == "all") {
      runScanBitmapVarianceBenchmark(
        name = "Delta DELETE DV scan/bitmap variance",
        conf = conf,
        existingDv = false,
        measuredPredicate = "id % 10 = 0")

      runScanBitmapVarianceBenchmark(
        name = "Delta DELETE DV scan/bitmap variance with existing deletion vectors",
        conf = conf,
        existingDv = true,
        measuredPredicate = "id % 10 = 1")
    }

    if (conf.mode == "plainvariance") {
      runPlainParquetScanBitmapVarianceBenchmark(
        name = "Delta DELETE DV plain Parquet scan/bitmap variance",
        conf = conf,
        measuredPredicate = "id % 10 = 0")
    }

    if (conf.mode == "best" || conf.mode == "all") {
      runBestPathBenchmark(
        name = "Delta DELETE DV best path comparison",
        conf = conf,
        existingDv = false,
        measuredPredicate = "id % 10 = 0")

      runBestPathBenchmark(
        name = "Delta DELETE DV best path comparison with existing deletion vectors",
        conf = conf,
        existingDv = true,
        measuredPredicate = "id % 10 = 1")
    }

    if (conf.mode == "crc" || conf.mode == "all") {
      runBestPathCrcBenchmark(
        name = "Delta DELETE DV best path CRC sensitivity",
        conf = conf,
        existingDv = false,
        measuredPredicate = "id % 10 = 0")

      runBestPathCrcBenchmark(
        name = "Delta DELETE DV best path CRC sensitivity with existing deletion vectors",
        conf = conf,
        existingDv = true,
        measuredPredicate = "id % 10 = 1")
    }

    if (conf.mode == "scanheavy" || conf.mode == "all") {
      runScanHeavyBenchmark(conf)
    }

    if (conf.mode == "full" || conf.mode == "all") {
      runDeleteBenchmark(
        name = "Delta DELETE creates deletion vectors",
        conf = conf,
        existingDv = false,
        measuredPredicate = "id % 10 = 0",
        includeMetadataRowIndexFallback = false)

      runDeleteBenchmark(
        name = "Delta DELETE updates existing deletion vectors",
        conf = conf,
        existingDv = true,
        measuredPredicate = "id % 10 = 1",
        includeMetadataRowIndexFallback = true
      )
    }
  }

  private def runScanHeavyBenchmark(conf: BenchmarkConf): Unit = {
    runBestPathCrcBenchmark(
      name = "Delta DELETE DV scan-heavy best path CRC sensitivity",
      conf = conf,
      existingDv = false,
      measuredPredicate = "id % 2 = 0")

    runBestPathCrcBenchmark(
      name = "Delta DELETE DV scan-heavy best path CRC sensitivity with existing deletion vectors",
      conf = conf,
      existingDv = true,
      measuredPredicate = "id % 2 = 1")

    runScanBitmapVarianceBenchmark(
      name = "Delta DELETE DV scan-heavy scan/bitmap variance",
      conf = conf,
      existingDv = false,
      measuredPredicate = "id % 2 = 0")

    runScanBitmapVarianceBenchmark(
      name = "Delta DELETE DV scan-heavy scan/bitmap variance with existing deletion vectors",
      conf = conf,
      existingDv = true,
      measuredPredicate = "id % 2 = 1")
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
      sparkSession = null
    }
    if (benchmarkRoot != null) {
      Utils.deleteRecursively(benchmarkRoot)
      benchmarkRoot = null
    }
  }

  private def spark: SparkSession = sparkSession

  private def parseArgs(args: Array[String]): BenchmarkConf = {
    BenchmarkConf(
      rowCount = args.headOption.map(_.toLong).getOrElse(BenchmarkConf().rowCount),
      files = args.lift(1).map(_.toInt).getOrElse(BenchmarkConf().files),
      iterations = args.lift(2).map(_.toInt).getOrElse(BenchmarkConf().iterations),
      mode = args.lift(3).map(_.toLowerCase(Locale.ROOT)).getOrElse(BenchmarkConf().mode)
    )
  }

  private def createSparkSession(benchmarkConf: BenchmarkConf): SparkSession = {
    val isMac = sys.props
      .get("os.name")
      .exists(_.toLowerCase(Locale.ROOT).contains("mac"))

    val conf = new SparkConf()
      .setAppName("DeltaDeleteDeletionVectorBenchmark")
      .setIfMissing("spark.master", "local[4]")
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.default.parallelism", benchmarkConf.files.toString)
      .set("spark.sql.shuffle.partitions", benchmarkConf.files.toString)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(GlutenConfig.FALLBACK_REPORTER_ENABLED.key, "false")
      .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, "true")
      .set(DeltaSQLConf.DELTA_COLLECT_STATS.key, "false")
      .set("spark.databricks.delta.snapshotPartitions", "2")
      .set("spark.gluten.sql.fallbackUnexpectedMetadataParquet", "true")

    if (isMac) {
      conf.set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
    }

    val spark = SparkSession.builder.config(conf).getOrCreate()
    spark.sessionState.conf.setConfString(GlutenConfig.FALLBACK_REPORTER_ENABLED.key, "false")
    spark
  }

  private def runDeleteBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String,
      includeMetadataRowIndexFallback: Boolean): Unit = {
    val nativeSparkScanPaths = prepareTables(s"$name-native-spark-scan", conf, existingDv)
    val nativeRowIndexScanNativeBitmapPaths =
      prepareTables(s"$name-native-row-index-scan-native-bitmap", conf, existingDv)
    val plainParquetTargetScanNativeBitmapPaths =
      prepareTables(s"$name-plain-parquet-target-scan-native-bitmap", conf, existingDv)
    val plainParquetTargetScanJvmBitmapPaths =
      prepareTables(s"$name-plain-parquet-target-scan-jvm-bitmap", conf, existingDv)
    val nativeRowIndexScanJvmBitmapPaths =
      prepareTables(s"$name-native-row-index-scan-jvm-bitmap", conf, existingDv)
    val vanillaDeltaWithGlutenPaths =
      prepareTables(s"$name-vanilla-delta-with-gluten", conf, existingDv)
    val sparkPaths = prepareTables(s"$name-spark", conf, existingDv)
    val fallbackPaths =
      if (includeMetadataRowIndexFallback) Some(prepareTables(s"$name-fallback", conf, existingDv))
      else None

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase("Gluten DELETE DV (Spark row-index scan)", conf.iterations) {
      iteration =>
        runDelete(
          nativeSparkScanPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = false,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase(
      "Gluten DELETE DV (native row-index scan, native bitmap aggregation)",
      conf.iterations) {
      iteration =>
        runDelete(
          nativeRowIndexScanNativeBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase(
      "Gluten DELETE DV (plain Parquet target scan, native bitmap aggregation)",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetTargetScanNativeBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          driverMergeMaxFiles = 0,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase(
      "Gluten DELETE DV (plain Parquet target scan, JVM bitmap merge)",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetTargetScanJvmBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase(
      "Gluten DELETE DV (native row-index scan, JVM bitmap merge)",
      conf.iterations) {
      iteration =>
        runDelete(
          nativeRowIndexScanJvmBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = false,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase("Spark DELETE DV (Gluten disabled)", conf.iterations) {
      iteration =>
        runDelete(
          sparkPaths(iteration),
          measuredPredicate,
          glutenEnabled = false,
          nativeWriteEnabled = false,
          nativeDmlRowIndexScanEnabled = false,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase("Delta DELETE DV (Gluten enabled, native write disabled)", conf.iterations) {
      iteration =>
        runDelete(
          vanillaDeltaWithGlutenPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = false,
          nativeDmlRowIndexScanEnabled = false,
          useMetadataRowIndex = true
        )
    }

    fallbackPaths.foreach {
      paths =>
        benchmark.addCase(
          "Gluten DELETE DV scan fallback without metadata row index",
          conf.iterations) {
          iteration =>
            runDelete(
              paths(iteration),
              measuredPredicate,
              glutenEnabled = true,
              nativeWriteEnabled = true,
              nativeDmlRowIndexScanEnabled = false,
              useMetadataRowIndex = false
            )
        }
    }

    benchmark.run()
  }

  private def runBestPathBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val plainParquetTargetScanNativeBitmapPaths =
      prepareTables(s"$name-plain-parquet-target-scan-native-bitmap", conf, existingDv)
    val sparkPaths = prepareTables(s"$name-spark", conf, existingDv)
    val vanillaDeltaWithGlutenPaths =
      prepareTables(s"$name-vanilla-delta-with-gluten", conf, existingDv)

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase(
      "best: Gluten plain Parquet target scan + native bitmap aggregation",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetTargetScanNativeBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase("best: Spark DELETE DV (Gluten disabled)", conf.iterations) {
      iteration =>
        runDelete(
          sparkPaths(iteration),
          measuredPredicate,
          glutenEnabled = false,
          nativeWriteEnabled = false,
          nativeDmlRowIndexScanEnabled = false,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase(
      "best: Delta DELETE DV (Gluten enabled, native write disabled)",
      conf.iterations) {
      iteration =>
        runDelete(
          vanillaDeltaWithGlutenPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = false,
          nativeDmlRowIndexScanEnabled = false,
          useMetadataRowIndex = true
        )
    }

    benchmark.run()
  }

  private def runBestPathCrcBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val glutenDefaultPaths = prepareTables(s"$name-gluten-default", conf, existingDv)
    val glutenNoAllFilesInCrcPaths =
      prepareTables(s"$name-gluten-no-all-files-in-crc", conf, existingDv)
    val glutenDvDeleteNoAllFilesInCrcPaths =
      prepareTables(s"$name-gluten-dv-delete-no-all-files-in-crc", conf, existingDv)
    val glutenDvDeleteNoChecksumPaths =
      prepareTables(s"$name-gluten-dv-delete-no-checksum", conf, existingDv)
    val sparkDefaultPaths = prepareTables(s"$name-spark-default", conf, existingDv)
    val sparkNoAllFilesInCrcPaths =
      prepareTables(s"$name-spark-no-all-files-in-crc", conf, existingDv)
    val noAllFilesInCrc =
      Seq(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key -> "false")

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase("crc: Gluten best path", conf.iterations) {
      iteration =>
        runDelete(
          glutenDefaultPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = false,
          plainParquetTargetScanEnabled = true,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase("crc: Gluten best path, allFilesInCRC disabled", conf.iterations) {
      iteration =>
        runDelete(
          glutenNoAllFilesInCrcPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = noAllFilesInCrc
        )
    }

    benchmark.addCase(
      "crc: Gluten best path, DV DELETE allFilesInCRC skipped",
      conf.iterations) {
      iteration =>
        runDelete(
          glutenDvDeleteNoAllFilesInCrcPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(SkipAllFilesInCrcForDvDelete -> "true")
        )
    }

    benchmark.addCase("crc: Gluten best path, DV DELETE checksum skipped", conf.iterations) {
      iteration =>
        runDelete(
          glutenDvDeleteNoChecksumPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(SkipChecksumForDvDelete -> "true")
        )
    }

    benchmark.addCase("crc: Spark DELETE DV", conf.iterations) {
      iteration =>
        runDelete(
          sparkDefaultPaths(iteration),
          measuredPredicate,
          glutenEnabled = false,
          nativeWriteEnabled = false,
          nativeDmlRowIndexScanEnabled = false,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase("crc: Spark DELETE DV, allFilesInCRC disabled", conf.iterations) {
      iteration =>
        runDelete(
          sparkNoAllFilesInCrcPaths(iteration),
          measuredPredicate,
          glutenEnabled = false,
          nativeWriteEnabled = false,
          nativeDmlRowIndexScanEnabled = false,
          useMetadataRowIndex = true,
          extraConfs = noAllFilesInCrc
        )
    }

    benchmark.run()
  }

  private def runTimedDeleteBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val plainParquetTargetScanJvmBitmapPaths =
      prepareTables(s"$name-plain-parquet-target-scan-jvm-bitmap", conf, existingDv)
    val plainParquetTargetScanNativeJvmBitmapPaths =
      prepareTables(s"$name-plain-parquet-target-scan-native-jvm-bitmap", conf, existingDv)
    val plainParquetTargetScanNativeColumnarDriverBitmapPaths =
      prepareTables(
        s"$name-plain-parquet-target-scan-native-columnar-driver-bitmap",
        conf,
        existingDv)
    val plainParquetTargetScanNativeJvmBitmapSkipCrcPaths =
      prepareTables(
        s"$name-plain-parquet-target-scan-native-jvm-bitmap-skip-crc",
        conf,
        existingDv)
    val plainParquetTargetScanNativeJvmBitmapSkipChecksumPaths =
      prepareTables(
        s"$name-plain-parquet-target-scan-native-jvm-bitmap-skip-checksum",
        conf,
        existingDv)
    val nativeColumnarMergePaths =
      prepareTables(s"$name-native-columnar-merge", conf, existingDv)
    val plainParquetNativeColumnarMergePaths =
      prepareTables(s"$name-plain-parquet-native-columnar-merge", conf, existingDv)
    val nativeRowIndexScanNativeBitmapPaths =
      prepareTables(s"$name-native-row-index-scan-native-bitmap", conf, existingDv)
    val plainParquetNativeBitmapPaths =
      prepareTables(s"$name-plain-parquet-native-bitmap", conf, existingDv)

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase(
      "timed: Gluten plain Parquet target scan + JVM bitmap merge",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetTargetScanJvmBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = false,
          plainParquetTargetScanEnabled = true,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(PlainParquetDriverBitmapScanGlutenEnabled -> "false")
        )
    }

    benchmark.addCase(
      "timed: Gluten plain Parquet target scan + native JVM bitmap merge",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetTargetScanNativeJvmBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = false,
          plainParquetTargetScanEnabled = true,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(PlainParquetDriverBitmapScanGlutenEnabled -> "true")
        )
    }

    benchmark.addCase(
      "timed: Gluten plain Parquet target scan + Arrow-loaded columnar JVM bitmap merge",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetTargetScanNativeColumnarDriverBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = false,
          plainParquetTargetScanEnabled = true,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(
            PlainParquetDriverBitmapScanGlutenEnabled -> "true",
            DriverColumnarBitmapMergeEnabled -> "true")
        )
    }

    benchmark.addCase(
      "timed: Gluten plain Parquet target scan + native JVM bitmap merge + DV CRC skip",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetTargetScanNativeJvmBitmapSkipCrcPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = false,
          plainParquetTargetScanEnabled = true,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(
            PlainParquetDriverBitmapScanGlutenEnabled -> "true",
            SkipAllFilesInCrcForDvDelete -> "true")
        )
    }

    benchmark.addCase(
      "timed: Gluten plain Parquet target scan + native JVM bitmap merge + DV checksum skip",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetTargetScanNativeJvmBitmapSkipChecksumPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = false,
          plainParquetTargetScanEnabled = true,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(
            PlainParquetDriverBitmapScanGlutenEnabled -> "true",
            SkipChecksumForDvDelete -> "true")
        )
    }

    benchmark.addCase(
      "timed: Gluten native row-index scan + columnar bitmap merge",
      conf.iterations) {
      iteration =>
        runDelete(
          nativeColumnarMergePaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = true,
          nativeBitmapAggregationEnabled = false,
          driverMergeMaxFiles = 0,
          timingEnabled = true,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase(
      "timed: Gluten plain Parquet target scan + columnar bitmap merge",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetNativeColumnarMergePaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = true,
          nativeBitmapAggregationEnabled = false,
          plainParquetTargetScanEnabled = true,
          driverMergeMaxFiles = 0,
          timingEnabled = true,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase(
      "timed: Gluten native row-index scan + native bitmap aggregation",
      conf.iterations) {
      iteration =>
        runDelete(
          nativeRowIndexScanNativeBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          timingEnabled = true,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase(
      "timed: Gluten plain Parquet target scan + native bitmap aggregation",
      conf.iterations) {
      iteration =>
        runDelete(
          plainParquetNativeBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          driverMergeMaxFiles = 0,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(
            PlainParquetDriverBitmapScanGlutenEnabled -> "true",
            SkipAllFilesInCrcForDvDelete -> "true")
        )
    }

    benchmark.run()
  }

  private def runPartitionedTimedDeleteBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val glutenDefaultPaths =
      preparePartitionedTables(s"$name-gluten-production", conf, existingDv)
    val glutenNoAllFilesInCrcPaths =
      preparePartitionedTables(s"$name-gluten-production-no-all-files-in-crc", conf, existingDv)
    val glutenSkipDvCrcPaths =
      preparePartitionedTables(s"$name-gluten-production-skip-dv-crc", conf, existingDv)
    val sparkDefaultPaths =
      preparePartitionedTables(s"$name-spark", conf, existingDv)
    val sparkNoAllFilesInCrcPaths =
      preparePartitionedTables(s"$name-spark-no-all-files-in-crc", conf, existingDv)
    val noAllFilesInCrc =
      Seq(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key -> "false")

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase(
      "partitioned: Gluten plain Parquet target scan + native bitmap aggregation",
      conf.iterations) {
      iteration =>
        runDelete(
          glutenDefaultPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          driverMergeMaxFiles = 0,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(PlainParquetDriverBitmapScanGlutenEnabled -> "true")
        )
    }

    benchmark.addCase(
      "partitioned: Gluten production path, allFilesInCRC disabled",
      conf.iterations) {
      iteration =>
        runDelete(
          glutenNoAllFilesInCrcPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          driverMergeMaxFiles = 0,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = noAllFilesInCrc :+ (PlainParquetDriverBitmapScanGlutenEnabled -> "true")
        )
    }

    benchmark.addCase(
      "partitioned: Gluten production path, DV DELETE allFilesInCRC skipped",
      conf.iterations) {
      iteration =>
        runDelete(
          glutenSkipDvCrcPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeWriteEnabled = true,
          nativeDmlRowIndexScanEnabled = true,
          nativeColumnarBitmapMergeEnabled = false,
          nativeBitmapAggregationEnabled = true,
          plainParquetTargetScanEnabled = true,
          driverMergeMaxFiles = 0,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = Seq(
            PlainParquetDriverBitmapScanGlutenEnabled -> "true",
            SkipAllFilesInCrcForDvDelete -> "true")
        )
    }

    benchmark.addCase("partitioned: Spark DELETE DV", conf.iterations) {
      iteration =>
        runDelete(
          sparkDefaultPaths(iteration),
          measuredPredicate,
          glutenEnabled = false,
          nativeWriteEnabled = false,
          nativeDmlRowIndexScanEnabled = false,
          timingEnabled = true,
          useMetadataRowIndex = true
        )
    }

    benchmark.addCase("partitioned: Spark DELETE DV, allFilesInCRC disabled", conf.iterations) {
      iteration =>
        runDelete(
          sparkNoAllFilesInCrcPaths(iteration),
          measuredPredicate,
          glutenEnabled = false,
          nativeWriteEnabled = false,
          nativeDmlRowIndexScanEnabled = false,
          timingEnabled = true,
          useMetadataRowIndex = true,
          extraConfs = noAllFilesInCrc
        )
    }

    benchmark.run()
  }

  private def runPlainParquetScanBitmapVarianceBenchmark(
      name: String,
      conf: BenchmarkConf,
      measuredPredicate: String): Unit = {
    val singleIterationConf = conf.copy(iterations = 1)
    val parquetPath = prepareParquetTables(s"$name-parquet", singleIterationConf).head

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase(
      "plainvariance: native Parquet row materialization + JVM bitmap build",
      conf.iterations) {
      _ =>
        runParquetJvmBitmapBuild(
          parquetPath,
          measuredPredicate,
          glutenEnabled = true)
    }

    benchmark.addCase(
      "plainvariance: Spark Parquet row materialization + JVM bitmap build",
      conf.iterations) {
      _ =>
        runParquetJvmBitmapBuild(
          parquetPath,
          measuredPredicate,
          glutenEnabled = false)
    }

    benchmark.run()
  }

  private def runCommitPostCommitBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val diagnosticConfs = Seq(
      DiagnosticConf("baseline", Nil),
      DiagnosticConf(
        "checksum file disabled",
        Seq(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "false")),
      DiagnosticConf(
        "checksum DV metrics disabled",
        Seq(
          DeltaSQLConf.DELTA_CHECKSUM_DV_METRICS_ENABLED.key -> "false",
          DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED.key -> "false")
      ),
      DiagnosticConf(
        "checksum DV histogram disabled",
        Seq(DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED.key -> "false")),
      DiagnosticConf(
        "incremental commit disabled",
        Seq(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "false")),
      DiagnosticConf(
        "all files in CRC disabled",
        Seq(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key -> "false")),
      DiagnosticConf(
        "history metrics disabled",
        Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false")),
      DiagnosticConf(
        "snapshot partitions = 1",
        Seq(DeltaSQLConf.DELTA_SNAPSHOT_PARTITIONS.key -> "1")),
      DiagnosticConf(
        s"snapshot partitions = ${conf.files}",
        Seq(DeltaSQLConf.DELTA_SNAPSHOT_PARTITIONS.key -> math.max(1, conf.files).toString)),
      DiagnosticConf("baseline repeat", Nil)
    )

    val pathsByDiagnostic = diagnosticConfs.map {
      diagnostic => diagnostic -> prepareTables(s"$name-${diagnostic.name}", conf, existingDv)
    }

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    pathsByDiagnostic.foreach {
      case (diagnostic, paths) =>
        benchmark.addCase(s"commit: ${diagnostic.name}", conf.iterations) {
          iteration =>
            runDelete(
              paths(iteration),
              measuredPredicate,
              glutenEnabled = true,
              nativeWriteEnabled = true,
              nativeDmlRowIndexScanEnabled = true,
              nativeColumnarBitmapMergeEnabled = false,
              nativeBitmapAggregationEnabled = true,
              plainParquetTargetScanEnabled = true,
              timingEnabled = true,
              useMetadataRowIndex = true,
              extraConfs = diagnostic.confs
            )
        }
    }

    benchmark.run()
  }

  private def runScanBitmapVarianceBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val singleIterationConf = conf.copy(iterations = 1)
    val deltaPath = prepareTables(s"$name-delta", singleIterationConf, existingDv).head
    val parquetPath = prepareParquetTables(s"$name-parquet", singleIterationConf).head

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase("variance: native Delta row-index scan count", conf.iterations) {
      _ =>
        runRowIndexScanCount(
          deltaPath,
          measuredPredicate,
          glutenEnabled = true,
          nativeDmlRowIndexScanEnabled = true)
    }

    benchmark.addCase("variance: native plain Parquet row-index scan count", conf.iterations) {
      _ =>
        runParquetRowIndexScanCount(
          parquetPath,
          measuredPredicate,
          glutenEnabled = true)
    }

    benchmark.addCase(
      "variance: native plain Parquet row materialization + JVM bitmap build",
      conf.iterations) {
      _ =>
        runParquetJvmBitmapBuild(
          parquetPath,
          measuredPredicate,
          glutenEnabled = true)
    }

    benchmark.addCase(
      "variance: Spark plain Parquet row materialization + JVM bitmap build",
      conf.iterations) {
      _ =>
        runParquetJvmBitmapBuild(
          parquetPath,
          measuredPredicate,
          glutenEnabled = false)
    }

    benchmark.run()
  }

  private def runScanPlanningBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val nativeDeltaPaths = prepareTables(s"$name-native-delta", conf, existingDv)
    val sparkDeltaPaths = prepareTables(s"$name-spark-delta", conf, existingDv)
    val parquetNativePaths = prepareParquetTables(s"$name-native-parquet", conf)
    val parquetSparkPaths = prepareParquetTables(s"$name-spark-parquet", conf)

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase("diagnose: native Delta row-index executed plan", conf.iterations) {
      iteration =>
        withDmlScanConfs(glutenEnabled = true, nativeDmlRowIndexScanEnabled = true) {
          materializeExecutedPlan(matchedRows(nativeDeltaPaths(iteration), measuredPredicate))
        }
    }

    benchmark.addCase("diagnose: native Delta row-index RDD partitions", conf.iterations) {
      iteration =>
        withDmlScanConfs(glutenEnabled = true, nativeDmlRowIndexScanEnabled = true) {
          materializeRddPartitions(matchedRows(nativeDeltaPaths(iteration), measuredPredicate))
        }
    }

    benchmark.addCase("diagnose: Spark Delta row-index executed plan", conf.iterations) {
      iteration =>
        withDmlScanConfs(glutenEnabled = false, nativeDmlRowIndexScanEnabled = false) {
          materializeExecutedPlan(matchedRows(sparkDeltaPaths(iteration), measuredPredicate))
        }
    }

    benchmark.addCase("diagnose: Spark Delta row-index RDD partitions", conf.iterations) {
      iteration =>
        withDmlScanConfs(glutenEnabled = false, nativeDmlRowIndexScanEnabled = false) {
          materializeRddPartitions(matchedRows(sparkDeltaPaths(iteration), measuredPredicate))
        }
    }

    benchmark.addCase("diagnose: native plain Delta RDD partitions", conf.iterations) {
      iteration =>
        withDmlScanConfs(glutenEnabled = true, nativeDmlRowIndexScanEnabled = true) {
          materializeRddPartitions(
            spark.read
              .format("delta")
              .load(nativeDeltaPaths(iteration))
              .filter(expr(measuredPredicate))
              .select(col("id")))
        }
    }

    benchmark.addCase("diagnose: native Parquet row-index executed plan", conf.iterations) {
      iteration =>
        withConfs(
          GlutenConfig.GLUTEN_ENABLED.key -> "true",
          GlutenConfig.FALLBACK_REPORTER_ENABLED.key -> "false") {
          materializeExecutedPlan(
            matchedParquetRows(parquetNativePaths(iteration), measuredPredicate))
        }
    }

    benchmark.addCase("diagnose: native Parquet row-index RDD partitions", conf.iterations) {
      iteration =>
        withConfs(
          GlutenConfig.GLUTEN_ENABLED.key -> "true",
          GlutenConfig.FALLBACK_REPORTER_ENABLED.key -> "false") {
          materializeRddPartitions(
            matchedParquetRows(parquetNativePaths(iteration), measuredPredicate))
        }
    }

    benchmark.addCase("diagnose: Spark Parquet row-index RDD partitions", conf.iterations) {
      iteration =>
        withConfs(
          GlutenConfig.GLUTEN_ENABLED.key -> "false",
          GlutenConfig.FALLBACK_REPORTER_ENABLED.key -> "false") {
          materializeRddPartitions(
            matchedParquetRows(parquetSparkPaths(iteration), measuredPredicate))
        }
    }

    benchmark.run()
  }

  private def runPhaseBenchmark(
      name: String,
      conf: BenchmarkConf,
      existingDv: Boolean,
      measuredPredicate: String): Unit = {
    val sparkScanPaths = prepareTables(s"$name-spark-row-index-scan", conf, existingDv)
    val nativeScanPaths = prepareTables(s"$name-native-row-index-scan", conf, existingDv)
    val jvmBitmapPaths = prepareTables(s"$name-jvm-bitmap-build", conf, existingDv)
    val nativeBitmapPaths = prepareTables(s"$name-native-bitmap-aggregation", conf, existingDv)
    val sparkDisabledPaths = prepareTables(s"$name-spark-disabled-row-index-scan", conf, existingDv)
    val parquetNativePaths = prepareParquetTables(s"$name-native-parquet-scan", conf)
    val parquetSparkDisabledPaths =
      prepareParquetTables(s"$name-spark-disabled-parquet-scan", conf)

    val benchmark = new Benchmark(
      name = s"$name (${conf.rowCount} rows, ${conf.files} files)",
      valuesPerIteration = conf.rowCount,
      minNumIters = 1,
      warmupTime = Duration.Zero,
      minTime = Duration.Zero,
      outputPerIteration = true,
      output = output
    )

    benchmark.addCase("phase: Spark row-index scan count", conf.iterations) {
      iteration =>
        runRowIndexScanCount(
          sparkScanPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeDmlRowIndexScanEnabled = false)
    }

    benchmark.addCase("phase: native row-index scan count", conf.iterations) {
      iteration =>
        runRowIndexScanCount(
          nativeScanPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeDmlRowIndexScanEnabled = true)
    }

    benchmark.addCase("phase: Spark disabled row-index scan count", conf.iterations) {
      iteration =>
        runRowIndexScanCount(
          sparkDisabledPaths(iteration),
          measuredPredicate,
          glutenEnabled = false,
          nativeDmlRowIndexScanEnabled = false)
    }

    benchmark.addCase("phase: native plain Delta scan count", conf.iterations) {
      iteration =>
        runPlainDeltaScanCount(
          nativeScanPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeDmlRowIndexScanEnabled = true)
    }

    benchmark.addCase("phase: Spark disabled plain Delta scan count", conf.iterations) {
      iteration =>
        runPlainDeltaScanCount(
          sparkDisabledPaths(iteration),
          measuredPredicate,
          glutenEnabled = false,
          nativeDmlRowIndexScanEnabled = false)
    }

    benchmark.addCase("phase: native plain Parquet scan count", conf.iterations) {
      iteration =>
        runPlainParquetScanCount(
          parquetNativePaths(iteration),
          measuredPredicate,
          glutenEnabled = true)
    }

    benchmark.addCase("phase: Spark disabled plain Parquet scan count", conf.iterations) {
      iteration =>
        runPlainParquetScanCount(
          parquetSparkDisabledPaths(iteration),
          measuredPredicate,
          glutenEnabled = false)
    }

    benchmark.addCase("phase: native Parquet row-index scan count", conf.iterations) {
      iteration =>
        runParquetRowIndexScanCount(
          parquetNativePaths(iteration),
          measuredPredicate,
          glutenEnabled = true)
    }

    benchmark.addCase("phase: Spark disabled Parquet row-index scan count", conf.iterations) {
      iteration =>
        runParquetRowIndexScanCount(
          parquetSparkDisabledPaths(iteration),
          measuredPredicate,
          glutenEnabled = false)
    }

    benchmark.addCase("phase: Spark row materialization + JVM bitmap build", conf.iterations) {
      iteration =>
        runJvmBitmapBuild(
          jvmBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeDmlRowIndexScanEnabled = false)
    }

    benchmark.addCase(
      "phase: native Parquet row materialization + JVM bitmap build",
      conf.iterations) {
      iteration =>
        runParquetJvmBitmapBuild(
          parquetNativePaths(iteration),
          measuredPredicate,
          glutenEnabled = true)
    }

    benchmark.addCase(
      "phase: Spark disabled Parquet row materialization + JVM bitmap build",
      conf.iterations) {
      iteration =>
        runParquetJvmBitmapBuild(
          parquetSparkDisabledPaths(iteration),
          measuredPredicate,
          glutenEnabled = false)
    }

    benchmark.addCase("phase: native scan + native bitmap aggregation", conf.iterations) {
      iteration =>
        runNativeBitmapAggregation(
          nativeBitmapPaths(iteration),
          measuredPredicate,
          glutenEnabled = true,
          nativeDmlRowIndexScanEnabled = true)
    }

    benchmark.run()
  }

  private def explainPhasePlans(conf: BenchmarkConf): Unit = {
    val path =
      prepareTables("Delta DELETE DV explain", conf.copy(iterations = 1), existingDv = false).head
    val existingDvPath =
      prepareTables(
        "Delta DELETE DV explain existing dv",
        conf.copy(iterations = 1),
        existingDv = true).head
    val predicate = "id % 10 = 0"
    explainPlan(
      "Spark row-index scan",
      glutenEnabled = true,
      nativeDmlRowIndexScanEnabled = false) {
      matchedRows(path, predicate)
    }
    explainPlan(
      "native row-index scan",
      glutenEnabled = true,
      nativeDmlRowIndexScanEnabled = true) {
      matchedRows(path, predicate)
    }
    explainPlan(
      "native row-index scan with existing DVs",
      glutenEnabled = true,
      nativeDmlRowIndexScanEnabled = true) {
      matchedRows(existingDvPath, predicate)
    }
    explainPlan(
      "native plain Delta scan",
      glutenEnabled = true,
      nativeDmlRowIndexScanEnabled = true) {
      spark.read.format("delta").load(path).filter(expr(predicate)).select(col("id"))
    }
    explainPlan(
      "native plain Delta scan with existing DVs",
      glutenEnabled = true,
      nativeDmlRowIndexScanEnabled = true,
      EnableNativeBitmapAggregation -> "true") {
      spark.read.format("delta").load(existingDvPath).filter(expr(predicate)).select(col("id"))
    }
    explainPlan(
      "Spark disabled plain Delta scan",
      glutenEnabled = false,
      nativeDmlRowIndexScanEnabled = false) {
      spark.read.format("delta").load(path).filter(expr(predicate)).select(col("id"))
    }
    val parquetPath =
      prepareParquetTables(
        "Delta DELETE DV explain parquet",
        conf.copy(iterations = 1)).head
    explainPlan(
      "native Parquet row-index scan",
      glutenEnabled = true,
      nativeDmlRowIndexScanEnabled = true) {
      matchedParquetRows(parquetPath, predicate)
    }
  }

  private def explainDeletePlans(conf: BenchmarkConf): Unit = {
    val deleteConf = conf.copy(iterations = 1)
    val path =
      prepareTables(
        "Delta DELETE DV delete explain native row-index",
        deleteConf,
        existingDv = false).head
    val plainPath =
      prepareTables(
        "Delta DELETE DV delete explain plain parquet",
        deleteConf,
        existingDv = false).head
    val existingDvPath =
      prepareTables(
        "Delta DELETE DV delete explain existing dv native row-index",
        deleteConf,
        existingDv = true).head
    val existingDvPlainPath =
      prepareTables(
        "Delta DELETE DV delete explain existing dv plain parquet",
        deleteConf,
        existingDv = true).head

    explainDeleteCase(
      "DELETE native row-index scan + native bitmap aggregation",
      path,
      "id % 10 = 0",
      plainParquetTargetScanEnabled = false)
    explainDeleteCase(
      "DELETE plain Parquet target scan + native bitmap aggregation",
      plainPath,
      "id % 10 = 0",
      plainParquetTargetScanEnabled = true)
    explainDeleteCase(
      "DELETE native row-index scan + native bitmap aggregation with existing DVs",
      existingDvPath,
      "id % 10 = 1",
      plainParquetTargetScanEnabled = false)
    explainDeleteCase(
      "DELETE plain Parquet target scan + native bitmap aggregation with existing DVs",
      existingDvPlainPath,
      "id % 10 = 1",
      plainParquetTargetScanEnabled = true
    )
  }

  private def explainDeleteCase(
      label: String,
      path: String,
      predicate: String,
      plainParquetTargetScanEnabled: Boolean): Unit = {
    val plans = collectExecutedPlans {
      runDelete(
        path,
        predicate,
        glutenEnabled = true,
        nativeWriteEnabled = true,
        nativeDmlRowIndexScanEnabled = true,
        nativeColumnarBitmapMergeEnabled = false,
        nativeBitmapAggregationEnabled = true,
        plainParquetTargetScanEnabled = plainParquetTargetScanEnabled,
        timingEnabled = true,
        useMetadataRowIndex = true,
        extraConfs = Seq(PlainParquetDriverBitmapScanGlutenEnabled -> "true")
      )
    }

    Console.out.println(s"===== $label =====")
    plans.zipWithIndex.foreach {
      case (plan, index) =>
        Console.out.println(s"--- executed plan $index ---")
        Console.out.println(plan.treeString)
    }
  }

  private def collectExecutedPlans(action: => Unit): Seq[SparkPlan] = {
    val plans = new CopyOnWriteArrayList[SparkPlan]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plans.add(qe.executedPlan)
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    spark.sparkContext.listenerBus.waitUntilEmpty(15000)
    spark.listenerManager.register(listener)
    try {
      action
      spark.sparkContext.listenerBus.waitUntilEmpty(15000)
    } finally {
      spark.listenerManager.unregister(listener)
    }
    plans.asScala.toSeq
  }

  private def prepareParquetTables(prefix: String, conf: BenchmarkConf): IndexedSeq[String] = {
    (0 until conf.iterations).map {
      iteration =>
        val path = new File(benchmarkRoot, s"${sanitize(prefix)}-$iteration").getCanonicalPath
        writeParquetTable(path, conf)
        path
    }
  }

  private def explainPlan(
      label: String,
      glutenEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean,
      extraConfs: (String, String)*)(df: => org.apache.spark.sql.DataFrame): Unit = {
    println(s"===== $label =====")
    withDmlScanConfs(glutenEnabled, nativeDmlRowIndexScanEnabled) {
      withConfs(extraConfs: _*) {
        df.explain(mode = "extended")
      }
    }
  }

  private def prepareTables(
      prefix: String,
      conf: BenchmarkConf,
      existingDv: Boolean): IndexedSeq[String] = {
    (0 until conf.iterations).map {
      iteration =>
        val path = new File(benchmarkRoot, s"${sanitize(prefix)}-$iteration").getCanonicalPath
        writeTable(path, conf)
        enableDeletionVectors(path)
        if (existingDv) {
          runDelete(
            path,
            "id % 10 = 0",
            glutenEnabled = false,
            nativeWriteEnabled = false,
            nativeDmlRowIndexScanEnabled = false,
            useMetadataRowIndex = true
          )
        }
        path
    }
  }

  private def preparePartitionedTables(
      prefix: String,
      conf: BenchmarkConf,
      existingDv: Boolean): IndexedSeq[String] = {
    (0 until conf.iterations).map {
      iteration =>
        val path = new File(benchmarkRoot, s"${sanitize(prefix)}-$iteration").getCanonicalPath
        writePartitionedTable(path, conf)
        enableDeletionVectors(path)
        if (existingDv) {
          runDelete(
            path,
            "part = 0 AND id % 10 = 0",
            glutenEnabled = false,
            nativeWriteEnabled = false,
            nativeDmlRowIndexScanEnabled = false,
            useMetadataRowIndex = true
          )
        }
        path
    }
  }

  private def writeTable(path: String, conf: BenchmarkConf): Unit = {
    spark
      .range(conf.rowCount)
      .selectExpr("cast(id as int) AS id", "concat('v', cast(id as string)) AS value")
      .repartition(conf.files)
      .write
      .format("delta")
      .mode("overwrite")
      .save(path)
  }

  private def writePartitionedTable(path: String, conf: BenchmarkConf): Unit = {
    spark
      .range(conf.rowCount)
      .selectExpr(
        "cast(id as int) AS id",
        "concat('v', cast(id as string)) AS value",
        "cast(id % 2 as int) AS part")
      .repartition(conf.files, col("part"))
      .write
      .format("delta")
      .partitionBy("part")
      .mode("overwrite")
      .save(path)
  }

  private def writeParquetTable(path: String, conf: BenchmarkConf): Unit = {
    spark
      .range(conf.rowCount)
      .selectExpr("cast(id as int) AS id", "concat('v', cast(id as string)) AS value")
      .repartition(conf.files)
      .write
      .format("parquet")
      .mode("overwrite")
      .save(path)
  }

  private def enableDeletionVectors(path: String): Unit = {
    spark.sql(
      s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
  }

  private def runDelete(
      path: String,
      predicate: String,
      glutenEnabled: Boolean,
      nativeWriteEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean,
      nativeColumnarBitmapMergeEnabled: Boolean = false,
      nativeBitmapAggregationEnabled: Boolean = true,
      plainParquetTargetScanEnabled: Boolean = false,
      driverMergeMaxFiles: Int = 64,
      timingEnabled: Boolean = false,
      useMetadataRowIndex: Boolean,
      extraConfs: Seq[(String, String)] = Nil): Unit = {
    val deleteConfs = Seq(
      GlutenConfig.GLUTEN_ENABLED.key -> glutenEnabled.toString,
      GlutenConfig.FALLBACK_REPORTER_ENABLED.key -> "false",
      VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> nativeWriteEnabled.toString,
      EnableNativeDmlRowIndexScan -> nativeDmlRowIndexScanEnabled.toString,
      EnableNativeColumnarBitmapMerge -> nativeColumnarBitmapMergeEnabled.toString,
      EnableNativeBitmapAggregation -> nativeBitmapAggregationEnabled.toString,
      EnablePlainParquetTargetScan -> plainParquetTargetScanEnabled.toString,
      DriverMergeMaxFiles -> driverMergeMaxFiles.toString,
      GlutenDeltaDeleteTiming.timingEnabledKey -> timingEnabled.toString,
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true",
      DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> useMetadataRowIndex.toString
    ) ++ extraConfs
    withConfs(deleteConfs: _*) {
      spark.sql(s"DELETE FROM delta.`$path` WHERE $predicate").collect()
    }
  }

  private def matchedRows(path: String, predicate: String) = {
    spark.read
      .format("delta")
      .load(path)
      .withColumn(
        DeletionVectorBitmapGenerator.FILE_NAME_COL,
        col(s"$METADATA_NAME.$FILE_PATH"))
      .filter(expr(predicate))
      .withColumn(
        DeletionVectorBitmapGenerator.ROW_INDEX_COL,
        col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}"))
      .select(
        col(DeletionVectorBitmapGenerator.FILE_NAME_COL),
        col(DeletionVectorBitmapGenerator.ROW_INDEX_COL))
  }

  private def matchedParquetRows(path: String, predicate: String) = {
    spark.read
      .format("parquet")
      .load(path)
      .withColumn(
        DeletionVectorBitmapGenerator.FILE_NAME_COL,
        col(s"$METADATA_NAME.$FILE_PATH"))
      .filter(expr(predicate))
      .withColumn(
        DeletionVectorBitmapGenerator.ROW_INDEX_COL,
        col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}"))
      .select(
        col(DeletionVectorBitmapGenerator.FILE_NAME_COL),
        col(DeletionVectorBitmapGenerator.ROW_INDEX_COL))
  }

  private def runRowIndexScanCount(
      path: String,
      predicate: String,
      glutenEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean): Unit = {
    withDmlScanConfs(glutenEnabled, nativeDmlRowIndexScanEnabled) {
      matchedRows(path, predicate).count()
    }
  }

  private def runPlainDeltaScanCount(
      path: String,
      predicate: String,
      glutenEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean): Unit = {
    withDmlScanConfs(glutenEnabled, nativeDmlRowIndexScanEnabled) {
      spark.read.format("delta").load(path).filter(expr(predicate)).select(col("id")).count()
    }
  }

  private def runPlainParquetScanCount(
      path: String,
      predicate: String,
      glutenEnabled: Boolean): Unit = {
    withConfs(
      GlutenConfig.GLUTEN_ENABLED.key -> glutenEnabled.toString,
      GlutenConfig.FALLBACK_REPORTER_ENABLED.key -> "false") {
      spark.read.format("parquet").load(path).filter(expr(predicate)).select(col("id")).count()
    }
  }

  private def runParquetRowIndexScanCount(
      path: String,
      predicate: String,
      glutenEnabled: Boolean): Unit = {
    withConfs(
      GlutenConfig.GLUTEN_ENABLED.key -> glutenEnabled.toString,
      GlutenConfig.FALLBACK_REPORTER_ENABLED.key -> "false") {
      matchedParquetRows(path, predicate).count()
    }
  }

  private def materializeExecutedPlan(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    if (plan.nodeName.isEmpty) {
      throw new IllegalStateException("Executed plan was not materialized")
    }
  }

  private def materializeRddPartitions(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    val partitions =
      if (plan.supportsColumnar) {
        plan.executeColumnar().partitions.length
      } else {
        plan.execute().partitions.length
      }
    if (partitions < 0) {
      throw new IllegalStateException("RDD partitions were not materialized")
    }
  }

  private def runJvmBitmapBuild(
      path: String,
      predicate: String,
      glutenEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean): Unit = {
    withDmlScanConfs(glutenEnabled, nativeDmlRowIndexScanEnabled) {
      buildJvmBitmaps(matchedRows(path, predicate))
    }
  }

  private def runParquetJvmBitmapBuild(
      path: String,
      predicate: String,
      glutenEnabled: Boolean): Unit = {
    withConfs(
      GlutenConfig.GLUTEN_ENABLED.key -> glutenEnabled.toString,
      GlutenConfig.FALLBACK_REPORTER_ENABLED.key -> "false") {
      buildJvmBitmaps(matchedParquetRows(path, predicate))
    }
  }

  private def buildJvmBitmaps(df: DataFrame): Unit = {
    val partials = df.queryExecution.toRdd
      .mapPartitions {
        rows =>
          val bitmaps = scala.collection.mutable.HashMap.empty[String, RoaringBitmapArray]
          rows.foreach {
            row =>
              val filePath = row.getUTF8String(0).toString
              val bitmap = bitmaps.getOrElseUpdate(filePath, new RoaringBitmapArray())
              bitmap.add(row.getLong(1))
          }
          bitmaps.iterator.map {
            case (filePath, bitmap) =>
              bitmap.runOptimize()
              (
                filePath,
                bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
                bitmap.cardinality)
          }
      }
      .collect()
      .toSeq

    partials.groupBy(_._1).values.foreach {
      filePartials =>
        val merged = new RoaringBitmapArray()
        filePartials.foreach {
          case (_, bytes, _) => merged.merge(RoaringBitmapArray.readFrom(bytes))
        }
        merged.runOptimize()
        merged.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
        merged.cardinality
    }
  }

  private def runNativeBitmapAggregation(
      path: String,
      predicate: String,
      glutenEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean): Unit = {
    withDmlScanConfs(glutenEnabled, nativeDmlRowIndexScanEnabled) {
      val bitmapAggregator = new BitmapAggregator(
        col(DeletionVectorBitmapGenerator.ROW_INDEX_COL).expr,
        RoaringBitmapArrayFormat.Portable)
      matchedRows(path, predicate)
        .groupBy(col(DeletionVectorBitmapGenerator.FILE_NAME_COL))
        .agg(
          Column(bitmapAggregator.toAggregateExpression(isDistinct = false))
            .as("CardinalityAndBitmapStruct"))
        .collect()
    }
  }

  private def withDmlScanConfs[T](
      glutenEnabled: Boolean,
      nativeDmlRowIndexScanEnabled: Boolean)(f: => T): T = {
    withConfs(
      GlutenConfig.GLUTEN_ENABLED.key -> glutenEnabled.toString,
      GlutenConfig.FALLBACK_REPORTER_ENABLED.key -> "false",
      VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "true",
      EnableNativeDmlRowIndexScan -> nativeDmlRowIndexScanEnabled.toString,
      EnableNativeColumnarBitmapMerge -> "false",
      EnableNativeBitmapAggregation -> "true",
      EnablePlainParquetTargetScan -> "false",
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true",
      DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> "true"
    ) {
      f
    }
  }

  private def withConfs[T](confs: (String, String)*)(f: => T): T = {
    val sqlConf = spark.sessionState.conf
    val previousValues =
      confs.map { case (key, _) => key -> Option(sqlConf.getConfString(key, null)) }
    confs.foreach { case (key, value) => sqlConf.setConfString(key, value) }
    try {
      f
    } finally {
      previousValues.foreach {
        case (key, Some(value)) => sqlConf.setConfString(key, value)
        case (key, None) => sqlConf.unsetConf(key)
      }
    }
  }

  private def sanitize(prefix: String): String = {
    prefix.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-").stripSuffix("-")
  }
}
