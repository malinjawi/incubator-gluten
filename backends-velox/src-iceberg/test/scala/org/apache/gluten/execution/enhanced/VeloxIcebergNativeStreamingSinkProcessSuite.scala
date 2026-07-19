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
package org.apache.gluten.execution.enhanced

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.VeloxIcebergWriteToDataSourceV2Exec
import org.apache.gluten.tags.EnhancedFeaturesTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

@EnhancedFeaturesTest
class VeloxIcebergNativeStreamingSinkProcessSuite extends AnyFunSuite {

  test("native Iceberg stream write survives driver process halt before sink commit") {
    withTempDir("gluten-native-iceberg-process") {
      workDir =>
        val warehouseDir = new File(workDir, "warehouse")
        val checkpointDir = new File(workDir, "checkpoint")
        Files.createDirectories(warehouseDir.toPath)
        Files.createDirectories(checkpointDir.toPath)

        val tableName =
          "iceberg_stream_driver_pre_commit_halt_tbl_" +
            java.util.UUID.randomUUID().toString.replace("-", "_")
        val crashLog = runDriver(
          mode = "crash-before-commit",
          workDir = workDir,
          warehouseDir = warehouseDir,
          checkpointDir = checkpointDir,
          tableName = tableName,
          expectedExitCode = 86
        )
        assert(
          crashLog.contains("Injected native streaming sink failure before BatchWrite.commit"),
          s"Driver halt log did not include the injected failure marker:\n$crashLog"
        )

        runDriver(
          mode = "recover-before-commit",
          workDir = workDir,
          warehouseDir = warehouseDir,
          checkpointDir = checkpointDir,
          tableName = tableName,
          expectedExitCode = 0
        )
    }
  }

  test("native Iceberg stream write survives driver process halt after sink commit") {
    withTempDir("gluten-native-iceberg-process") {
      workDir =>
        val warehouseDir = new File(workDir, "warehouse")
        val checkpointDir = new File(workDir, "checkpoint")
        Files.createDirectories(warehouseDir.toPath)
        Files.createDirectories(checkpointDir.toPath)

        val tableName =
          "iceberg_stream_driver_halt_tbl_" + java.util.UUID.randomUUID().toString.replace("-", "_")
        val crashLog = runDriver(
          mode = "crash-after-commit",
          workDir = workDir,
          warehouseDir = warehouseDir,
          checkpointDir = checkpointDir,
          tableName = tableName,
          expectedExitCode = 86
        )
        assert(
          crashLog.contains("Injected native streaming sink failure after BatchWrite.commit"),
          s"Driver halt log did not include the injected failure marker:\n$crashLog"
        )

        runDriver(
          mode = "recover",
          workDir = workDir,
          warehouseDir = warehouseDir,
          checkpointDir = checkpointDir,
          tableName = tableName,
          expectedExitCode = 0
        )
    }
  }

  test("native Iceberg stream write fails replay when committed data file is missing") {
    withTempDir("gluten-native-iceberg-process") {
      workDir =>
        val warehouseDir = new File(workDir, "warehouse")
        val checkpointDir = new File(workDir, "checkpoint")
        Files.createDirectories(warehouseDir.toPath)
        Files.createDirectories(checkpointDir.toPath)

        val tableName =
          "iceberg_stream_missing_file_tbl_" +
            java.util.UUID.randomUUID().toString.replace("-", "_")
        runDriver(
          mode = "crash-after-commit",
          workDir = workDir,
          warehouseDir = warehouseDir,
          checkpointDir = checkpointDir,
          tableName = tableName,
          expectedExitCode = 86
        )

        val deletedFile = deleteFirstRawWarehouseParquetFile(warehouseDir, tableName)
        assert(
          deletedFile.nonEmpty,
          s"Expected a committed Iceberg parquet file to delete in $warehouseDir")

        val replayLog = runDriver(
          mode = "recover-missing-file",
          workDir = workDir,
          warehouseDir = warehouseDir,
          checkpointDir = checkpointDir,
          tableName = tableName,
          expectedExitCode = 1
        )
        assert(
          replayLog.contains("data file(s) are missing from object store"),
          s"Missing-file replay log did not include the catalog/object-store audit marker:\n" +
            replayLog
        )
        assertCommitEpochs(checkpointDir, Set.empty)
    }
  }

  test("native Iceberg stream write survives repeated executor process halts after native write") {
    withTempDir("gluten-native-iceberg-process") {
      workDir =>
        val warehouseDir = new File(workDir, "warehouse")
        val checkpointDir = new File(workDir, "checkpoint")
        Files.createDirectories(warehouseDir.toPath)
        Files.createDirectories(checkpointDir.toPath)

        val tableName =
          "iceberg_stream_executor_halt_tbl_" +
            java.util.UUID.randomUUID().toString.replace("-", "_")
        val output = runDriver(
          mode = "executor-halt-after-write",
          workDir = workDir,
          warehouseDir = warehouseDir,
          checkpointDir = checkpointDir,
          tableName = tableName,
          expectedExitCode = 0
        )
        assert(
          output.contains("ExecutorLostFailure") && output.contains("code 86"),
          s"Executor halt log did not include the expected executor-loss marker:\n$output"
        )
    }
  }

  test("native Iceberg stream write survives repeated executor process halts after writer commit") {
    withTempDir("gluten-native-iceberg-process") {
      workDir =>
        val warehouseDir = new File(workDir, "warehouse")
        val checkpointDir = new File(workDir, "checkpoint")
        Files.createDirectories(warehouseDir.toPath)
        Files.createDirectories(checkpointDir.toPath)

        val tableName =
          "iceberg_stream_executor_commit_halt_tbl_" +
            java.util.UUID.randomUUID().toString.replace("-", "_")
        val output = runDriver(
          mode = "executor-halt-after-writer-commit",
          workDir = workDir,
          warehouseDir = warehouseDir,
          checkpointDir = checkpointDir,
          tableName = tableName,
          expectedExitCode = 0
        )
        assert(
          output.contains("ExecutorLostFailure") && output.contains("code 87"),
          "Executor post-commit halt log did not include the expected executor-loss marker:\n" +
            output
        )
    }
  }

  test("native Iceberg stream write survives multi-partition executor halt after writer commit") {
    withTempDir("gluten-native-iceberg-process") {
      workDir =>
        val warehouseDir = new File(workDir, "warehouse")
        val checkpointDir = new File(workDir, "checkpoint")
        Files.createDirectories(warehouseDir.toPath)
        Files.createDirectories(checkpointDir.toPath)

        val tableName =
          "iceberg_stream_executor_commit_halt_multi_partition_tbl_" +
            java.util.UUID.randomUUID().toString.replace("-", "_")
        val output = runDriver(
          mode = "executor-halt-after-writer-commit-multi-partition",
          workDir = workDir,
          warehouseDir = warehouseDir,
          checkpointDir = checkpointDir,
          tableName = tableName,
          expectedExitCode = 0
        )
        assert(
          output.contains("ExecutorLostFailure") && output.contains("code 87"),
          "Executor post-commit halt log did not include the expected executor-loss marker:\n" +
            output
        )
    }
  }

  private def runDriver(
      mode: String,
      workDir: File,
      warehouseDir: File,
      checkpointDir: File,
      tableName: String,
      expectedExitCode: Int): String = {
    val logFile = new File(workDir, s"$mode.log")
    val processBuilder = new ProcessBuilder(
      driverCommand(mode, warehouseDir, checkpointDir, tableName).asJava)
    processBuilder.environment().put("SPARK_HOME", minimalSparkHome(workDir).getCanonicalPath)
    processBuilder.environment().put("SPARK_SCALA_VERSION", "2.13")
    processBuilder.redirectErrorStream(true)
    processBuilder.redirectOutput(ProcessBuilder.Redirect.to(logFile))
    val process = processBuilder.start()
    val finished = process.waitFor(240, TimeUnit.SECONDS)
    if (!finished) {
      process.destroyForcibly()
      fail(s"Timed out waiting for native Iceberg process-driver mode '$mode'")
    }
    val output = new String(Files.readAllBytes(logFile.toPath), StandardCharsets.UTF_8)
    assert(
      process.exitValue() == expectedExitCode,
      s"Expected process-driver mode '$mode' to exit $expectedExitCode but got " +
        s"${process.exitValue()}.\n$output"
    )
    output
  }

  private def driverCommand(
      mode: String,
      warehouseDir: File,
      checkpointDir: File,
      tableName: String): Seq[String] = {
    val javaBin = new File(new File(System.getProperty("java.home"), "bin"), "java").getPath
    val classpath = Option(System.getProperty("surefire.test.class.path"))
      .filter(_.nonEmpty)
      .getOrElse(System.getProperty("java.class.path"))
    val javaLibraryPath = Option(System.getProperty("java.library.path"))
      .filter(_.nonEmpty)
      .map(value => s"-Djava.library.path=$value")
      .toSeq
    Seq(
      javaBin,
      "-Xmx3g",
      "-Xss128m",
      "-XX:ReservedCodeCacheSize=1g",
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Djdk.reflect.useDirectMethodHandle=false",
      "-Dio.netty.tryReflectionSetAccessible=true"
    ) ++
      javaLibraryPath ++
      Seq(
        "-cp",
        classpath,
        "org.apache.gluten.execution.enhanced.VeloxIcebergNativeStreamingSinkProcessDriver",
        mode,
        warehouseDir.getCanonicalPath,
        checkpointDir.getCanonicalPath,
        tableName
      )
  }

  private def minimalSparkHome(workDir: File): File = {
    val sparkHome = new File(workDir, "spark-home")
    Files.createDirectories(new File(sparkHome, "jars").toPath)
    Files.createDirectories(new File(sparkHome, "conf").toPath)
    sparkHome
  }

  private def withTempDir(prefix: String)(body: File => Unit): Unit = {
    val dir = Files.createTempDirectory(prefix).toFile
    try {
      body(dir)
    } finally {
      deleteRecursively(dir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.exists()) {
      val paths = Files.walk(file.toPath)
      try {
        paths.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
      } finally {
        paths.close()
      }
    }
  }

  private def deleteFirstRawWarehouseParquetFile(
      warehouseDir: File,
      tableName: String): Option[File] = {
    if (!warehouseDir.exists()) {
      None
    } else {
      val paths = Files.walk(warehouseDir.toPath)
      try {
        val maybePath = paths
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .filter(path => path.getFileName.toString.endsWith(".parquet"))
          .find(path => tableRelativePathOption(tableName, path.normalize().toString).nonEmpty)
        maybePath.map {
          path =>
            Files.delete(path)
            path.toFile
        }
      } finally {
        paths.close()
      }
    }
  }

  private def assertCommitEpochs(checkpointDir: File, expectedEpochs: Set[String]): Unit = {
    val commitDir = new File(checkpointDir, "commits")
    val committedEpochs = Option(commitDir.list())
      .map(_.filterNot(_.startsWith(".")).toSet)
      .getOrElse(Set.empty[String])
    assert(committedEpochs == expectedEpochs)
  }

  private def tableRelativePathOption(tableName: String, rawPath: String): Option[String] = {
    val normalizedPath =
      try {
        new File(new java.net.URI(rawPath)).toPath.normalize().toString
      } catch {
        case _: java.net.URISyntaxException => rawPath
        case _: IllegalArgumentException => rawPath
      }
    val tablePathSegment = s"${File.separator}$tableName${File.separator}"
    val index = normalizedPath.indexOf(tablePathSegment)
    if (index >= 0) {
      Some(
        normalizedPath
          .substring(index + tablePathSegment.length)
          .replace(File.separatorChar, '/'))
    } else {
      None
    }
  }
}

object VeloxIcebergNativeStreamingSinkProcessDriver {

  def main(args: Array[String]): Unit = {
    require(args.length == 4, s"Expected 4 arguments but received ${args.length}")
    val mode = args(0)
    val warehouseDir = new File(args(1))
    val checkpointDir = new File(args(2))
    val tableName = args(3)

    val spark = SparkSession
      .builder()
      .master(masterForMode(mode))
      .appName(s"VeloxIcebergNativeStreamingSinkProcessDriver-$mode")
      .config(sparkConf(warehouseDir, mode))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      mode match {
        case "crash-before-commit" =>
          runCrashBeforeCommit(spark, tableName, checkpointDir)
        case "crash-after-commit" =>
          runCrashAfterCommit(spark, tableName, checkpointDir)
        case "recover-before-commit" =>
          runRecoverBeforeCommit(spark, tableName, warehouseDir, checkpointDir)
        case "recover" =>
          runRecover(spark, tableName, warehouseDir, checkpointDir)
        case "recover-missing-file" =>
          runRecoverMissingFile(spark, tableName, checkpointDir)
        case "executor-halt-after-write" =>
          runExecutorHaltAfterWrite(spark, tableName, warehouseDir, checkpointDir)
        case "executor-halt-after-writer-commit" =>
          runExecutorHaltAfterWriterCommit(spark, tableName, warehouseDir, checkpointDir)
        case "executor-halt-after-writer-commit-multi-partition" =>
          runExecutorHaltAfterWriterCommitMultiPartition(
            spark,
            tableName,
            warehouseDir,
            checkpointDir)
        case other =>
          throw new IllegalArgumentException(s"Unknown process-driver mode: $other")
      }
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def masterForMode(mode: String): String = {
    mode match {
      case "executor-halt-after-write" | "executor-halt-after-writer-commit" |
          "executor-halt-after-writer-commit-multi-partition" =>
        "local-cluster[2,1,4096]"
      case _ => "local[1]"
    }
  }

  private def sparkConf(warehouseDir: File, mode: String): SparkConf = {
    val conf = new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.enabled", "false")
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.NATIVE_STREAMING_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key, "true")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hadoop")
      .set("spark.sql.catalog.spark_catalog.warehouse", warehouseDir.toURI.toString)
    mode match {
      case "executor-halt-after-write" | "executor-halt-after-writer-commit" |
          "executor-halt-after-writer-commit-multi-partition" =>
        configureExecutorProcessConf(conf)
      case _ => conf
    }
  }

  private def configureExecutorProcessConf(conf: SparkConf): SparkConf = {
    val classpath = Option(System.getProperty("surefire.test.class.path"))
      .filter(_.nonEmpty)
      .getOrElse(System.getProperty("java.class.path"))
    val javaLibraryPath = Option(System.getProperty("java.library.path")).filter(_.nonEmpty)
    conf
      .set("spark.executor.extraClassPath", classpath)
      .set("spark.executor.extraJavaOptions", executorJavaOptions)
      .set("spark.executor.instances", "2")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "1g")
      .set("spark.task.maxFailures", "4")
      .set("spark.speculation", "false")
      .set("spark.dynamicAllocation.enabled", "false")
    javaLibraryPath.foreach(path => conf.set("spark.executor.extraLibraryPath", path))
    conf
  }

  private def executorJavaOptions: String = {
    Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Djdk.reflect.useDirectMethodHandle=false",
      "-Dio.netty.tryReflectionSetAccessible=true"
    ).mkString(" ")
  }

  private def runCrashBeforeCommit(
      spark: SparkSession,
      tableName: String,
      checkpointDir: File): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (a INT, b STRING) USING iceberg")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitKey, "true")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitActionKey, "halt")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitKey, "false")
    val query = startQuery(spark, tableName, checkpointDir)
    awaitProcessDriverQueryTermination(query)
    query.stop()
    throw new IllegalStateException("Expected the native Iceberg streaming process driver to halt")
  }

  private def runCrashAfterCommit(
      spark: SparkSession,
      tableName: String,
      checkpointDir: File): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (a INT, b STRING) USING iceberg")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitKey, "true")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitActionKey, "halt")
    val query = startQuery(spark, tableName, checkpointDir)
    awaitProcessDriverQueryTermination(query)
    query.stop()
    throw new IllegalStateException("Expected the native Iceberg streaming process driver to halt")
  }

  private def runRecoverBeforeCommit(
      spark: SparkSession,
      tableName: String,
      warehouseDir: File,
      checkpointDir: File): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (a INT, b STRING) USING iceberg")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitActionKey, "throw")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitActionKey, "throw")
    assertTableRows(spark, tableName, Seq.empty)
    assertCommitEpochs(checkpointDir, Set.empty)
    assert(icebergFileNamesForEpoch(spark, tableName, 0L).isEmpty)
    assertRawWarehouseParquetFilesAreRetryStableForEpoch(warehouseDir, 0L)

    val query = startQuery(spark, tableName, checkpointDir)
    try {
      awaitProcessDriverQueryTermination(query)
      assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query)
    } finally {
      query.stop()
    }
    assertTableRows(spark, tableName, Seq(Row(0, "a")))
    assertCommitEpochs(checkpointDir, Set("0"))
    assert(icebergFileNamesForEpoch(spark, tableName, 0L).nonEmpty)
    assertNoRawWarehouseOrphanParquetFiles(spark, warehouseDir, tableName)
  }

  private def runRecover(
      spark: SparkSession,
      tableName: String,
      warehouseDir: File,
      checkpointDir: File): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (a INT, b STRING) USING iceberg")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitActionKey, "throw")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitActionKey, "throw")
    val visibleRowsAfterCrash = tableRows(spark, tableName)
    assert(visibleRowsAfterCrash.nonEmpty)
    assertCommitEpochs(checkpointDir, Set.empty)
    val epoch0Files = icebergFileNamesForEpoch(spark, tableName, 0L)
    assert(epoch0Files.nonEmpty)
    assertNoRawWarehouseOrphanParquetFiles(spark, warehouseDir, tableName)

    val query = startQuery(spark, tableName, checkpointDir)
    try {
      awaitProcessDriverQueryTermination(query)
      assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query)
    } finally {
      query.stop()
    }
    assertTableRows(spark, tableName, visibleRowsAfterCrash)
    assertCommitEpochs(checkpointDir, Set("0"))
    assert(icebergFileNamesForEpoch(spark, tableName, 0L) == epoch0Files)
    assertNoRawWarehouseOrphanParquetFiles(spark, warehouseDir, tableName)
  }

  private def runRecoverMissingFile(
      spark: SparkSession,
      tableName: String,
      checkpointDir: File): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (a INT, b STRING) USING iceberg")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitActionKey, "throw")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitActionKey, "throw")

    val query = startQuery(spark, tableName, checkpointDir)
    try {
      awaitProcessDriverQueryTermination(query)
    } finally {
      query.stop()
    }
    throw new IllegalStateException("Expected missing Iceberg data file replay to fail")
  }

  private def runExecutorHaltAfterWrite(
      spark: SparkSession,
      tableName: String,
      warehouseDir: File,
      checkpointDir: File): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (a INT, b STRING) USING iceberg")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterWriteKey, "true")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterWriteActionKey, "halt")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterWritePartitionIdKey, "0")

    (0 until 3).foreach {
      epochId =>
        val query = startQuery(spark, tableName, checkpointDir, latestOffset = epochId + 1L)
        try {
          awaitProcessDriverQueryTermination(query)
          assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query)
        } finally {
          query.stop()
        }
        assertCommitEpochs(checkpointDir, (0 to epochId).map(_.toString).toSet)
        assert(icebergFileNamesForEpoch(spark, tableName, epochId).nonEmpty)
        assertNoRawWarehouseOrphanParquetFiles(spark, warehouseDir, tableName)
    }
    assertTableRows(spark, tableName, Seq(Row(0, "a"), Row(1, "a-1"), Row(2, "a-2")))
    assertNoRawWarehouseOrphanParquetFiles(spark, warehouseDir, tableName)
  }

  private def runExecutorHaltAfterWriterCommit(
      spark: SparkSession,
      tableName: String,
      warehouseDir: File,
      checkpointDir: File): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (a INT, b STRING) USING iceberg")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterWriteKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterCommitKey, "true")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterCommitActionKey, "halt")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterCommitPartitionIdKey, "0")

    (0 until 3).foreach {
      epochId =>
        val query = startQuery(spark, tableName, checkpointDir, latestOffset = epochId + 1L)
        try {
          awaitProcessDriverQueryTermination(query)
          assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query)
        } finally {
          query.stop()
        }
        assertCommitEpochs(checkpointDir, (0 to epochId).map(_.toString).toSet)
        assert(icebergFileNamesForEpoch(spark, tableName, epochId).nonEmpty)
        assertNoRawWarehouseOrphanParquetFiles(spark, warehouseDir, tableName)
    }
    assertTableRows(spark, tableName, Seq(Row(0, "a"), Row(1, "a-1"), Row(2, "a-2")))
    assertNoRawWarehouseOrphanParquetFiles(spark, warehouseDir, tableName)
  }

  private def runExecutorHaltAfterWriterCommitMultiPartition(
      spark: SparkSession,
      tableName: String,
      warehouseDir: File,
      checkpointDir: File): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (a INT, b STRING) USING iceberg")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failBeforeCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failAfterCommitKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterWriteKey, "false")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterCommitKey, "true")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterCommitActionKey, "halt")
    spark.conf.set(NativeIcebergStreamingSinkProcessConfs.failTaskAfterCommitPartitionIdKey, "0")

    val retryCycles =
      positiveIntEnv("GLUTEN_NATIVE_ICEBERG_MULTI_PARTITION_RETRY_CYCLES", defaultValue = 1)
    val rowsPerEpoch = 3
    (0 until retryCycles).foreach {
      epochId =>
        val latestOffset = (epochId + 1L) * rowsPerEpoch
        val query = startQuery(spark, tableName, checkpointDir, latestOffset = latestOffset)
        try {
          awaitProcessDriverQueryTermination(query)
          assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query)
        } finally {
          query.stop()
        }

        assertCommitEpochs(checkpointDir, (0 to epochId).map(_.toString).toSet)
        val epochFiles = icebergFileNamesForEpoch(spark, tableName, epochId.toLong)
        assert(
          epochFiles.size >= rowsPerEpoch,
          s"Expected one active Iceberg file per retried multi-partition epoch task " +
            s"for epoch $epochId, found $epochFiles")
        assertTableRows(spark, tableName, expectedRows(latestOffset.toInt))
        assertNoRawWarehouseOrphanParquetFiles(spark, warehouseDir, tableName)
    }
  }

  private def startQuery(
      spark: SparkSession,
      tableName: String,
      checkpointDir: File,
      latestOffset: Long = 1L): StreamingQuery = {
    spark.readStream
      .format(classOf[FiniteOneRowMicroBatchProvider].getName)
      .option(FiniteOneRowMicroBatchProvider.LatestOffsetOption, latestOffset)
      .load()
      .writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpointDir.getCanonicalPath)
      .format("iceberg")
      .toTable(tableName)
  }

  private def awaitProcessDriverQueryTermination(query: StreamingQuery): Unit = {
    if (!query.awaitTermination(120000)) {
      query.stop()
      throw new IllegalStateException("Timed out waiting for process-driver streaming query")
    }
  }

  private def assertTableRows(
      spark: SparkSession,
      tableName: String,
      expectedRows: Seq[Row]): Unit = {
    withGlutenDisabled(spark) {
      val actualRows = tableRows(spark, tableName)
      assert(actualRows == expectedRows, s"Expected rows $expectedRows but found $actualRows")
    }
  }

  private def expectedRows(count: Int): Seq[Row] = {
    (0 until count).map {
      value => Row(value, if (value == 0) "a" else s"a-$value")
    }
  }

  private def positiveIntEnv(name: String, defaultValue: Int): Int = {
    sys.env.get(name) match {
      case Some(raw) =>
        val parsed =
          try {
            raw.toInt
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"$name must be a positive integer, got '$raw'")
          }
        require(parsed > 0, s"$name must be a positive integer, got '$raw'")
        parsed
      case None => defaultValue
    }
  }

  private def tableRows(spark: SparkSession, tableName: String): Seq[Row] = {
    withGlutenDisabled(spark) {
      spark.sql(s"SELECT * FROM $tableName ORDER BY a").collect().toSeq
    }
  }

  private def assertCommitEpochs(checkpointDir: File, expectedEpochs: Set[String]): Unit = {
    val commitDir = new File(checkpointDir, "commits")
    val committedEpochs = Option(commitDir.list())
      .map(_.filterNot(_.startsWith(".")).toSet)
      .getOrElse(Set.empty[String])
    assert(committedEpochs == expectedEpochs)
  }

  private def icebergFileNamesForEpoch(
      spark: SparkSession,
      tableName: String,
      epochId: Long): Set[String] = {
    val epochPattern = s"\\d{5}-.+-$epochId-\\d{5}\\.parquet"
    allIcebergFileNames(spark, tableName).filter(_.matches(epochPattern))
  }

  private def allIcebergFileNames(spark: SparkSession, tableName: String): Set[String] = {
    allIcebergRelativePaths(spark, tableName).map(_.split('/').last)
  }

  private def allIcebergRelativePaths(spark: SparkSession, tableName: String): Set[String] = {
    withGlutenDisabled(spark) {
      spark
        .sql(s"SELECT file_path FROM default.$tableName.files")
        .collect()
        .map(row => tableRelativePath(tableName, row.getString(0)))
        .toSet
    }
  }

  private def assertNoRawWarehouseOrphanParquetFiles(
      spark: SparkSession,
      warehouseDir: File,
      tableName: String): Unit = {
    val rawFiles = rawWarehouseParquetRelativePaths(warehouseDir, tableName)
    val activeFiles = allIcebergRelativePaths(spark, tableName)
    val missingActiveFiles = activeFiles.diff(rawFiles)
    val orphanFiles = rawFiles.diff(activeFiles)
    assert(
      missingActiveFiles.isEmpty,
      s"Raw warehouse scan did not find active Iceberg parquet files for $tableName. " +
        s"missing=$missingActiveFiles, active=$activeFiles, raw=$rawFiles"
    )
    assert(
      orphanFiles.isEmpty,
      s"Found raw Iceberg parquet files not referenced by active metadata for $tableName. " +
        s"orphans=$orphanFiles, active=$activeFiles, raw=$rawFiles"
    )
  }

  private def assertRawWarehouseParquetFilesAreRetryStableForEpoch(
      warehouseDir: File,
      epochId: Long): Unit = {
    val rawFiles = rawWarehouseParquetFileNames(warehouseDir)
    val retryStableEpochPattern = s"\\d{5}-.+-$epochId-\\d{5}\\.parquet"
    val unstableFiles = rawFiles.filterNot(_.matches(retryStableEpochPattern))
    assert(
      unstableFiles.isEmpty,
      s"Found raw Iceberg parquet files that cannot be deterministically retried for " +
        s"epoch $epochId. unstable=$unstableFiles, raw=$rawFiles"
    )
  }

  private def rawWarehouseParquetFileNames(warehouseDir: File): Set[String] = {
    if (!warehouseDir.exists()) {
      Set.empty
    } else {
      val paths = Files.walk(warehouseDir.toPath)
      try {
        paths
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .map(_.getFileName.toString)
          .filter(_.endsWith(".parquet"))
          .toSet
      } finally {
        paths.close()
      }
    }
  }

  private def rawWarehouseParquetRelativePaths(
      warehouseDir: File,
      tableName: String): Set[String] = {
    if (!warehouseDir.exists()) {
      Set.empty
    } else {
      val paths = Files.walk(warehouseDir.toPath)
      try {
        paths
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .filter(path => path.getFileName.toString.endsWith(".parquet"))
          .flatMap(path => tableRelativePathOption(tableName, path.normalize().toString))
          .toSet
      } finally {
        paths.close()
      }
    }
  }

  private def tableRelativePath(tableName: String, rawPath: String): String = {
    tableRelativePathOption(tableName, rawPath).getOrElse {
      assert(
        false,
        s"Expected Iceberg data file path to contain table directory segment for $tableName: $rawPath")
      rawPath
    }
  }

  private def tableRelativePathOption(tableName: String, rawPath: String): Option[String] = {
    val normalizedPath =
      try {
        new File(new java.net.URI(rawPath)).toPath.normalize().toString
      } catch {
        case _: java.net.URISyntaxException => rawPath
        case _: IllegalArgumentException => rawPath
      }
    val tablePathSegment = s"${File.separator}$tableName${File.separator}"
    val index = normalizedPath.indexOf(tablePathSegment)
    if (index >= 0) {
      Some(
        normalizedPath
          .substring(index + tablePathSegment.length)
          .replace(File.separatorChar, '/'))
    } else {
      None
    }
  }

  private def assertLastExecutionPlanContains[T: ClassTag](query: StreamingQuery): Unit = {
    val expectedClass = implicitly[ClassTag[T]].runtimeClass
    val executedPlan =
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
    assert(
      executedPlan.exists(expectedClass.isInstance),
      s"Expected an executed ${expectedClass.getName} plan, observed:\n${executedPlan.treeString}")
  }

  private def withGlutenDisabled[T](spark: SparkSession)(body: => T): T = {
    val oldGlutenEnabled = spark.conf.getOption(GlutenConfig.GLUTEN_ENABLED.key)
    spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, "false")
    try {
      body
    } finally {
      oldGlutenEnabled match {
        case Some(value) => spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, value)
        case None => spark.conf.unset(GlutenConfig.GLUTEN_ENABLED.key)
      }
    }
  }
}

private object NativeIcebergStreamingSinkProcessConfs {
  val failBeforeCommitKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failBeforeCommit.enabled"

  val failBeforeCommitActionKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failBeforeCommit.action"

  val failAfterCommitKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failAfterCommit.enabled"

  val failAfterCommitActionKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failAfterCommit.action"

  val failTaskAfterWriteKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failTaskAfterWrite.enabled"

  val failTaskAfterWriteActionKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failTaskAfterWrite.action"

  val failTaskAfterWritePartitionIdKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failTaskAfterWrite.partitionId"

  val failTaskAfterCommitKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failTaskAfterCommit.enabled"

  val failTaskAfterCommitActionKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failTaskAfterCommit.action"

  val failTaskAfterCommitPartitionIdKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failTaskAfterCommit.partitionId"
}

class FiniteOneRowMicroBatchProvider extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    FiniteOneRowMicroBatchProvider.schema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table =
    new FiniteOneRowMicroBatchTable

  override def supportsExternalMetadata(): Boolean = false
}

private class FiniteOneRowMicroBatchTable extends Table with SupportsRead {
  override def name(): String = "finite-one-row-micro-batch"

  override def schema(): StructType = FiniteOneRowMicroBatchProvider.schema

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ScanBuilder {
      override def build(): Scan = new Scan {
        override def readSchema(): StructType = FiniteOneRowMicroBatchProvider.schema

        override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
          new FiniteOneRowMicroBatchStream(FiniteOneRowMicroBatchProvider.latestOffset(options))

        override def columnarSupportMode(): Scan.ColumnarSupportMode =
          Scan.ColumnarSupportMode.UNSUPPORTED
      }
    }
}

private class FiniteOneRowMicroBatchStream(latestAvailableOffset: Long) extends MicroBatchStream {
  override def initialOffset(): Offset = FiniteOneRowOffset(0)

  override def deserializeOffset(json: String): Offset = FiniteOneRowOffset(json.toLong)

  override def latestOffset(): Offset = FiniteOneRowOffset(latestAvailableOffset)

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[FiniteOneRowOffset].value
    val endOffset = end.asInstanceOf[FiniteOneRowOffset].value
    (startOffset until endOffset).map(FiniteOneRowInputPartition).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new PartitionReaderFactory {
      override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
        new FiniteOneRowPartitionReader(partition.asInstanceOf[FiniteOneRowInputPartition].offset)
    }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}
}

private case class FiniteOneRowOffset(value: Long) extends Offset {
  override def json(): String = value.toString
}

private case class FiniteOneRowInputPartition(offset: Long) extends InputPartition {
  override def preferredLocations(): Array[String] = Array.empty
}

private class FiniteOneRowPartitionReader(offset: Long) extends PartitionReader[InternalRow] {
  private var consumed = false
  private val row = new GenericInternalRow(
    Array[Any](offset.toInt, UTF8String.fromString(if (offset == 0) "a" else s"a-$offset")))

  override def next(): Boolean = {
    if (consumed) {
      false
    } else {
      consumed = true
      true
    }
  }

  override def get(): InternalRow = row

  override def close(): Unit = {}
}

private object FiniteOneRowMicroBatchProvider {
  val LatestOffsetOption = "latest-offset"

  val schema: StructType = new StructType()
    .add("a", IntegerType, nullable = false)
    .add("b", StringType, nullable = false)

  def latestOffset(options: CaseInsensitiveStringMap): Long = {
    Option(options.get(LatestOffsetOption)).map(_.toLong).getOrElse(1L)
  }
}
