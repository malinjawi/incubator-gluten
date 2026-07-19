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
import org.apache.gluten.execution.{VeloxIcebergWriteToDataSourceV2Exec, WholeStageTransformerSuite}
import org.apache.gluten.tags.EnhancedFeaturesTest

import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

@EnhancedFeaturesTest
class VeloxIcebergNativeStreamingSinkSuite extends WholeStageTransformerSuite {

  import testImplicits._

  private val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hadoop")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/tpch-data-iceberg-velox")
  }

  test("native Iceberg stream write preserves epochs across checkpoint restart") {
    val tableName =
      "iceberg_stream_restart_tbl_" + java.util.UUID.randomUUID().toString.replace("-", "_")
    withTable(tableName) {
      withTempDir {
        checkpointDir =>
          withSQLConf(
            SQLConf.ANSI_ENABLED.key -> "false",
            GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "true"
          ) {
            spark.sql(s"CREATE TABLE $tableName (a INT, b STRING) USING iceberg")

            val inputData = MemoryStream[(Int, String)]
            val df = inputData.toDS().toDF("a", "b")
            val checkpointPath = checkpointDir.getCanonicalPath

            def startQuery() = {
              df.writeStream
                .option("checkpointLocation", checkpointPath)
                .format("iceberg")
                .toTable(tableName)
            }

            val query1 = startQuery()
            try {
              inputData.addData((1, "a"))
              query1.processAllAvailable()
              assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query1)
            } finally {
              query1.stop()
            }
            checkTableAnswer(tableName, Seq(Row(1, "a")))
            assertCommitEpochs(checkpointDir, Set("0"))
            assertHasIcebergFilesForEpochs(tableName, Set(0L))

            val query2 = startQuery()
            try {
              inputData.addData((2, "b"))
              query2.processAllAvailable()
              assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query2)
            } finally {
              query2.stop()
            }
            checkTableAnswer(tableName, Seq(Row(1, "a"), Row(2, "b")))
            assertCommitEpochs(checkpointDir, Set("0", "1"))
            assertHasIcebergFilesForEpochs(tableName, Set(0L, 1L))
          }
      }
    }
  }

  test("native Iceberg stream write replays missing Spark commit log without duplicate output") {
    val tableName =
      "iceberg_stream_replay_tbl_" + java.util.UUID.randomUUID().toString.replace("-", "_")
    withTable(tableName) {
      withTempDir {
        checkpointDir =>
          withSQLConf(
            SQLConf.ANSI_ENABLED.key -> "false",
            GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "true"
          ) {
            spark.sql(s"CREATE TABLE $tableName (a INT, b STRING) USING iceberg")

            val inputData = MemoryStream[(Int, String)]
            val df = inputData.toDS().toDF("a", "b")
            val checkpointPath = checkpointDir.getCanonicalPath

            def startQuery() = {
              df.writeStream
                .option("checkpointLocation", checkpointPath)
                .format("iceberg")
                .toTable(tableName)
            }

            val query1 = startQuery()
            try {
              inputData.addData((1, "a"))
              query1.processAllAvailable()
              assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query1)
            } finally {
              query1.stop()
            }
            checkTableAnswer(tableName, Seq(Row(1, "a")))
            assertCommitEpochs(checkpointDir, Set("0"))
            assertHasIcebergFilesForEpochs(tableName, Set(0L))
            val epoch0Files = icebergFileNamesForEpoch(tableName, 0L)
            assert(epoch0Files.nonEmpty)

            deleteCommitEpoch(checkpointDir, "0")
            assertCommitEpochs(checkpointDir, Set.empty)

            val query2 = startQuery()
            try {
              query2.processAllAvailable()
              assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query2)
            } finally {
              query2.stop()
            }
            checkTableAnswer(tableName, Seq(Row(1, "a")))
            assertCommitEpochs(checkpointDir, Set("0"))
            assert(icebergFileNamesForEpoch(tableName, 0L) == epoch0Files)

            deleteCommitEpoch(checkpointDir, "0")
            assertCommitEpochs(checkpointDir, Set.empty)

            val query3 = startQuery()
            try {
              query3.processAllAvailable()
              assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query3)
            } finally {
              query3.stop()
            }
            checkTableAnswer(tableName, Seq(Row(1, "a")))
            assertCommitEpochs(checkpointDir, Set("0"))
            assert(icebergFileNamesForEpoch(tableName, 0L) == epoch0Files)
          }
      }
    }
  }

  test("native Iceberg stream write replays driver failure after sink commit") {
    val tableName =
      "iceberg_stream_driver_commit_failure_tbl_" +
        java.util.UUID.randomUUID().toString.replace("-", "_")
    withTable(tableName) {
      withTempDir {
        checkpointDir =>
          withSQLConf(
            SQLConf.ANSI_ENABLED.key -> "false",
            GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "true"
          ) {
            spark.sql(s"CREATE TABLE $tableName (a INT, b STRING) USING iceberg")

            val inputData = MemoryStream[(Int, String)]
            val df = inputData.toDS().toDF("a", "b")
            val checkpointPath = checkpointDir.getCanonicalPath

            def startQuery() = {
              df.writeStream
                .option("checkpointLocation", checkpointPath)
                .format("iceberg")
                .toTable(tableName)
            }

            val failedQuery = withSQLConf(
              NativeIcebergStreamingSinkTestConfs.failAfterCommitKey -> "true") {
              startQuery()
            }
            try {
              inputData.addData((1, "a"))
              val failure = intercept[StreamingQueryException] {
                failedQuery.processAllAvailable()
              }
              assert(causeMessages(failure).contains(
                "Injected native streaming sink failure after BatchWrite.commit"))
            } finally {
              failedQuery.stop()
            }
            checkTableAnswer(tableName, Seq(Row(1, "a")))
            assertCommitEpochs(checkpointDir, Set.empty)
            val epoch0Files = icebergFileNamesForEpoch(tableName, 0L)
            assert(epoch0Files.nonEmpty)

            val restartedQuery = withSQLConf(
              NativeIcebergStreamingSinkTestConfs.failAfterCommitKey -> "false") {
              startQuery()
            }
            try {
              restartedQuery.processAllAvailable()
              assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](restartedQuery)
            } finally {
              restartedQuery.stop()
            }
            checkTableAnswer(tableName, Seq(Row(1, "a")))
            assertCommitEpochs(checkpointDir, Set("0"))
            assert(icebergFileNamesForEpoch(tableName, 0L) == epoch0Files)
          }
      }
    }
  }

  test("native Iceberg stream write recovers failed epoch without duplicate output") {
    val tableName =
      "iceberg_stream_failed_epoch_tbl_" + java.util.UUID.randomUUID().toString.replace("-", "_")
    withTable(tableName) {
      withTempDir {
        checkpointDir =>
          withSQLConf(
            SQLConf.ANSI_ENABLED.key -> "false",
            GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "true"
          ) {
            spark.sql(s"CREATE TABLE $tableName (a INT, b STRING) USING iceberg")

            NativeIcebergStreamingRetryFailpoint.arm()
            val failOnce = udf {
              value: Int => NativeIcebergStreamingRetryFailpoint.failOnceOnFirstAttempt(value)
            }
            val inputData = MemoryStream[(Int, String)]
            val df = inputData
              .toDS()
              .toDF("a", "b")
              .select(failOnce($"a").as("a"), $"b")
            val checkpointPath = checkpointDir.getCanonicalPath

            def startQuery() = {
              df.writeStream
                .option("checkpointLocation", checkpointPath)
                .format("iceberg")
                .toTable(tableName)
            }

            val failedQuery = startQuery()
            try {
              inputData.addData((1, "a"), (2, "b"))
              val failure = intercept[StreamingQueryException] {
                failedQuery.processAllAvailable()
              }
              assert(failure.getMessage.contains("Injected native Iceberg streaming task failure"))
            } finally {
              failedQuery.stop()
            }

            assert(NativeIcebergStreamingRetryFailpoint.wasTriggered)
            checkTableAnswer(tableName, Seq.empty)
            assertCommitEpochs(checkpointDir, Set.empty)

            val restartedQuery = startQuery()
            try {
              restartedQuery.processAllAvailable()
              assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](restartedQuery)
            } finally {
              restartedQuery.stop()
            }

            checkTableAnswer(tableName, Seq(Row(1, "a"), Row(2, "b")))
            assertCommitEpochs(checkpointDir, Set("0"))
            assertHasIcebergFilesForEpochs(tableName, Set(0L))
          }
      }
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

  private def checkTableAnswer(tableName: String, expectedRows: Seq[Row]): Unit = {
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      checkAnswer(spark.sql(s"SELECT * FROM $tableName ORDER BY a"), expectedRows)
    }
  }

  private def assertCommitEpochs(checkpointDir: java.io.File, expectedEpochs: Set[String]): Unit = {
    val commitDir = new java.io.File(checkpointDir, "commits")
    val committedEpochs = Option(commitDir.list())
      .map(_.filterNot(_.startsWith(".")).toSet)
      .getOrElse(Set.empty[String])
    assert(committedEpochs == expectedEpochs)
  }

  private def deleteCommitEpoch(checkpointDir: java.io.File, epoch: String): Unit = {
    val commitDir = new java.io.File(checkpointDir, "commits")
    val commitFile = new java.io.File(commitDir, epoch)
    assert(commitFile.delete(), s"Expected to delete commit log $commitFile")
    val checksumSidecar = new java.io.File(commitDir, s".$epoch.crc")
    if (checksumSidecar.exists()) {
      assert(checksumSidecar.delete(), s"Expected to delete commit log checksum $checksumSidecar")
    }
  }

  private def assertHasIcebergFilesForEpochs(tableName: String, expectedEpochs: Set[Long]): Unit = {
    expectedEpochs.foreach {
      epochId =>
        assert(
          icebergFileNamesForEpoch(tableName, epochId).nonEmpty,
          s"Expected a native Iceberg file for epoch $epochId, but found: " +
            s"${allIcebergFileNames(tableName).mkString(", ")}"
        )
    }
  }

  private def icebergFileNamesForEpoch(tableName: String, epochId: Long): Set[String] = {
    val epochPattern = s"\\d{5}-.+-$epochId-\\d{5}\\.parquet"
    allIcebergFileNames(tableName).filter(_.matches(epochPattern))
  }

  private def allIcebergFileNames(tableName: String): Set[String] = {
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      spark
        .sql(s"SELECT file_path FROM default.$tableName.files")
        .collect()
        .map(row => row.getString(0).split('/').last)
        .toSet
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

@EnhancedFeaturesTest
class VeloxIcebergNativeStreamingSinkRetrySuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var warehouseDir: File = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    warehouseDir = Files.createTempDirectory("gluten-native-iceberg-retry-warehouse").toFile
    spark = SparkSession
      .builder()
      .master("local[2,2]")
      .appName("VeloxIcebergNativeStreamingSinkRetrySuite")
      .config(sparkConf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        spark = null
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      if (warehouseDir != null) {
        deleteRecursively(warehouseDir)
      }
    } finally {
      super.afterAll()
    }
  }

  test("native Iceberg stream write survives Spark task retry without duplicate output") {
    val sparkSession = spark
    import sparkSession.implicits._
    implicit val sqlContext = sparkSession.sqlContext

    val tableName =
      "iceberg_stream_task_retry_tbl_" + java.util.UUID.randomUUID().toString.replace("-", "_")
    withTable(tableName) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"CREATE TABLE $tableName (a INT, b STRING) USING iceberg")

          NativeIcebergStreamingRetryFailpoint.arm()
          val failOnce = udf {
            value: Int => NativeIcebergStreamingRetryFailpoint.failOnceOnFirstAttempt(value)
          }
          val inputData = MemoryStream[(Int, String)]
          val df = inputData
            .toDS()
            .toDF("a", "b")
            .select(failOnce($"a").as("a"), $"b")
          val checkpointPath = checkpointDir.getCanonicalPath

          val query = df.writeStream
            .option("checkpointLocation", checkpointPath)
            .format("iceberg")
            .toTable(tableName)
          try {
            inputData.addData((1, "a"), (2, "b"))
            query.processAllAvailable()
            assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query)
          } finally {
            query.stop()
          }

          assert(NativeIcebergStreamingRetryFailpoint.wasTriggered)
          assertTableRows(tableName, Seq(Row(1, "a"), Row(2, "b")))
          assertCommitEpochs(checkpointDir, Set("0"))
          assertHasIcebergFilesForEpochs(tableName, Set(0L))
          assertNoRawWarehouseOrphanParquetFiles(tableName)
      }
    }
  }

  test("native Iceberg stream write survives repeated Spark task retries across micro-batches") {
    val sparkSession = spark
    import sparkSession.implicits._
    implicit val sqlContext = sparkSession.sqlContext

    val tableName =
      "iceberg_stream_retry_soak_tbl_" + java.util.UUID.randomUUID().toString.replace("-", "_")
    withTable(tableName) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"CREATE TABLE $tableName (a INT, b STRING) USING iceberg")

          val failOnce = udf {
            value: Int => NativeIcebergStreamingRetryFailpoint.failOnceOnFirstAttempt(value)
          }
          val inputData = MemoryStream[(Int, String)]
          val df = inputData
            .toDS()
            .toDF("a", "b")
            .select(failOnce($"a").as("a"), $"b")
          val checkpointPath = checkpointDir.getCanonicalPath
          val expectedRows = (0 until 3).flatMap {
            batchId =>
              Seq(Row(batchId * 10 + 1, s"a-$batchId"), Row(batchId * 10 + 2, s"b-$batchId"))
          }
          val baselineFailures = NativeIcebergStreamingRetryFailpoint.failureCount

          val query = df.writeStream
            .option("checkpointLocation", checkpointPath)
            .format("iceberg")
            .toTable(tableName)
          try {
            (0 until 3).foreach {
              batchId =>
                NativeIcebergStreamingRetryFailpoint.arm()
                inputData.addData((batchId * 10 + 1, s"a-$batchId"))
                inputData.addData((batchId * 10 + 2, s"b-$batchId"))
                query.processAllAvailable()
                assertLastExecutionPlanContains[VeloxIcebergWriteToDataSourceV2Exec](query)
                assert(
                  NativeIcebergStreamingRetryFailpoint.failureCount == baselineFailures + batchId + 1)
                assertCommitEpochs(checkpointDir, (0 to batchId).map(_.toString).toSet)
                assertNoRawWarehouseOrphanParquetFiles(tableName)
            }
          } finally {
            query.stop()
          }

          assertTableRows(tableName, expectedRows)
          assertHasIcebergFilesForEpochs(tableName, Set(0L, 1L, 2L))
          assertNoRawWarehouseOrphanParquetFiles(tableName)
      }
    }
  }

  private def sparkConf: SparkConf = {
    new SparkConf()
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
  }

  private def withTable(tableName: String)(body: => Unit): Unit = {
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      body
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  private def withTempDir(body: File => Unit): Unit = {
    val dir = Files.createTempDirectory("gluten-native-iceberg-retry-checkpoint").toFile
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

  private def assertLastExecutionPlanContains[T: ClassTag](query: StreamingQuery): Unit = {
    val expectedClass = implicitly[ClassTag[T]].runtimeClass
    val executedPlan =
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
    assert(
      executedPlan.exists(expectedClass.isInstance),
      s"Expected an executed ${expectedClass.getName} plan, observed:\n${executedPlan.treeString}")
  }

  private def assertTableRows(tableName: String, expectedRows: Seq[Row]): Unit = {
    withGlutenDisabled {
      val actualRows = spark.sql(s"SELECT * FROM $tableName ORDER BY a").collect().toSeq
      assert(actualRows == expectedRows)
    }
  }

  private def assertCommitEpochs(checkpointDir: File, expectedEpochs: Set[String]): Unit = {
    val commitDir = new File(checkpointDir, "commits")
    val committedEpochs = Option(commitDir.list())
      .map(_.filterNot(_.startsWith(".")).toSet)
      .getOrElse(Set.empty[String])
    assert(committedEpochs == expectedEpochs)
  }

  private def assertHasIcebergFilesForEpochs(tableName: String, expectedEpochs: Set[Long]): Unit = {
    expectedEpochs.foreach {
      epochId =>
        assert(
          icebergFileNamesForEpoch(tableName, epochId).nonEmpty,
          s"Expected a native Iceberg file for epoch $epochId, but found: " +
            s"${allIcebergFileNames(tableName).mkString(", ")}"
        )
    }
  }

  private def icebergFileNamesForEpoch(tableName: String, epochId: Long): Set[String] = {
    val epochPattern = s"\\d{5}-.+-$epochId-\\d{5}\\.parquet"
    allIcebergFileNames(tableName).filter(_.matches(epochPattern))
  }

  private def allIcebergFileNames(tableName: String): Set[String] = {
    allIcebergRelativePaths(tableName).map(_.split('/').last)
  }

  private def allIcebergRelativePaths(tableName: String): Set[String] = {
    withGlutenDisabled {
      spark
        .sql(s"SELECT file_path FROM default.$tableName.files")
        .collect()
        .map(row => tableRelativePath(tableName, row.getString(0)))
        .toSet
    }
  }

  private def assertNoRawWarehouseOrphanParquetFiles(tableName: String): Unit = {
    val rawFiles = rawWarehouseParquetRelativePaths(tableName)
    val activeFiles = allIcebergRelativePaths(tableName)
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

  private def rawWarehouseParquetRelativePaths(tableName: String): Set[String] = {
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

  private def withGlutenDisabled[T](body: => T): T = {
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

@EnhancedFeaturesTest
class VeloxIcebergNativeStreamingSinkSpeculationSuite
  extends AnyFunSuite
  with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var warehouseDir: File = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    warehouseDir = Files.createTempDirectory("gluten-native-iceberg-speculation-warehouse").toFile
    spark = SparkSession
      .builder()
      .master("local[2,2]")
      .appName("VeloxIcebergNativeStreamingSinkSpeculationSuite")
      .config(sparkConf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        spark = null
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      if (warehouseDir != null) {
        deleteRecursively(warehouseDir)
        warehouseDir = null
      }
    } finally {
      super.afterAll()
    }
  }

  test("native Iceberg streaming sink is disabled when Spark speculation is enabled") {
    val sparkSession = spark
    import sparkSession.implicits._
    implicit val sqlContext = sparkSession.sqlContext

    val tableName =
      "iceberg_stream_speculation_guard_tbl_" +
        java.util.UUID.randomUUID().toString.replace("-", "_")
    withTable(tableName) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"CREATE TABLE $tableName (a INT, b STRING) USING iceberg")

          val inputData = MemoryStream[(Int, String)]
          val df = inputData.toDS().toDF("a", "b")
          val query = df.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("iceberg")
            .toTable(tableName)
          try {
            inputData.addData((1, "a"))
            query.processAllAvailable()
            assertLastExecutionPlanDoesNotContain[VeloxIcebergWriteToDataSourceV2Exec](query)
          } finally {
            query.stop()
          }

          assertTableRows(tableName, Seq(Row(1, "a")))
          assertCommitEpochs(checkpointDir, Set("0"))
      }
    }
  }

  private def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.ui.enabled", "false")
      .set("spark.speculation", "true")
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
  }

  private def withTable(tableName: String)(body: => Unit): Unit = {
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      body
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  private def withTempDir(body: File => Unit): Unit = {
    val dir = Files.createTempDirectory("gluten-native-iceberg-speculation-checkpoint").toFile
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

  private def assertLastExecutionPlanDoesNotContain[T: ClassTag](query: StreamingQuery): Unit = {
    val blockedClass = implicitly[ClassTag[T]].runtimeClass
    val executedPlan =
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
    assert(
      !executedPlan.exists(blockedClass.isInstance),
      s"Expected no executed ${blockedClass.getName} plan under speculation:\n" +
        executedPlan.treeString)
  }

  private def assertTableRows(tableName: String, expectedRows: Seq[Row]): Unit = {
    withGlutenDisabled {
      val actualRows = spark.sql(s"SELECT * FROM $tableName ORDER BY a").collect().toSeq
      assert(actualRows == expectedRows, s"Expected rows $expectedRows but found $actualRows")
    }
  }

  private def assertCommitEpochs(checkpointDir: File, expectedEpochs: Set[String]): Unit = {
    val commitDir = new File(checkpointDir, "commits")
    val committedEpochs = Option(commitDir.list())
      .map(_.filterNot(_.startsWith(".")).toSet)
      .getOrElse(Set.empty[String])
    assert(committedEpochs == expectedEpochs)
  }

  private def withGlutenDisabled[T](body: => T): T = {
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

private object NativeIcebergStreamingSinkTestConfs {
  val failAfterCommitKey: String =
    "spark.gluten.sql.streaming.native.sinks.test.failAfterCommit.enabled"
}

private object NativeIcebergStreamingRetryFailpoint {
  private val failed = new AtomicBoolean(false)
  private val armed = new AtomicBoolean(false)
  private val failures = new AtomicInteger(0)

  def arm(): Unit = {
    failed.set(false)
    armed.set(true)
  }

  def failOnceOnFirstAttempt(value: Int): Int = {
    val context = TaskContext.get()
    if (
      armed.get() && context != null && context.attemptNumber() == 0 &&
      failed.compareAndSet(false, true)
    ) {
      armed.set(false)
      failures.incrementAndGet()
      throw new RuntimeException("Injected native Iceberg streaming task failure")
    }
    value
  }

  def wasTriggered: Boolean = failed.get()

  def failureCount: Int = failures.get()
}
