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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.config.VeloxDeltaConfig
import org.apache.gluten.execution.{DeltaParquetScanTransformer, DeltaScanTransformer, FileSourceScanExecTransformer}

import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.v2.{GlutenDeltaLeafRunnableCommand, GlutenDeltaLeafV2CommandExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.hadoop.fs.Path

import java.io.File
import java.util.concurrent.CopyOnWriteArrayList

import scala.jdk.CollectionConverters._

class DeltaNativeWriteSuite extends DeltaSQLCommandTest {

  import testImplicits._

  private val NativeDmlRowIndexScanKey =
    "spark.gluten.sql.delta.enableNativeDmlRowIndexScan"
  private val NativeBitmapAggregationKey =
    "spark.gluten.sql.delta.delete.dv.enableNativeBitmapAggregation"
  private val PlainParquetTargetScanKey =
    "spark.gluten.sql.delta.delete.dv.enablePlainParquetTargetScan"
  private val DriverMergeMaxFilesKey =
    "spark.gluten.sql.delta.delete.dv.driverMergeMaxFiles"
  private val SkipAllFilesInCrcForDvDeleteKey =
    GlutenOptimisticTransaction.SkipAllFilesInCrcForDvDeleteKey
  private val SkipChecksumForDvDeleteKey =
    GlutenOptimisticTransaction.SkipChecksumForDvDeleteKey

  private lazy val isMac = sys.props
    .get("os.name")
    .exists(_.toLowerCase(java.util.Locale.ROOT).contains("mac"))

  private def withNativeWriteOffloadConf[T](f: => T): T = {
    val confs = Seq(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false",
      "spark.sql.debug.maxToStringFields" -> "2000",
      DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false"
    ) ++
      (if (isMac) {
         Seq(GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> "false")
       } else {
         Seq.empty
       })

    withSQLConf(confs: _*) {
      assert(
        !spark.sessionState.conf.ansiEnabled,
        s"${SQLConf.ANSI_ENABLED.key} should be false in native write tests")
      assert(
        spark.sessionState.conf.sessionLocalTimeZone == "UTC",
        s"${SQLConf.SESSION_LOCAL_TIMEZONE.key} should be UTC in native write tests")
      assert(
        !spark.sessionState.conf
          .getConfString(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key)
          .toBoolean,
        s"${GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key} should be false in native write tests"
      )
      assert(
        !spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS),
        s"${DeltaSQLConf.DELTA_COLLECT_STATS.key} should be false in native write tests")
      if (isMac) {
        assert(
          !spark.sessionState.conf
            .getConfString(GlutenConfig.NATIVE_VALIDATION_ENABLED.key)
            .toBoolean,
          s"${GlutenConfig.NATIVE_VALIDATION_ENABLED.key} should be false on macOS"
        )
      }
      f
    }
  }

  private def hasGlutenDeltaWriteCommand(plan: SparkPlan): Boolean = {
    val nativeClassMatch = plan
      .collectFirst {
        case ExecutedCommandExec(_: GlutenDeltaLeafRunnableCommand) => true
        case _: GlutenDeltaLeafV2CommandExec => true
      }
      .getOrElse(false)

    val nativeNodeMatch = plan
      .collectFirst {
        case p if p.nodeName.startsWith("Execute GlutenDelta ") => true
        case p if p.nodeName.startsWith("GlutenDelta ") => true
      }
      .getOrElse(false)

    val nativeTreeMatch = plan.treeString.contains("GlutenDelta ")

    nativeClassMatch || nativeNodeMatch || nativeTreeMatch
  }

  private def hasDeltaScanTransformer(plan: SparkPlan): Boolean = {
    plan.collectFirst { case _: DeltaScanTransformer => true }.getOrElse(false) ||
    plan.treeString.contains("DeltaScanTransformer")
  }

  private def containsDeletionVectorRowIndex(plan: SparkPlan): Boolean = {
    val planText = plan.treeString
    planText.contains("__delta_internal_row_index") ||
    planText.contains("rowIndexCol") ||
    planText.contains("_tmp_metadata_row_index")
  }

  private def hasNativeDeltaScanWithDeletionVectorRowIndex(plan: SparkPlan): Boolean = {
    plan.exists {
      case scan: DeltaScanTransformer =>
        val scanColumnNames = (scan.output.map(_.name) ++ scan.requiredSchema.fieldNames).toSet
        scanColumnNames.exists {
          name =>
            name == "__delta_internal_row_index" ||
            name == "rowIndexCol" ||
            name == "_tmp_metadata_row_index"
        }
      case scan: DeltaParquetScanTransformer =>
        val scanColumnNames = (scan.output.map(_.name) ++ scan.requiredSchema.fieldNames).toSet
        scanColumnNames.exists {
          name =>
            name == "__delta_internal_row_index" ||
            name == "rowIndexCol" ||
            name == "_tmp_metadata_row_index"
        }
      case _ => false
    } || {
      val planText = plan.treeString
      (planText.contains("DeltaScanTransformer") ||
        planText.contains("DeltaParquetScanTransformer")) &&
      containsDeletionVectorRowIndex(plan)
    }
  }

  private def hasNativeParquetRowIndexScanWithPlainFileIndex(plan: SparkPlan): Boolean = {
    plan.exists {
      case scan: DeltaParquetScanTransformer =>
        scan.relation.location.isInstanceOf[InMemoryFileIndex] && {
          val scanColumnNames = (scan.output.map(_.name) ++ scan.requiredSchema.fieldNames).toSet
          scanColumnNames.exists {
            name =>
              name == "__delta_internal_row_index" ||
              name == "rowIndexCol" ||
              name == "_tmp_metadata_row_index"
          }
        }
      case scan: FileSourceScanExecTransformer =>
        scan.relation.location.isInstanceOf[InMemoryFileIndex] && {
          val scanColumnNames = (scan.output.map(_.name) ++ scan.requiredSchema.fieldNames).toSet
          scanColumnNames.exists {
            name =>
              name == "__delta_internal_row_index" ||
              name == "rowIndexCol" ||
              name == "_tmp_metadata_row_index"
          }
        }
      case _ => false
    } || {
      val planText = plan.treeString
      (planText.contains("DeltaParquetScanTransformer") ||
        planText.contains("FileSourceScanExecTransformer")) &&
      planText.contains("InMemoryFileIndex") &&
      containsDeletionVectorRowIndex(plan)
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

  private def assertContainsNativeWriteCommand(plans: Seq[SparkPlan], context: String): Unit = {
    assert(
      plans.exists(hasGlutenDeltaWriteCommand),
      s"Expected native delta write command for $context, but got plans:\n" +
        plans.map(_.treeString).mkString("\n---\n")
    )
  }

  private def assertNoNativeWriteCommand(plans: Seq[SparkPlan], context: String): Unit = {
    assert(
      !plans.exists(hasGlutenDeltaWriteCommand),
      s"Expected no native delta write command for $context, but got plans:\n" +
        plans.map(_.treeString).mkString("\n---\n")
    )
  }

  private def assertContainsNativeDeltaScan(plans: Seq[SparkPlan], context: String): Unit = {
    assert(
      plans.exists(hasDeltaScanTransformer),
      s"Expected native delta scan for $context, but got plans:\n" +
        plans.map(_.treeString).mkString("\n---\n")
    )
  }

  private def assertNoNativeDeltaScan(plans: Seq[SparkPlan], context: String): Unit = {
    assert(
      !plans.exists(hasDeltaScanTransformer),
      s"Expected no native delta scan for $context, but got plans:\n" +
        plans.map(_.treeString).mkString("\n---\n")
    )
  }

  private def assertNoNativeDmlRowIndexScan(plans: Seq[SparkPlan], context: String): Unit = {
    assert(
      !plans.exists(hasNativeDeltaScanWithDeletionVectorRowIndex),
      s"Expected no native Delta row-index DML scan for $context, but got plans:\n" +
        plans.map(_.treeString).mkString("\n---\n")
    )
  }

  private def assertContainsNativeDmlRowIndexScan(
      plans: Seq[SparkPlan],
      context: String): Unit = {
    assert(
      plans.exists(hasNativeDeltaScanWithDeletionVectorRowIndex),
      s"Expected native Delta row-index DML scan for $context, but got plans:\n" +
        plans.map(_.treeString).mkString("\n---\n")
    )
  }

  private def assertContainsPlainFileIndexNativeDmlRowIndexScan(
      plans: Seq[SparkPlan],
      context: String): Unit = {
    assert(
      plans.exists(hasNativeParquetRowIndexScanWithPlainFileIndex),
      s"Expected native Parquet row-index DML scan to use a plain file index for $context, " +
        s"but got plans:\n${plans.map(_.treeString).mkString("\n---\n")}"
    )
  }

  private def assertContainsDeletionVectorRowIndex(plans: Seq[SparkPlan], context: String): Unit = {
    assert(
      plans.exists(containsDeletionVectorRowIndex),
      s"Expected DELETE target scan to retain a deletion-vector row index for $context, " +
        s"but got plans:\n${plans.map(_.treeString).mkString("\n---\n")}"
    )
  }

  private def assertNativeDvRead(
      plans: Seq[SparkPlan],
      context: String): Unit = {
    assertContainsNativeDeltaScan(plans, context)
    val nativeDeltaPlanText = plans
      .filter(hasDeltaScanTransformer)
      .map(_.treeString)
      .mkString("\n---\n")
    assert(!nativeDeltaPlanText.contains("__delta_internal_is_row_deleted"))
    assert(!nativeDeltaPlanText.contains("__delta_internal_row_index"))
  }

  private def withDeletionVectorDeleteConf[T](
      useMetadataRowIndex: Boolean = true)(f: => T): T = {
    withSQLConf(
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true",
      DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key ->
        useMetadataRowIndex.toString
    ) {
      f
    }
  }

  private def enableDeletionVectorsInTable(path: String): Unit = {
    spark.sql(
      s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
  }

  private def assertDeleteMetrics(path: String, numDeletedRows: Long): Unit = {
    val deleteMetrics =
      DeltaMetricsUtils.getLastOperationMetrics(io.delta.tables.DeltaTable.forPath(path))
    assert(deleteMetrics.getOrElse("numDeletedRows", -1L) === numDeletedRows)
  }

  private def checksumForLatestVersion(log: DeltaLog): VersionChecksum = {
    val version = log.update().version
    log
      .readChecksum(version)
      .getOrElse(fail(s"Expected checksum file for Delta version $version"))
  }

  private def getFilesWithDeletionVectors(log: DeltaLog): Seq[AddFile] =
    log.update().allFiles.collect().filter(_.deletionVector != null).toSeq

  private def getFileActionsInLastVersion(log: DeltaLog): (Seq[AddFile], Seq[RemoveFile]) = {
    val version = log.update().version
    val allFiles = log.getChanges(version).toSeq.head._2
    val add = allFiles.collect { case a: AddFile => a }
    val remove = allFiles.collect { case r: RemoveFile => r }
    (add, remove)
  }

  private def assertDeletionVectorsExist(log: DeltaLog, filesWithDVs: Seq[AddFile]): Unit = {
    val tablePath = new Path(log.dataPath.toUri.getPath)
    val dvStore = DeletionVectorStore.createInstance(spark.sessionState.newHadoopConf())
    for (file <- filesWithDVs) {
      val dv = file.deletionVector
      assert(dv != null)
      assert(dv.isOnDisk && !dv.isInline)
      assert(dv.offset.isDefined)

      val dvPath = dv.absolutePath(tablePath)
      assert(new File(dvPath.toString).exists(), s"DV not found $dvPath")

      val bitmap = dvStore.read(dvPath, dv.offset.get, dv.sizeInBytes)
      assert(dv.cardinality === bitmap.cardinality)
    }
  }

  private def deletionVectorUniqueId(file: AddFile): String = {
    Option(file.deletionVector)
      .map(_.uniqueId)
      .getOrElse(fail(s"Expected AddFile ${file.path} to have a deletion vector"))
  }

  private def deletionVectorUniqueId(file: RemoveFile): String = {
    Option(file.deletionVector)
      .map(_.uniqueId)
      .getOrElse(fail(s"Expected RemoveFile ${file.path} to have a deletion vector"))
  }

  private def deletionVectorIdsByPath(files: Seq[AddFile]): Map[String, String] =
    files.map(file => file.path -> deletionVectorUniqueId(file)).toMap

  private def assertDeletionVectorReplacement(
      addFile: AddFile,
      removeFile: RemoveFile,
      previousUniqueId: Option[String] = None): Unit = {
    assert(addFile.path === removeFile.path)
    val addedDv = Option(addFile.deletionVector)
      .getOrElse(fail(s"Expected AddFile ${addFile.path} to have a deletion vector"))
    val removedDv = Option(removeFile.deletionVector)
      .getOrElse(fail(s"Expected RemoveFile ${removeFile.path} to have a deletion vector"))
    val removedUniqueId = deletionVectorUniqueId(removeFile)
    previousUniqueId.foreach(expected => assert(removedUniqueId === expected))
    assert(addedDv.uniqueId != removedUniqueId)
    assert(addedDv.cardinality >= removedDv.cardinality)
  }

  private def assertDeletionVectorStatsAreWide(filesWithDVs: Seq[AddFile]): Unit = {
    for (file <- filesWithDVs) {
      val dv = Option(file.deletionVector)
        .getOrElse(fail(s"Expected AddFile ${file.path} to have a deletion vector"))
      val physicalRecords =
        file.numPhysicalRecords.getOrElse(fail(s"Missing physical row count for ${file.path}"))
      val logicalRecords =
        file.numLogicalRecords.getOrElse(fail(s"Missing logical row count for ${file.path}"))
      val stats = JsonUtils.mapper.readTree(file.stats)
      assert(stats.has("numRecords"))
      assert(stats.get("numRecords").asLong() === physicalRecords)
      assert(logicalRecords === physicalRecords - dv.cardinality)
      assert(!stats.get("tightBounds").asBoolean())
    }
  }

  test("native delta delete command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value").write.format("delta").save(path)

          val deleteDf = sql(s"DELETE FROM delta.`$path` WHERE id = 1")
          assertContainsNativeWriteCommand(Seq(deleteDf.queryExecution.executedPlan), "DELETE")
          deleteDf.collect()

          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(2, "b"), Row(3, "c")))
      }
    }
  }

  test("native delta delete should create a deletion vector") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
              .toDF("id", "value")
              .coalesce(1)
              .write
              .format("delta")
              .save(path)
            enableDeletionVectorsInTable(path)

            val plans = collectExecutedPlans {
              sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
            }

            assertContainsNativeWriteCommand(plans, "DELETE creating a deletion vector")
            assertNoNativeDmlRowIndexScan(plans, "DELETE creating a deletion vector")
            assertContainsDeletionVectorRowIndex(plans, "DELETE creating a deletion vector")
            assertDeleteMetrics(path, numDeletedRows = 2)

            val result = spark.read.format("delta").load(path)
            assert(result.collect().toSet == Set(
              Row(1, "a"),
              Row(3, "c"),
              Row(4, "d"),
              Row(6, "f")))

            val log = DeltaLog.forTable(spark, new Path(path))
            val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
            assert(addFiles.size === 1)
            assert(removeFiles.size === 1)
            assert(addFiles.head.path === removeFiles.head.path)
            assert(addFiles.head.deletionVector.cardinality === 2)
            assert(removeFiles.head.deletionVector == null)
            assertDeletionVectorsExist(log, addFiles)
            assertDeletionVectorStatsAreWide(addFiles)
        }
      }
    }
  }

  test("native delta delete DV checksum configs should be scoped and restore confs") {
    withNativeWriteOffloadConf {
      withSQLConf(
        SkipAllFilesInCrcForDvDeleteKey -> "true",
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key -> "true"
      ) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
              .toDF("id", "value")
              .coalesce(1)
              .write
              .format("delta")
              .save(path)

            sql(s"DELETE FROM delta.`$path` WHERE id = 1").collect()

            val log = DeltaLog.forTable(spark, new Path(path))
            assert(spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED))
            assert(checksumForLatestVersion(log).allFiles.nonEmpty)
            assert(spark.read.format("delta").load(path).collect().toSet == Set(
              Row(2, "b"),
              Row(3, "c"),
              Row(4, "d")))
        }

        withDeletionVectorDeleteConf() {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
                .toDF("id", "value")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              enableDeletionVectorsInTable(path)

              sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 4)").collect()

              val log = DeltaLog.forTable(spark, new Path(path))
              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.exists(_.deletionVector != null))
              assert(removeFiles.exists(_.deletionVector == null))
              assert(spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED))
              assert(checksumForLatestVersion(log).allFiles.isEmpty)
              assert(spark.read.format("delta").load(path).collect().toSet == Set(
                Row(1, "a"),
                Row(3, "c")))
          }
        }
      }

      withSQLConf(
        SkipChecksumForDvDeleteKey -> "true",
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key -> "true") {
        withDeletionVectorDeleteConf() {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
                .toDF("id", "value")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              enableDeletionVectorsInTable(path)

              sql(s"DELETE FROM delta.`$path` WHERE id IN (1, 3)").collect()

              val log = DeltaLog.forTable(spark, new Path(path))
              val version = log.update().version
              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.exists(_.deletionVector != null))
              assert(removeFiles.exists(_.deletionVector == null))
              assert(spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED))
              assert(spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED))
              assert(log.readChecksum(version).isEmpty)
              assert(spark.read.format("delta").load(path).collect().toSet == Set(
                Row(2, "b"),
                Row(4, "d")))
          }
        }
      }
    }
  }

  test("native delta delete should create a deletion vector with native row index scan") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          NativeBitmapAggregationKey -> "true",
          PlainParquetTargetScanKey -> "false") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
                .toDF("id", "value")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              enableDeletionVectorsInTable(path)

              val plans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
              }

              assertContainsNativeWriteCommand(plans, "native row-index DELETE creating a DV")
              assertContainsPlainFileIndexNativeDmlRowIndexScan(
                plans,
                "native row-index DELETE creating a DV")
              assertContainsDeletionVectorRowIndex(plans, "native row-index DELETE creating a DV")
              assertDeleteMetrics(path, numDeletedRows = 2)

              val result = spark.read.format("delta").load(path)
              assert(result.collect().toSet == Set(
                Row(1, "a"),
                Row(3, "c"),
                Row(4, "d"),
                Row(6, "f")))

              val log = DeltaLog.forTable(spark, new Path(path))
              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 1)
              assert(removeFiles.size === 1)
              assert(addFiles.head.path === removeFiles.head.path)
              assert(addFiles.head.deletionVector.cardinality === 2)
              assert(removeFiles.head.deletionVector == null)
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
          }
        }
      }
    }
  }

  test("native delta delete should update an existing deletion vector with native row index scan") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          NativeBitmapAggregationKey -> "true",
          PlainParquetTargetScanKey -> "false") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
                .toDF("id", "value")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              enableDeletionVectorsInTable(path)

              withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (5, 6)").collect()
              }

              val log = DeltaLog.forTable(spark, new Path(path))
              val initialDvFiles = getFilesWithDeletionVectors(log)
              assert(initialDvFiles.size === 1)
              assert(initialDvFiles.map(_.deletionVector.cardinality).sum === 2)
              val initialDvIds = deletionVectorIdsByPath(initialDvFiles)

              val plans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
              }

              assertContainsNativeWriteCommand(
                plans,
                "native row-index DELETE updating a DV")
              assertContainsPlainFileIndexNativeDmlRowIndexScan(
                plans,
                "native row-index DELETE updating a DV")
              assertContainsDeletionVectorRowIndex(
                plans,
                "native row-index DELETE updating a DV")
              assertDeleteMetrics(path, numDeletedRows = 1)

              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 1)
              assert(removeFiles.size === 1)
              assert(addFiles.head.path === removeFiles.head.path)
              assert(addFiles.head.deletionVector.cardinality === 3)
              assert(removeFiles.head.deletionVector.cardinality === 2)
              assertDeletionVectorReplacement(
                addFiles.head,
                removeFiles.head,
                initialDvIds.get(removeFiles.head.path))
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)

              val sparkResult = withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
                spark.read.format("delta").load(path).collect().toSet
              }
              assert(sparkResult == Set(Row(1, "a"), Row(3, "c"), Row(4, "d")))

              val result = spark.read.format("delta").load(path)
              assert(result.collect().toSet == Set(Row(1, "a"), Row(3, "c"), Row(4, "d")))
          }
        }
      }
    }
  }

  test(
    "native delta delete should create a deletion vector with default plain Parquet target scan") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          NativeBitmapAggregationKey -> "true") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
                .toDF("id", "value")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              enableDeletionVectorsInTable(path)

              val plans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
              }

              assertContainsNativeWriteCommand(
                plans,
                "default plain Parquet target DELETE creating a DV")
              assertNoNativeDmlRowIndexScan(
                plans,
                "default plain Parquet target DELETE creating a DV")
              assertContainsDeletionVectorRowIndex(
                plans,
                "default plain Parquet target DELETE creating a DV")
              assertDeleteMetrics(path, numDeletedRows = 2)

              val result = spark.read.format("delta").load(path)
              assert(result.collect().toSet == Set(
                Row(1, "a"),
                Row(3, "c"),
                Row(4, "d"),
                Row(6, "f")))

              val log = DeltaLog.forTable(spark, new Path(path))
              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 1)
              assert(removeFiles.size === 1)
              assert(addFiles.head.path === removeFiles.head.path)
              assert(addFiles.head.deletionVector.cardinality === 2)
              assert(removeFiles.head.deletionVector == null)
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
          }
        }
      }
    }
  }

  test("native delta delete should use plain Parquet target scan for existing DVs") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          NativeBitmapAggregationKey -> "true",
          PlainParquetTargetScanKey -> "true") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
                .toDF("id", "value")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              enableDeletionVectorsInTable(path)

              withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (5, 6)").collect()
              }

              val log = DeltaLog.forTable(spark, new Path(path))
              val initialDvFiles = getFilesWithDeletionVectors(log)
              assert(initialDvFiles.size === 1)
              assert(initialDvFiles.map(_.deletionVector.cardinality).sum === 2)
              val initialDvIds = deletionVectorIdsByPath(initialDvFiles)

              val plans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
              }

              assertContainsNativeWriteCommand(plans, "plain Parquet DELETE updating a DV")
              assertNoNativeDmlRowIndexScan(plans, "plain Parquet DELETE updating a DV")
              assertContainsDeletionVectorRowIndex(
                plans,
                "plain Parquet DELETE updating a DV")
              assertDeleteMetrics(path, numDeletedRows = 1)

              val result = spark.read.format("delta").load(path)
              assert(result.collect().toSet == Set(Row(1, "a"), Row(3, "c"), Row(4, "d")))

              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 1)
              assert(removeFiles.size === 1)
              assert(addFiles.head.path === removeFiles.head.path)
              assert(addFiles.head.deletionVector.cardinality === 3)
              assert(removeFiles.head.deletionVector.cardinality === 2)
              assertDeletionVectorReplacement(
                addFiles.head,
                removeFiles.head,
                initialDvIds.get(removeFiles.head.path))
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
          }
        }
      }
    }
  }

  test("native delta delete should default native bitmap aggregation past driver merge limit") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          PlainParquetTargetScanKey -> "true",
          DriverMergeMaxFilesKey -> "0") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
                .toDF("id", "value")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              enableDeletionVectorsInTable(path)

              withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (5, 6)").collect()
              }

              val log = DeltaLog.forTable(spark, new Path(path))
              val initialDvFiles = getFilesWithDeletionVectors(log)
              assert(initialDvFiles.size === 1)
              assert(initialDvFiles.map(_.deletionVector.cardinality).sum === 2)
              val initialDvIds = deletionVectorIdsByPath(initialDvFiles)

              val plans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
              }

              assertContainsNativeWriteCommand(
                plans,
                "native bitmap aggregation plain Parquet DELETE updating a DV")
              assertNoNativeDmlRowIndexScan(
                plans,
                "native bitmap aggregation plain Parquet DELETE updating a DV")
              assertContainsDeletionVectorRowIndex(
                plans,
                "native bitmap aggregation plain Parquet DELETE updating a DV")
              assertDeleteMetrics(path, numDeletedRows = 1)

              val result = spark.read.format("delta").load(path)
              assert(result.collect().toSet == Set(Row(1, "a"), Row(3, "c"), Row(4, "d")))

              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 1)
              assert(removeFiles.size === 1)
              assert(addFiles.head.path === removeFiles.head.path)
              assert(addFiles.head.deletionVector.cardinality === 3)
              assert(removeFiles.head.deletionVector.cardinality === 2)
              assertDeletionVectorReplacement(
                addFiles.head,
                removeFiles.head,
                initialDvIds.get(removeFiles.head.path))
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
          }
        }
      }
    }
  }

  test("native delta delete should create deletion vectors in multiple files with plain Parquet target scan") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          NativeBitmapAggregationKey -> "true",
          PlainParquetTargetScanKey -> "true") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq(
                (1, "a", 1),
                (2, "b", 0),
                (3, "c", 1),
                (4, "d", 0))
                .toDF("id", "value", "bucket")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              Seq(
                (5, "e", 1),
                (6, "f", 0),
                (7, "g", 1),
                (8, "h", 0))
                .toDF("id", "value", "bucket")
                .coalesce(1)
                .write
                .format("delta")
                .mode("append")
                .save(path)
              enableDeletionVectorsInTable(path)

              val plans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 3, 6, 7)").collect()
              }

              assertContainsNativeWriteCommand(plans, "plain Parquet target DELETE across files")
              assertNoNativeDmlRowIndexScan(plans, "plain Parquet target DELETE across files")
              assertContainsDeletionVectorRowIndex(
                plans,
                "plain Parquet target DELETE across files")
              assertDeleteMetrics(path, numDeletedRows = 4)

              val result = spark.read.format("delta").load(path)
              assert(
                result.select("id", "value", "bucket").collect().toSet == Set(
                  Row(1, "a", 1),
                  Row(4, "d", 0),
                  Row(5, "e", 1),
                  Row(8, "h", 0)))

              val log = DeltaLog.forTable(spark, new Path(path))
              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 2)
              assert(removeFiles.size === 2)
              assert(addFiles.map(_.path).toSet === removeFiles.map(_.path).toSet)
              assert(addFiles.map(_.deletionVector.cardinality).sum === 4)
              assert(removeFiles.forall(_.deletionVector == null))
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
          }
        }
      }
    }
  }

  test(
    "native delta delete should handle repeated plain Parquet target deletes with null predicates") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          NativeBitmapAggregationKey -> "true",
          PlainParquetTargetScanKey -> "true") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq(
                (1, Option("alpha"), Option(1)),
                (2, Option.empty[String], Option(1)),
                (3, Option("bravo"), Option.empty[Int]),
                (4, Option("alpha"), Option.empty[Int]),
                (5, Option("charlie"), Option(1)),
                (6, Option.empty[String], Option.empty[Int])
              )
                .toDF("id", "value", "flag")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              enableDeletionVectorsInTable(path)

              val firstPlans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE value IS NULL OR (flag IS NULL AND id = 4)")
                  .collect()
              }

              assertContainsNativeWriteCommand(firstPlans, "first null predicate DELETE")
              assertNoNativeDmlRowIndexScan(firstPlans, "first null predicate DELETE")
              assertContainsDeletionVectorRowIndex(firstPlans, "first null predicate DELETE")
              assertDeleteMetrics(path, numDeletedRows = 3)

              val log = DeltaLog.forTable(spark, new Path(path))
              val firstDvFiles = getFilesWithDeletionVectors(log)
              assert(firstDvFiles.size === 1)
              assert(firstDvFiles.map(_.deletionVector.cardinality).sum === 3)
              assertDeletionVectorsExist(log, firstDvFiles)
              val firstDvIds = deletionVectorIdsByPath(firstDvFiles)

              val secondPlans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE value = 'alpha' OR id IN (2, 5)").collect()
              }

              assertContainsNativeWriteCommand(secondPlans, "second overlapping predicate DELETE")
              assertNoNativeDmlRowIndexScan(secondPlans, "second overlapping predicate DELETE")
              assertContainsDeletionVectorRowIndex(
                secondPlans,
                "second overlapping predicate DELETE")
              assertDeleteMetrics(path, numDeletedRows = 2)

              val result = spark.read.format("delta").load(path)
              assert(result.select("id", "value", "flag").collect().toSet == Set(
                Row(3, "bravo", null)))

              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 1)
              assert(removeFiles.size === 1)
              assert(addFiles.head.path === removeFiles.head.path)
              assert(addFiles.head.deletionVector.cardinality === 5)
              assert(removeFiles.head.deletionVector.cardinality === 3)
              assertDeletionVectorReplacement(
                addFiles.head,
                removeFiles.head,
                firstDvIds.get(removeFiles.head.path))
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
          }
        }
      }
    }
  }

  test("native delta delete should update an existing deletion vector") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
              .toDF("id", "value")
              .coalesce(1)
              .write
              .format("delta")
              .save(path)
            enableDeletionVectorsInTable(path)

            withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
              sql(s"DELETE FROM delta.`$path` WHERE id IN (5, 6)").collect()
            }

            val log = DeltaLog.forTable(spark, new Path(path))
            val initialDvFiles = getFilesWithDeletionVectors(log)
            assert(initialDvFiles.size === 1)
            assert(initialDvFiles.map(_.deletionVector.cardinality).sum === 2)
            assertDeletionVectorsExist(log, initialDvFiles)
            val initialDvIds = deletionVectorIdsByPath(initialDvFiles)

            val plans = collectExecutedPlans {
              sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
            }

            assertContainsNativeWriteCommand(plans, "DELETE with an existing deletion vector")
            assertNoNativeDmlRowIndexScan(plans, "DELETE with an existing deletion vector")
            assertContainsDeletionVectorRowIndex(plans, "DELETE with an existing deletion vector")
            assertDeleteMetrics(path, numDeletedRows = 1)

            val result = spark.read.format("delta").load(path)
            assert(result.collect().toSet == Set(Row(1, "a"), Row(3, "c"), Row(4, "d")))

            val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
            assert(addFiles.size === 1)
            assert(removeFiles.size === 1)
            assert(addFiles.head.path === removeFiles.head.path)
            assert(addFiles.head.deletionVector.cardinality === 3)
            assert(removeFiles.head.deletionVector.cardinality === 2)
            assertDeletionVectorReplacement(
              addFiles.head,
              removeFiles.head,
              initialDvIds.get(removeFiles.head.path))
            assertDeletionVectorsExist(log, addFiles)
            assertDeletionVectorStatsAreWide(addFiles)
        }
      }
    }
  }

  test("native delta delete should update partitioned deletion vectors") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          NativeBitmapAggregationKey -> "true",
          PlainParquetTargetScanKey -> "true") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq(
                (1, "a", 0),
                (2, "b", 0),
                (3, "c", 0),
                (4, "d", 0),
                (5, "e", 1),
                (6, "f", 1),
                (7, "g", 1),
                (8, "h", 1))
                .toDF("id", "value", "part")
                .repartition(2, $"part")
                .write
                .format("delta")
                .partitionBy("part")
                .save(path)
              enableDeletionVectorsInTable(path)

              withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 7)").collect()
              }

              val log = DeltaLog.forTable(spark, new Path(path))
              val initialDvFiles = getFilesWithDeletionVectors(log)
              assert(initialDvFiles.size === 2)
              assert(initialDvFiles.map(_.deletionVector.cardinality).sum === 2)
              assertDeletionVectorsExist(log, initialDvFiles)
              val initialDvIds = deletionVectorIdsByPath(initialDvFiles)

              val plans = collectExecutedPlans {
                sql(
                  s"DELETE FROM delta.`$path` " +
                    "WHERE (part = 0 AND id IN (1, 2)) OR (part = 1 AND id IN (6, 7))")
                  .collect()
              }

              assertContainsNativeWriteCommand(plans, "partitioned DELETE with existing DVs")
              assertNoNativeDmlRowIndexScan(plans, "partitioned DELETE with existing DVs")
              assertContainsPlainFileIndexNativeDmlRowIndexScan(
                plans,
                "partitioned DELETE with existing DVs")
              assertContainsDeletionVectorRowIndex(plans, "partitioned DELETE with existing DVs")
              assertDeleteMetrics(path, numDeletedRows = 2)

              val result = spark.read.format("delta").load(path)
              assert(
                result.select("id", "value", "part").collect().toSet == Set(
                  Row(3, "c", 0),
                  Row(4, "d", 0),
                  Row(5, "e", 1),
                  Row(8, "h", 1)))

              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 2)
              assert(removeFiles.size === 2)
              assert(addFiles.map(_.path).toSet === removeFiles.map(_.path).toSet)
              assert(addFiles.map(_.deletionVector.cardinality).sum === 4)
              assert(removeFiles.map(_.deletionVector.cardinality).sum === 2)
              val removeFilesByPath = removeFiles.map(file => file.path -> file).toMap
              addFiles.foreach {
                addFile =>
                  val removeFile = removeFilesByPath(addFile.path)
                  assertDeletionVectorReplacement(
                    addFile,
                    removeFile,
                    initialDvIds.get(addFile.path))
              }
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
          }
        }
      }
    }
  }

  test("native delta delete should create and update deletion vectors with column mapping") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          NativeBitmapAggregationKey -> "true",
          PlainParquetTargetScanKey -> "true") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
                .toDF("id", "value col")
                .coalesce(1)
                .write
                .format("delta")
                .option(DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")
                .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "true")
                .save(path)

              val plans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
              }

              assertContainsNativeWriteCommand(plans, "column mapping DELETE creating a DV")
              assertNoNativeDmlRowIndexScan(plans, "column mapping DELETE creating a DV")
              assertContainsPlainFileIndexNativeDmlRowIndexScan(
                plans,
                "column mapping DELETE creating a DV")
              assertContainsDeletionVectorRowIndex(plans, "column mapping DELETE creating a DV")
              assertDeleteMetrics(path, numDeletedRows = 2)

              val result = spark.read.format("delta").load(path)
              assert(result.collect().toSet == Set(
                Row(1, "a"),
                Row(3, "c"),
                Row(4, "d"),
                Row(6, "f")))

              val log = DeltaLog.forTable(spark, new Path(path))
              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 1)
              assert(removeFiles.size === 1)
              assert(addFiles.head.path === removeFiles.head.path)
              assert(addFiles.head.deletionVector.cardinality === 2)
              assert(removeFiles.head.deletionVector == null)
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
              val firstDvIds = deletionVectorIdsByPath(addFiles)

              val updatePlans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 4)").collect()
              }

              assertContainsNativeWriteCommand(updatePlans, "column mapping DELETE updating a DV")
              assertNoNativeDmlRowIndexScan(
                updatePlans,
                "column mapping DELETE updating a DV")
              assertContainsPlainFileIndexNativeDmlRowIndexScan(
                updatePlans,
                "column mapping DELETE updating a DV")
              assertContainsDeletionVectorRowIndex(
                updatePlans,
                "column mapping DELETE updating a DV")
              assertDeleteMetrics(path, numDeletedRows = 1)

              val updatedResult = spark.read.format("delta").load(path)
              assert(updatedResult.collect().toSet == Set(
                Row(1, "a"),
                Row(3, "c"),
                Row(6, "f")))

              val (updatedAddFiles, updatedRemoveFiles) = getFileActionsInLastVersion(log)
              assert(updatedAddFiles.size === 1)
              assert(updatedRemoveFiles.size === 1)
              assert(updatedAddFiles.head.path === updatedRemoveFiles.head.path)
              assert(updatedAddFiles.head.deletionVector.cardinality === 3)
              assert(updatedRemoveFiles.head.deletionVector.cardinality === 2)
              assertDeletionVectorReplacement(
                updatedAddFiles.head,
                updatedRemoveFiles.head,
                firstDvIds.get(updatedRemoveFiles.head.path))
              assertDeletionVectorsExist(log, updatedAddFiles)
              assertDeletionVectorStatsAreWide(updatedAddFiles)
          }
        }
      }
    }
  }

  test("native delta delete should update partitioned deletion vectors with column mapping") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf() {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          PlainParquetTargetScanKey -> "true") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq(
                (1, "a", 0),
                (2, "b", 0),
                (3, "c", 0),
                (4, "d", 0),
                (5, "e", 1),
                (6, "f", 1),
                (7, "g", 1),
                (8, "h", 1))
                .toDF("id", "value col", "part col")
                .repartition(2, $"part col")
                .write
                .format("delta")
                .option(DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")
                .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "true")
                .partitionBy("part col")
                .save(path)

              withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 7)").collect()
              }

              val log = DeltaLog.forTable(spark, new Path(path))
              val initialDvFiles = getFilesWithDeletionVectors(log)
              assert(initialDvFiles.size === 2)
              assert(initialDvFiles.map(_.deletionVector.cardinality).sum === 2)
              val initialDvIds = deletionVectorIdsByPath(initialDvFiles)

              val plans = collectExecutedPlans {
                sql(
                  s"DELETE FROM delta.`$path` " +
                    "WHERE (`part col` = 0 AND id IN (1, 2)) OR " +
                    "(`part col` = 1 AND id IN (6, 7))")
                  .collect()
              }

              assertContainsNativeWriteCommand(
                plans,
                "partitioned column mapping DELETE with existing DVs")
              assertNoNativeDmlRowIndexScan(
                plans,
                "partitioned column mapping DELETE with existing DVs")
              assertContainsPlainFileIndexNativeDmlRowIndexScan(
                plans,
                "partitioned column mapping DELETE with existing DVs")
              assertContainsDeletionVectorRowIndex(
                plans,
                "partitioned column mapping DELETE with existing DVs")
              assertDeleteMetrics(path, numDeletedRows = 2)

              val result = spark.read.format("delta").load(path)
              assert(result.select("id", "value col", "part col").collect().toSet == Set(
                Row(3, "c", 0),
                Row(4, "d", 0),
                Row(5, "e", 1),
                Row(8, "h", 1)))

              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 2)
              assert(removeFiles.size === 2)
              assert(addFiles.map(_.path).toSet === removeFiles.map(_.path).toSet)
              assert(addFiles.map(_.deletionVector.cardinality).sum === 4)
              assert(removeFiles.map(_.deletionVector.cardinality).sum === 2)
              val removeFilesByPath = removeFiles.map(file => file.path -> file).toMap
              addFiles.foreach {
                addFile =>
                  val removeFile = removeFilesByPath(addFile.path)
                  assertDeletionVectorReplacement(
                    addFile,
                    removeFile,
                    initialDvIds.get(addFile.path))
              }
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
          }
        }
      }
    }
  }

  test("native delta delete should use plain Parquet target scan without Delta metadata row index") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf(useMetadataRowIndex = false) {
        withSQLConf(
          NativeDmlRowIndexScanKey -> "true",
          PlainParquetTargetScanKey -> "true") {
          withTempDir {
            dir =>
              val path = dir.getCanonicalPath
              Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"))
                .toDF("id", "value")
                .coalesce(1)
                .write
                .format("delta")
                .save(path)
              enableDeletionVectorsInTable(path)

              withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
                sql(s"DELETE FROM delta.`$path` WHERE id = 5").collect()
              }

              val log = DeltaLog.forTable(spark, new Path(path))
              val initialDvFiles = getFilesWithDeletionVectors(log)
              assert(initialDvFiles.size === 1)
              assert(initialDvFiles.map(_.deletionVector.cardinality).sum === 1)
              val initialDvIds = deletionVectorIdsByPath(initialDvFiles)

              val plans = collectExecutedPlans {
                sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
              }

              assertContainsNativeWriteCommand(
                plans,
                "DELETE with metadata row index disabled")
              assertNoNativeDmlRowIndexScan(plans, "DELETE with metadata row index disabled")
              assertContainsPlainFileIndexNativeDmlRowIndexScan(
                plans,
                "DELETE with metadata row index disabled")
              assertContainsDeletionVectorRowIndex(plans, "DELETE with metadata row index disabled")
              assertDeleteMetrics(path, numDeletedRows = 1)

              val result = spark.read.format("delta").load(path)
              assert(result.collect().toSet == Set(Row(1, "a"), Row(3, "c"), Row(4, "d")))

              val (addFiles, removeFiles) = getFileActionsInLastVersion(log)
              assert(addFiles.size === 1)
              assert(removeFiles.size === 1)
              assert(addFiles.head.path === removeFiles.head.path)
              assert(addFiles.head.deletionVector.cardinality === 2)
              assert(removeFiles.head.deletionVector.cardinality === 1)
              assertDeletionVectorReplacement(
                addFiles.head,
                removeFiles.head,
                initialDvIds.get(removeFiles.head.path))
              assertDeletionVectorsExist(log, addFiles)
              assertDeletionVectorStatsAreWide(addFiles)
          }
        }
      }
    }
  }

  test("native delta scan should read deletion vectors without metadata row index") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf(useMetadataRowIndex = false) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"))
              .toDF("id", "value")
              .coalesce(1)
              .write
              .format("delta")
              .save(path)
            enableDeletionVectorsInTable(path)

            withSQLConf(
              "spark.gluten.enabled" -> "false",
              VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
              sql(s"DELETE FROM delta.`$path` WHERE id IN (4, 5)").collect()
            }

            val plans = collectExecutedPlans {
              val result = spark.read
                .format("delta")
                .load(path)
                .where("id >= 2")
                .collect()
                .toSet
              assert(result == Set(Row(2, "b"), Row(3, "c")))
            }

            assertContainsNativeDeltaScan(
              plans,
              "DV read with metadata row index disabled")
            assertNativeDvRead(plans, "DV read with metadata row index disabled")
        }
      }
    }
  }

  test("native delta scan should read partitioned deletion vectors without metadata row index") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf(useMetadataRowIndex = false) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            Seq(
              (1, "a", 0),
              (2, "b", 0),
              (3, "c", 0),
              (4, "d", 0),
              (5, "e", 1),
              (6, "f", 1),
              (7, "g", 1),
              (8, "h", 1))
              .toDF("id", "value", "part")
              .repartition(2, $"part")
              .write
              .format("delta")
              .partitionBy("part")
              .save(path)
            enableDeletionVectorsInTable(path)

            withSQLConf(
              "spark.gluten.enabled" -> "false",
              VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
              sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 7)").collect()
            }

            val log = DeltaLog.forTable(spark, new Path(path))
            val dvFiles = getFilesWithDeletionVectors(log)
            assert(dvFiles.size === 2)
            assert(dvFiles.map(_.deletionVector.cardinality).sum === 2)

            val plans = collectExecutedPlans {
              val result = spark.read
                .format("delta")
                .load(path)
                .where("id >= 2")
                .select("id", "value", "part")
                .collect()
                .toSet
              assert(result == Set(
                Row(3, "c", 0),
                Row(4, "d", 0),
                Row(5, "e", 1),
                Row(6, "f", 1),
                Row(8, "h", 1)))
            }

            assertNativeDvRead(
              plans,
              "partitioned DV read with metadata row index disabled")
        }
      }
    }
  }

  test("native delta scan should read multiple deletion vector files without metadata row index") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf(useMetadataRowIndex = false) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
              .toDF("id", "value")
              .coalesce(1)
              .write
              .format("delta")
              .save(path)
            enableDeletionVectorsInTable(path)
            Seq((5, "e"), (6, "f"), (7, "g"), (8, "h"))
              .toDF("id", "value")
              .coalesce(1)
              .write
              .format("delta")
              .mode("append")
              .save(path)

            withSQLConf(
              "spark.gluten.enabled" -> "false",
              VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
              sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 6)").collect()
            }

            val log = DeltaLog.forTable(spark, new Path(path))
            val dvFiles = getFilesWithDeletionVectors(log)
            assert(dvFiles.size === 2)
            assert(dvFiles.map(_.deletionVector.cardinality).sum === 2)

            val plans = collectExecutedPlans {
              val result = spark.read
                .format("delta")
                .load(path)
                .where("id >= 2")
                .collect()
                .toSet
              assert(result == Set(
                Row(3, "c"),
                Row(4, "d"),
                Row(5, "e"),
                Row(7, "g"),
                Row(8, "h")))
            }

            assertNativeDvRead(
              plans,
              "multi-file DV read with metadata row index disabled")
        }
      }
    }
  }

  test("native delta scan should read column mapping deletion vectors without metadata row index") {
    withNativeWriteOffloadConf {
      withDeletionVectorDeleteConf(useMetadataRowIndex = false) {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"))
              .toDF("id", "value col")
              .coalesce(1)
              .write
              .format("delta")
              .option(DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")
              .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "true")
              .save(path)

            withSQLConf(
              "spark.gluten.enabled" -> "false",
              VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
              sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 5)").collect()
            }

            val log = DeltaLog.forTable(spark, new Path(path))
            val dvFiles = getFilesWithDeletionVectors(log)
            assert(dvFiles.size === 1)
            assert(dvFiles.map(_.deletionVector.cardinality).sum === 2)

            val plans = collectExecutedPlans {
              val result = spark.read
                .format("delta")
                .load(path)
                .where("id >= 2")
                .select("id", "value col")
                .collect()
                .toSet
              assert(result == Set(Row(3, "c"), Row(4, "d")))
            }

            assertNativeDvRead(
              plans,
              "column mapping DV read with metadata row index disabled")
        }
      }
    }
  }

  test("native delta update command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          Seq((1, "a"), (2, "b")).toDF("id", "value").write.format("delta").save(path)

          val updateDf = sql(s"UPDATE delta.`$path` SET value = 'bb' WHERE id = 2")
          assertContainsNativeWriteCommand(Seq(updateDf.queryExecution.executedPlan), "UPDATE")
          updateDf.collect()

          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(1, "a"), Row(2, "bb")))
      }
    }
  }

  test("native delta CTAS command should be offloaded") {
    withNativeWriteOffloadConf {
      withTable("delta_native_write_ctas") {
        val ctasDf = sql(
          "CREATE TABLE delta_native_write_ctas USING delta AS " +
            "SELECT id, concat('v', cast(id as string)) AS value FROM range(1, 4)")
        assertContainsNativeWriteCommand(Seq(ctasDf.queryExecution.executedPlan), "CTAS")
        ctasDf.collect()

        val result = sql("SELECT * FROM delta_native_write_ctas ORDER BY id")
        assert(result.collect().toSeq == Seq(Row(1L, "v1"), Row(2L, "v2"), Row(3L, "v3")))
      }
    }
  }

  test("native delta RTAS command should be offloaded") {
    withNativeWriteOffloadConf {
      withTable("delta_native_write_rtas") {
        sql(
          "CREATE TABLE delta_native_write_rtas USING delta AS " +
            "SELECT id, concat('v', cast(id as string)) AS value FROM range(1, 4)")
          .collect()

        val rtasDf = sql(
          "REPLACE TABLE delta_native_write_rtas USING delta AS " +
            "SELECT id, concat('r', cast(id as string)) AS value FROM range(2, 5)")
        assertContainsNativeWriteCommand(Seq(rtasDf.queryExecution.executedPlan), "RTAS")
        rtasDf.collect()

        val result = sql("SELECT * FROM delta_native_write_rtas ORDER BY id")
        assert(result.collect().toSeq == Seq(Row(2L, "r2"), Row(3L, "r3"), Row(4L, "r4")))
      }
    }
  }

  test("native delta save command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val plans = collectExecutedPlans {
            Seq((1, "a"), (2, "b"))
              .toDF("id", "value")
              .write
              .format("delta")
              .mode("overwrite")
              .save(path)
          }

          assertContainsNativeWriteCommand(plans, "DataFrameWriter.save(overwrite)")
          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(1, "a"), Row(2, "b")))
      }
    }
  }

  test("native delta append save command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          Seq((1, "a")).toDF("id", "value").write.format("delta").mode("overwrite").save(path)

          val plans = collectExecutedPlans {
            Seq((2, "b"), (3, "c"))
              .toDF("id", "value")
              .write
              .format("delta")
              .mode("append")
              .save(path)
          }

          assertContainsNativeWriteCommand(plans, "DataFrameWriter.save(append)")
          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(1, "a"), Row(2, "b"), Row(3, "c")))
      }
    }
  }

  test("native delta partitioned save command should be offloaded") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val plans = collectExecutedPlans {
            Seq((1, "a", 0), (2, "b", 1))
              .toDF("id", "value", "part")
              .write
              .format("delta")
              .partitionBy("part")
              .mode("overwrite")
              .save(path)
          }

          assertContainsNativeWriteCommand(plans, "partitioned DataFrameWriter.save(overwrite)")
          val result = spark.read.format("delta").load(path)
          assert(
            result.select("id", "value", "part").collect().toSet == Set(
              Row(1, "a", 0),
              Row(2, "b", 1)))
      }
    }
  }

  test("delta save command should not be offloaded when native write is disabled") {
    withNativeWriteOffloadConf {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val plans = withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
            collectExecutedPlans {
              Seq((1, "a"), (2, "b"))
                .toDF("id", "value")
                .write
                .format("delta")
                .mode("overwrite")
                .save(path)
            }
          }

          assertNoNativeWriteCommand(
            plans,
            "DataFrameWriter.save(overwrite) with native write disabled")
          val result = spark.read.format("delta").load(path)
          assert(result.collect().toSet == Set(Row(1, "a"), Row(2, "b")))
      }
    }
  }
}
