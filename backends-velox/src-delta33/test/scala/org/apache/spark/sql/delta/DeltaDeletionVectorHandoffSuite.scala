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
import org.apache.gluten.execution.{DeltaScanTransformer, FilterExecTransformerBase, HashAggregateExecTransformer, ProjectExecTransformerBase}
import org.apache.gluten.extension.DeltaDeletionVectorDmlUtils
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.commands.GlutenDeleteCommand
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.GlutenDeltaLeafRunnableCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.SparkVersionUtil

import org.apache.hadoop.fs.Path

import java.io.File
import java.util.Locale

@ExtendedSQLTest
class DeltaDeletionVectorHandoffSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with DeletionVectorsTestUtils {

  import testImplicits._

  private val DmlFallbackReason = "fallback Delta DV DML row-index scan"
  private val DmlRowIndexColumnNames =
    Seq("__delta_internal_row_index", "_tmp_metadata_row_index", "rowIndexCol")
  private val EnableNativeDmlRowIndexScan =
    "spark.gluten.sql.delta.enableNativeDmlRowIndexScan"

  private lazy val isMac = sys.props
    .get("os.name")
    .exists(_.toLowerCase(Locale.ROOT).contains("mac"))

  private def containsDmlFallbackScan(plan: SparkPlan): Boolean = {
    plan.exists {
      case scan: FileSourceScanExec =>
        DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(scan) &&
        FallbackTags.getOption(scan).exists(_.reason().contains(DmlFallbackReason))
      case _ => false
    }
  }

  private def hasSparkParentOverDmlFallbackScan(plan: SparkPlan): Boolean = {
    plan.exists {
      case ProjectExec(_, child) if containsDmlFallbackScan(child) => true
      case FilterExec(_, child) if containsDmlFallbackScan(child) => true
      case _ => false
    }
  }

  private def hasNativeParentOverDmlFallbackScan(plan: SparkPlan): Boolean = {
    plan.exists {
      case project: ProjectExecTransformerBase if containsDmlFallbackScan(project.child) => true
      case filter: FilterExecTransformerBase if containsDmlFallbackScan(filter.child) => true
      case _ => false
    }
  }

  private def containsDmlRowIndexTargetScanText(plan: SparkPlan): Boolean = {
    val planText = plan.treeString
    planText.contains("FileScan parquet") &&
    planText.contains("file_path") &&
    DmlRowIndexColumnNames.exists(planText.contains) &&
    (planText.contains("TahoeBatchFileIndex") || planText.contains("PreparedDeltaFileIndex"))
  }

  private def captureDeletePlans(
      path: String,
      predicate: String,
      useMetadataRowIndex: Boolean): Seq[SparkPlan] = {
    var executedPlans: Seq[SparkPlan] = Seq.empty
    withSQLConf(
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key ->
        useMetadataRowIndex.toString,
      "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite" -> "false",
      "spark.gluten.sql.delta.enableNativeDmlRowIndexScan" -> "false"
    ) {
      executedPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
        spark.sql(s"DELETE FROM delta.`$path` WHERE $predicate").collect()
      }.map(_.executedPlan)
    }
    executedPlans
  }

  private def assertSparkDmlFallback(executedPlans: Seq[SparkPlan]): Unit = {
    val planText = executedPlans.map(_.treeString).mkString("\n\n")
    if (executedPlans.exists(containsDmlFallbackScan)) {
      assert(executedPlans.exists(hasSparkParentOverDmlFallbackScan), planText)
      assert(!executedPlans.exists(hasNativeParentOverDmlFallbackScan), planText)
    } else {
      assert(executedPlans.exists(containsDmlRowIndexTargetScanText), planText)
    }
  }

  private def assertReadPlanAfterDmlFallback(path: String, useMetadataRowIndex: Boolean): Unit = {
    withSQLConf(
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> useMetadataRowIndex.toString) {
      val df = spark.read.format("delta").load(path)
      val executedPlan = df.queryExecution.executedPlan
      val planText = executedPlan.treeString
      if (useMetadataRowIndex) {
        assert(executedPlan.collect { case _: DeltaScanTransformer => true }.nonEmpty, planText)
        assert(!planText.contains(DmlFallbackReason), planText)
      } else {
        assert(executedPlan.collect { case _: DeltaScanTransformer => true }.isEmpty, planText)
      }
      checkAnswer(df, Seq((1, "a"), (2, "b")).toDF())
    }
  }

  private def activeDvCardinality(path: String): Long = {
    val log = DeltaLog.forTable(spark, new Path(path))
    log.update().allFiles.collect().flatMap(
      file => Option(file.deletionVector).map(_.cardinality)).sum
  }

  private def captureNativeDeletePlans(path: String, predicate: String): Seq[SparkPlan] = {
    val confs = Seq(
      SQLConf.ANSI_ENABLED.key -> "false",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false",
      DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false",
      VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "true",
      EnableNativeDmlRowIndexScan -> "true",
      DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> "true",
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true"
    ) ++
      (if (isMac) {
         Seq(GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> "false")
       } else {
         Seq.empty
       })

    var executedPlans: Seq[SparkPlan] = Seq.empty
    withSQLConf(confs: _*) {
      executedPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
        spark.sql(s"DELETE FROM delta.`$path` WHERE $predicate").collect()
      }.map(_.executedPlan)
    }
    executedPlans
  }

  private def assertNativeBitmapDeletePlans(plans: Seq[SparkPlan], context: String): Unit = {
    val planText = plans.map(_.treeString).mkString("\n---\n")
    assert(plans.exists(hasGlutenDeleteCommand), s"$context\n$planText")
    assert(plans.exists(hasDeltaScanTransformer), s"$context\n$planText")
    assert(plans.exists(hasNativeBitmapAggregate), s"$context\n$planText")
    assert(!plans.exists(hasSparkBitmapAggregate), s"$context\n$planText")
    assert(!plans.exists(containsDmlFallbackScan), s"$context\n$planText")
  }

  private def assertNativeDeletePlans(plans: Seq[SparkPlan], context: String): Unit = {
    val planText = plans.map(_.treeString).mkString("\n---\n")
    assert(plans.exists(hasGlutenDeleteCommand), s"$context\n$planText")
    assert(plans.exists(hasDeltaScanTransformer), s"$context\n$planText")
    assert(!plans.exists(containsDmlFallbackScan), s"$context\n$planText")
  }

  private def hasGlutenDeleteCommand(plan: SparkPlan): Boolean = {
    val commandClassMatch = plan
      .collectFirst {
        case ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(_: GlutenDeleteCommand)) => true
      }
      .getOrElse(false)
    commandClassMatch ||
    plan.exists(_.nodeName.contains("GlutenDeleteCommand")) ||
    plan.treeString.contains("GlutenDeleteCommand")
  }

  private def hasDeltaScanTransformer(plan: SparkPlan): Boolean =
    plan.collect { case _: DeltaScanTransformer => true }.nonEmpty ||
      plan.treeString.contains("FileDeltaScanTransformer")

  private def hasNativeBitmapAggregate(plan: SparkPlan): Boolean = {
    val planText = plan.treeString.toLowerCase(Locale.ROOT)
    containsBitmapAggregator(planText) &&
    (plan.collect { case _: HashAggregateExecTransformer => true }.nonEmpty ||
      planText.contains("hashaggregatetransformer"))
  }

  private def hasSparkBitmapAggregate(plan: SparkPlan): Boolean = {
    val planText = plan.treeString.toLowerCase(Locale.ROOT)
    containsBitmapAggregator(planText) && !planText.contains("hashaggregatetransformer")
  }

  private def containsBitmapAggregator(plan: SparkPlan): Boolean = {
    val planText = plan.treeString.toLowerCase(Locale.ROOT)
    containsBitmapAggregator(planText)
  }

  private def containsBitmapAggregator(planText: String): Boolean =
    planText.contains("bitmapaggregator") || planText.contains("bitmap_aggregator")

  private def assertDeleteMetrics(path: String, expected: (String, Long)*): Unit = {
    val metrics = io.delta.tables.DeltaTable
      .forPath(path)
      .history()
      .select("operationMetrics")
      .take(1)
      .head
      .getMap(0)
      .asInstanceOf[Map[String, String]]
      .map { case (key, value) => key -> value.toLong }
    expected.foreach {
      case (key, value) =>
        assert(metrics.getOrElse(key, -1L) === value, s"Unexpected metric $key: $metrics")
    }
  }

  test("Spark 3.5 Delta DV scan handoff should filter deleted rows") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)")

        val log = DeltaLog.forTable(spark, new Path(path))
        val addFileWithDv = log.update().allFiles.collect().find(_.deletionVector != null)
        assert(addFileWithDv.nonEmpty)

        val dataFile = addFileWithDv.get
        assert(dataFile.deletionVector.cardinality == 2L)

        val df = spark.read.format("delta").load(path)
        val executedPlan = df.queryExecution.executedPlan
        assert(executedPlan.collect { case _: DeltaScanTransformer => true }.nonEmpty)
        val planText = executedPlan.toString()
        assert(!planText.contains("__delta_internal_is_row_deleted"))
        assert(!planText.contains("__delta_internal_row_index"))
        checkAnswer(df, Seq((1, "a"), (2, "b")).toDF())
    }
  }

  Seq(true, false).foreach {
    useMetadataRowIndex =>
      test(
        "Delta DV DML row-index scan should fall back with Spark project/filter, " +
          s"metadata row index=$useMetadataRowIndex") {
        assume(SparkVersionUtil.gteSpark35, "DML row-index scan fallback is Spark 3.5+ coverage")
        withTempDir {
          tempDir =>
            val path = tempDir.getCanonicalPath
            Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
              .toDF("id", "value")
              .coalesce(1)
              .write
              .format("delta")
              .save(path)

            spark.sql(
              s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
                "('delta.enableDeletionVectors' = true)")

            var executedPlans: Seq[SparkPlan] = Seq.empty
            withSQLConf(
              DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key ->
                useMetadataRowIndex.toString,
              "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite" -> "false",
              "spark.gluten.sql.delta.enableNativeDmlRowIndexScan" -> "false"
            ) {
              executedPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
                spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)").collect()
              }.map(_.executedPlan)
            }
            assertSparkDmlFallback(executedPlans)

            val log = DeltaLog.forTable(spark, new Path(path))
            assert(log.update().allFiles.collect().exists(_.deletionVector != null))
            assertReadPlanAfterDmlFallback(path, useMetadataRowIndex)
        }
      }
  }

  test("Delta DV DML row-index scan should fall back when updating an existing DV") {
    assume(SparkVersionUtil.gteSpark35, "DML row-index scan fallback is Spark 3.5+ coverage")
    withTempDir {
      tempDir =>
        val path = new File(tempDir, "delta table with spaces").getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
            "('delta.enableDeletionVectors' = true)")

        assertSparkDmlFallback(captureDeletePlans(path, "id IN (5, 6)", useMetadataRowIndex = true))
        assert(activeDvCardinality(path) === 2L)

        assertSparkDmlFallback(captureDeletePlans(path, "id IN (3, 4)", useMetadataRowIndex = true))
        assert(activeDvCardinality(path) === 4L)

        assertReadPlanAfterDmlFallback(path, useMetadataRowIndex = true)
    }
  }

  test("Delta DELETE DV native bitmap construction should create and update persistent DVs") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        spark.range(0, 10, 1, numPartitions = 1).toDF("id").write.format("delta").save(path)
        val log = DeltaLog.forTable(spark, path)

        def assertRows(expected: Long*): Unit = {
          checkAnswer(
            spark.sql(s"SELECT id FROM delta.`$path` ORDER BY id"),
            expected.map(id => Row(id)))
        }

        def assertActiveDeletionVectors(expectedFiles: Int, expectedCardinality: Long): Unit = {
          val filesWithDVs = getFilesWithDeletionVectors(log)
          assert(filesWithDVs.size === expectedFiles)
          assert(filesWithDVs.map(_.deletionVector.cardinality).sum === expectedCardinality)
          assertDeletionVectorsExist(log, filesWithDVs)
        }

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
            "('delta.enableDeletionVectors' = true)")

        val createPlans = captureNativeDeletePlans(path, "id % 3 = 0")
        assertRows(1, 2, 4, 5, 7, 8)
        assertActiveDeletionVectors(expectedFiles = 1, expectedCardinality = 4)
        assertDeleteMetrics(
          path,
          "numDeletedRows" -> 4L,
          "numDeletionVectorsAdded" -> 1L,
          "numDeletionVectorsUpdated" -> 0L,
          "numDeletionVectorsRemoved" -> 0L)
        assertNativeBitmapDeletePlans(createPlans, "create-DV DELETE")

        val updatePlans = captureNativeDeletePlans(path, "id IN (0, 4, 5, 7)")
        assertRows(1, 2, 8)
        assertActiveDeletionVectors(expectedFiles = 1, expectedCardinality = 7)
        assertDeleteMetrics(
          path,
          "numDeletedRows" -> 3L,
          "numDeletionVectorsAdded" -> 0L,
          "numDeletionVectorsUpdated" -> 1L,
          "numDeletionVectorsRemoved" -> 0L)
        assertNativeBitmapDeletePlans(updatePlans, "update-existing-DV DELETE")
    }
  }

  test("Delta DELETE DV native write should update and remove partitioned DVs") {
    withTempDir {
      tempDir =>
        val path = new File(tempDir, "partitioned delta table with spaces").getCanonicalPath
        spark.range(0, 6, 1, numPartitions = 1)
          .selectExpr("id", "cast(0 as int) as part")
          .write
          .format("delta")
          .partitionBy("part")
          .save(path)
        spark.range(6, 12, 1, numPartitions = 1)
          .selectExpr("id", "cast(1 as int) as part")
          .write
          .format("delta")
          .partitionBy("part")
          .mode("append")
          .save(path)
        val log = DeltaLog.forTable(spark, path)

        def assertRows(expected: Long*): Unit = {
          checkAnswer(
            spark.sql(s"SELECT id FROM delta.`$path` ORDER BY id"),
            expected.map(id => Row(id)))
        }

        def assertPartitions(expected: Int*): Unit = {
          checkAnswer(
            spark.sql(s"SELECT DISTINCT part FROM delta.`$path` ORDER BY part"),
            expected.map(part => Row(part)))
        }

        def assertActiveDeletionVectors(expectedFiles: Int, expectedCardinality: Long): Unit = {
          val filesWithDVs = getFilesWithDeletionVectors(log)
          assert(filesWithDVs.size === expectedFiles)
          assert(filesWithDVs.map(_.deletionVector.cardinality).sum === expectedCardinality)
          assertDeletionVectorsExist(log, filesWithDVs)
        }

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
            "('delta.enableDeletionVectors' = true)")

        val createPlans = captureNativeDeletePlans(path, "id IN (0, 2, 6)")
        assertRows(1, 3, 4, 5, 7, 8, 9, 10, 11)
        assertPartitions(0, 1)
        assertActiveDeletionVectors(expectedFiles = 2, expectedCardinality = 3)
        assertDeleteMetrics(
          path,
          "numDeletedRows" -> 3L,
          "numRemovedFiles" -> 0L,
          "numDeletionVectorsAdded" -> 2L,
          "numDeletionVectorsUpdated" -> 0L,
          "numDeletionVectorsRemoved" -> 0L)
        assertNativeBitmapDeletePlans(createPlans, "partitioned create-DV DELETE")

        val updatePlans = captureNativeDeletePlans(path, "id IN (3, 7, 8)")
        assertRows(1, 4, 5, 9, 10, 11)
        assertPartitions(0, 1)
        assertActiveDeletionVectors(expectedFiles = 2, expectedCardinality = 6)
        assertDeleteMetrics(
          path,
          "numDeletedRows" -> 3L,
          "numRemovedFiles" -> 0L,
          "numDeletionVectorsUpdated" -> 2L)
        assertNativeBitmapDeletePlans(updatePlans, "partitioned update-existing-DV DELETE")

        val removePlans = captureNativeDeletePlans(path, "id IN (1, 4, 5)")
        assertRows(9, 10, 11)
        assertPartitions(1)
        assertActiveDeletionVectors(expectedFiles = 1, expectedCardinality = 3)
        assertDeleteMetrics(
          path,
          "numDeletedRows" -> 3L,
          "numRemovedFiles" -> 1L,
          "numDeletionVectorsAdded" -> 0L,
          "numDeletionVectorsUpdated" -> 0L,
          "numDeletionVectorsRemoved" -> 1L)
        assertNativeDeletePlans(removePlans, "partitioned full-file DELETE")
    }
  }
}
