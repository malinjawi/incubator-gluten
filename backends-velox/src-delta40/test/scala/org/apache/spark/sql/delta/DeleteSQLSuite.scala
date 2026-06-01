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

import org.apache.gluten.config.VeloxDeltaConfig

import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaExcludedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.tags.ExtendedSQLTest

// spotless:off
@ExtendedSQLTest
class DeleteSQLSuite extends DeleteSuiteBase
  with DeltaExcludedTestMixin
  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def executeDelete(target: String, where: String = null): Unit = {
    val whereClause = Option(where).map(c => s"WHERE $c").getOrElse("")
    sql(s"DELETE FROM $target $whereClause")
  }

  override def excluded: Seq[String] = super.excluded ++
    Seq(
      // FIXME: Excluded by Gluten as results are mismatch.
      "test delete on temp view - nontrivial projection - SQL TempView",
      "test delete on temp view - nontrivial projection - Dataset TempView",
      // FIXME: Different error messages.
      "test delete on temp view - superset cols - SQL TempView",
      "test delete on temp view - superset cols - Dataset TempView"
    )

  // For EXPLAIN, which is not supported in OSS
  test("explain") {
    append(Seq((2, 2)).toDF("key", "value"))
    val df = sql(s"EXPLAIN DELETE FROM delta.`$tempPath` WHERE key = 2")
    val outputs = df.collect().map(_.mkString).mkString
    assert(outputs.contains("Delta"))
    assert(!outputs.contains("index") && !outputs.contains("ActionLog"))
    // no change should be made by explain
    checkAnswer(readDeltaTable(tempPath), Row(2, 2))
  }

  test("delete from a temp view") {
    withTable("tab") {
      withTempView("v") {
        Seq((1, 1), (0, 3), (1, 5)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        spark.table("tab").as("name").createTempView("v")
        sql("DELETE FROM v WHERE key = 1")
        checkAnswer(spark.table("tab"), Row(0, 3))
      }
    }
  }

  test("delete from a SQL temp view") {
    withTable("tab") {
      withTempView("v") {
        Seq((1, 1), (0, 3), (1, 5)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        sql("CREATE TEMP VIEW v AS SELECT * FROM tab")
        sql("DELETE FROM v WHERE key = 1 AND VALUE = 5")
        checkAnswer(spark.table("tab"), Seq(Row(1, 1), Row(0, 3)))
      }
    }
  }

  Seq(true, false).foreach { partitioned =>
    test(s"User defined _change_type column doesn't get dropped - partitioned=$partitioned") {
      withTable("tab") {
        sql(
          s"""CREATE TABLE tab USING DELTA
             |${if (partitioned) "PARTITIONED BY (part) " else ""}
             |TBLPROPERTIES (delta.enableChangeDataFeed = false)
             |AS SELECT id, int(id / 10) AS part, 'foo' as _change_type
             |FROM RANGE(1000)
             |""".stripMargin)
        val rowsToDelete = (1 to 1000 by 42).mkString("(", ", ", ")")
        executeDelete("tab", s"id in $rowsToDelete")
        sql("SELECT id, _change_type FROM tab").collect().foreach { row =>
          val _change_type = row.getString(1)
          assert(_change_type === "foo", s"Invalid _change_type for id=${row.get(0)}")
        }
      }
    }
  }
}

@ExtendedSQLTest
class DeleteSQLNameColumnMappingSuite extends DeleteSQLSuite
  with DeltaColumnMappingEnableNameMode {

  protected override def runOnlyTests: Seq[String] = Seq(true, false).map { isPartitioned =>
    s"basic case - delete from a Delta table by name - Partition=$isPartitioned"
  } ++ Seq(true, false).flatMap { isPartitioned =>
    Seq(
      s"where key columns - Partition=$isPartitioned",
      s"where data columns - Partition=$isPartitioned")
  }

}

@ExtendedSQLTest
class DeleteSQLWithDeletionVectorsSuite extends DeleteSQLSuite
  with DeltaExcludedTestMixin
  with DeletionVectorsTestUtils {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark, delete = true)
    spark.conf.set(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key, "false")
  }

  override def excluded: Seq[String] = super.excluded ++
    Seq(
      // The following two tests must fail when DV is used. Covered by another test case:
      // "throw error when non-pinned TahoeFileIndex snapshot is used".
      "data and partition columns - Partition=true Skipping=false",
      "data and partition columns - Partition=false Skipping=false",
      // The scan schema contains additional row index filter columns.
      "nested schema pruning on data condition",
      // The number of records is not recomputed when using DVs
      "delete throws error if number of records increases",
      "delete logs error if number of records are missing in stats",
      // FIXME: Excluded by Gluten as results are mismatch.
      "test delete on temp view - nontrivial projection - SQL TempView",
      "test delete on temp view - nontrivial projection - Dataset TempView"
  )

  // This works correctly with DVs, but fails in classic DELETE.
  override def testSuperSetColsTempView(): Unit = {
    testComplexTempViews("superset cols")(
      text = "SELECT key, value, 1 FROM tab",
      expectResult = Row(0, 3, 1) :: Nil)
  }

  private val deleteMetricKeys = Seq(
    "numDeletedRows",
    "numRemovedFiles",
    "numDeletionVectorsAdded",
    "numDeletionVectorsUpdated",
    "numDeletionVectorsRemoved")

  private case class LatestDeleteActionSummary(
      addFiles: Int,
      removeFiles: Int,
      addFilesWithDvs: Int,
      addedDvCardinality: Long,
      addFilesWithStats: Int,
      addFilesWithPhysicalRecords: Int,
      metrics: Map[String, Long])

  private case class RepeatedDeleteScenarioResult(
      finalRows: Seq[Long],
      finalPartitions: Seq[Int],
      activeDvFiles: Int,
      activeDvCardinality: Long,
      actionSummaries: Seq[LatestDeleteActionSummary])

  private def latestDeleteMetrics(path: String): Map[String, Long] = {
    val metrics = io.delta.tables.DeltaTable
      .forPath(path)
      .history()
      .select("operationMetrics")
      .take(1)
      .head
      .getMap(0)
      .asInstanceOf[Map[String, String]]
      .map { case (key, value) => key -> value.toLong }
    deleteMetricKeys.map(key => key -> metrics.getOrElse(key, 0L)).toMap
  }

  private def latestActionSummary(log: DeltaLog, path: String): LatestDeleteActionSummary = {
    val (adds, removes) = getFileActionsInLastVersion(log)
    LatestDeleteActionSummary(
      addFiles = adds.size,
      removeFiles = removes.size,
      addFilesWithDvs = adds.count(_.deletionVector != null),
      addedDvCardinality = adds.flatMap(add => Option(add.deletionVector).map(_.cardinality)).sum,
      addFilesWithStats = adds.count(add => Option(add.stats).exists(_.nonEmpty)),
      addFilesWithPhysicalRecords = adds.count(_.numPhysicalRecords.isDefined),
      metrics = latestDeleteMetrics(path))
  }

  private def runRepeatedDeleteScenario(nativeWriteEnabled: Boolean): RepeatedDeleteScenarioResult = {
    var result = Option.empty[RepeatedDeleteScenarioResult]
    withTempDir { (dir: java.io.File) =>
      val path = dir.getCanonicalPath
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
      val summaries = Seq.newBuilder[LatestDeleteActionSummary]

      def rows: Seq[Long] =
        sql(s"SELECT id FROM delta.`$path` ORDER BY id").collect().map(_.getLong(0)).toSeq

      def partitions: Seq[Int] =
        sql(s"SELECT DISTINCT part FROM delta.`$path` ORDER BY part")
          .collect()
          .map(_.getInt(0))
          .toSeq

      def assertRows(expected: Long*): Unit = {
        checkAnswer(sql(s"SELECT id FROM delta.`$path` ORDER BY id"), expected.map(Row(_)))
      }

      def activeDeletionVectorSummary(): (Int, Long) = {
        val filesWithDVs = getFilesWithDeletionVectors(log)
        (filesWithDVs.size, filesWithDVs.map(_.deletionVector.cardinality).sum)
      }

      withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> nativeWriteEnabled.toString) {
        executeDelete(s"delta.`$path`", "id IN (0, 2, 6)")
        assertRows(1, 3, 4, 5, 7, 8, 9, 10, 11)
        assert(activeDeletionVectorSummary() === ((2, 3L)))
        summaries += latestActionSummary(log, path)

        executeDelete(s"delta.`$path`", "id IN (2, 3, 6, 7, 8)")
        assertRows(1, 4, 5, 9, 10, 11)
        assert(activeDeletionVectorSummary() === ((2, 6L)))
        summaries += latestActionSummary(log, path)

        executeDelete(s"delta.`$path`", "id IN (1, 4, 5)")
        assertRows(9, 10, 11)
        assert(partitions === Seq(1))
        assert(activeDeletionVectorSummary() === ((1, 3L)))
        summaries += latestActionSummary(log, path)
      }

      val (activeDvFiles, activeDvCardinality) = activeDeletionVectorSummary()
      result = Some(RepeatedDeleteScenarioResult(
        finalRows = rows,
        finalPartitions = partitions,
        activeDvFiles = activeDvFiles,
        activeDvCardinality = activeDvCardinality,
        actionSummaries = summaries.result()))
    }
    result.get
  }

  test("persistent DV DELETE action shape matches native write disabled path") {
    val sparkResult = runRepeatedDeleteScenario(nativeWriteEnabled = false)
    val nativeResult = runRepeatedDeleteScenario(nativeWriteEnabled = true)
    assert(nativeResult === sparkResult)
    assert(
      nativeResult.actionSummaries === Seq(
        LatestDeleteActionSummary(
          addFiles = 2,
          removeFiles = 2,
          addFilesWithDvs = 2,
          addedDvCardinality = 3,
          addFilesWithStats = 2,
          addFilesWithPhysicalRecords = 2,
          metrics = Map(
            "numDeletedRows" -> 3L,
            "numRemovedFiles" -> 0L,
            "numDeletionVectorsAdded" -> 2L,
            "numDeletionVectorsUpdated" -> 0L,
            "numDeletionVectorsRemoved" -> 0L)),
        LatestDeleteActionSummary(
          addFiles = 2,
          removeFiles = 2,
          addFilesWithDvs = 2,
          addedDvCardinality = 6,
          addFilesWithStats = 2,
          addFilesWithPhysicalRecords = 2,
          metrics = Map(
            "numDeletedRows" -> 3L,
            "numRemovedFiles" -> 0L,
            "numDeletionVectorsAdded" -> 0L,
            "numDeletionVectorsUpdated" -> 2L,
            "numDeletionVectorsRemoved" -> 0L)),
        LatestDeleteActionSummary(
          addFiles = 0,
          removeFiles = 1,
          addFilesWithDvs = 0,
          addedDvCardinality = 0,
          addFilesWithStats = 0,
          addFilesWithPhysicalRecords = 0,
          metrics = Map(
            "numDeletedRows" -> 3L,
            "numRemovedFiles" -> 1L,
            "numDeletionVectorsAdded" -> 0L,
            "numDeletionVectorsUpdated" -> 0L,
            "numDeletionVectorsRemoved" -> 1L))))
  }

  test("repeated DELETE produces, updates, and removes persistent deletion vectors") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 10, 1, numPartitions = 1).toDF("id").write.format("delta").save(path)
      val log = DeltaLog.forTable(spark, path)

      def assertRows(expected: Long*): Unit = {
        checkAnswer(
          sql(s"SELECT id FROM delta.`$path` ORDER BY id"),
          expected.map(id => Row(id)))
      }

      def assertActiveDeletionVectors(expectedFiles: Int, expectedCardinality: Long): Unit = {
        val filesWithDVs = getFilesWithDeletionVectors(log)
        assert(filesWithDVs.size === expectedFiles)
        assert(filesWithDVs.map(_.deletionVector.cardinality).sum === expectedCardinality)
      }

      def assertDeleteMetrics(expected: (String, Long)*): Unit = {
        val metrics = io.delta.tables.DeltaTable
          .forPath(path)
          .history()
          .select("operationMetrics")
          .take(1)
          .head
          .getMap(0)
          .asInstanceOf[Map[String, String]]
          .map { case (key, value) => key -> value.toLong }
        expected.foreach { case (key, value) =>
          assert(metrics.getOrElse(key, -1L) === value, s"Unexpected metric $key: $metrics")
        }
      }

      def assertDeleteMetricAtLeast(key: String, expected: Long): Unit = {
        val metrics = io.delta.tables.DeltaTable
          .forPath(path)
          .history()
          .select("operationMetrics")
          .take(1)
          .head
          .getMap(0)
          .asInstanceOf[Map[String, String]]
          .map { case (metricKey, value) => metricKey -> value.toLong }
        assert(metrics.getOrElse(key, -1L) >= expected, s"Unexpected metric $key: $metrics")
      }

      executeDelete(s"delta.`$path`", "id % 3 = 0")
      assertRows(1, 2, 4, 5, 7, 8)
      assertActiveDeletionVectors(expectedFiles = 1, expectedCardinality = 4)
      assertDeleteMetrics(
        "numDeletedRows" -> 4L,
        "numDeletionVectorsAdded" -> 1L,
        "numDeletionVectorsUpdated" -> 0L,
        "numDeletionVectorsRemoved" -> 0L)

      executeDelete(s"delta.`$path`", "id IN (4, 5, 7)")
      assertRows(1, 2, 8)
      assertActiveDeletionVectors(expectedFiles = 1, expectedCardinality = 7)
      assertDeleteMetrics(
        "numDeletedRows" -> 3L,
        "numDeletionVectorsUpdated" -> 1L)

      executeDelete(s"delta.`$path`", "id IN (1, 2, 8)")
      assertRows()
      assertActiveDeletionVectors(expectedFiles = 0, expectedCardinality = 0)
      assertDeleteMetrics(
        "numDeletedRows" -> 3L,
        "numRemovedFiles" -> 1L)
      assertDeleteMetricAtLeast("numDeletionVectorsRemoved", 1L)
    }
  }
}

@ExtendedSQLTest
class DeleteSQLWithDeletionVectorsAndPredicatePushdownSuite
    extends DeleteSQLWithDeletionVectorsSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key, "true")
  }
}
// spotless:on
