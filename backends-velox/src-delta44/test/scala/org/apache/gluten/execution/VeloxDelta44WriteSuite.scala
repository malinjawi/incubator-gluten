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

import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaLog}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.types.{NullType, StructType}

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.json4s.{JInt, JNothing}
import org.json4s.jackson.JsonMethods.parse

import scala.jdk.CollectionConverters._

/** Gluten's native Delta write must produce the files vanilla Delta 4.4 writes, or fall back. */
class VeloxDelta44WriteSuite extends DeltaSQLCommandTest {

  private def footers(path: String): Seq[(AddFile, ParquetMetadata)] = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    deltaLog.update().allFiles.collect().toSeq.map {
      file =>
        val reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(DeltaFileOperations.absolutePath(path, file.path), hadoopConf))
        try {
          file -> reader.getFooter
        } finally {
          reader.close()
        }
    }
  }

  private def fieldNames(footer: ParquetMetadata): Seq[String] =
    footer.getFileMetaData.getSchema.getFields.asScala.map(_.getName).toSeq

  // Vanilla Spark writes with parquet-mr; the Velox writer does not.
  private def writtenByVelox(footer: ParquetMetadata): Boolean =
    !footer.getFileMetaData.getCreatedBy.startsWith("parquet-mr")

  private def createPartitionedTable(path: String, properties: (String, String)*): Unit = {
    val tblProperties = if (properties.isEmpty) {
      ""
    } else {
      properties.map { case (k, v) => s"'$k' = '$v'" }.mkString(" TBLPROPERTIES (", ", ", ")")
    }
    sql(
      s"CREATE TABLE delta.`$path` (id BIGINT, value STRING, part INT) USING delta " +
        s"PARTITIONED BY (part)$tblProperties")
  }

  private def writeRows(path: String): Unit = {
    spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) AS value", "CAST(id % 2 AS INT) AS part")
      .coalesce(1)
      .write
      .format("delta")
      .mode("append")
      .save(path)
  }

  private def assertRows(path: String): Unit = {
    assert(
      spark.read.format("delta").load(path).collect().toSet ==
        (0 until 10).map(i => Row(i.toLong, i.toString, i % 2)).toSet)
  }

  test("native partitioned write stores the partition columns after the data columns") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createPartitionedTable(path)
        writeRows(path)

        val files = footers(path)
        assert(files.size == 2)
        files.foreach {
          case (file, footer) =>
            assert(writtenByVelox(footer), file.path)
            assert(fieldNames(footer) == Seq("id", "value", "part"), file.path)
            // Statistics cover the data columns only and match the rows of the file.
            val part = file.partitionValues("part").toInt
            val stats = parse(file.stats)
            assert(stats \ "numRecords" == JInt(5), file.stats)
            assert(stats \ "minValues" \ "id" == JInt(part), file.stats)
            assert(stats \ "maxValues" \ "id" == JInt(8 + part), file.stats)
            assert(stats \ "minValues" \ "part" == JNothing, file.stats)
        }
        assertRows(path)
    }
  }

  test("native partitioned write leaves the partition columns out when the table says so") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createPartitionedTable(path, "delta.writePartitionColumnsToParquet" -> "false")
        writeRows(path)

        footers(path).foreach {
          case (file, footer) =>
            assert(writtenByVelox(footer), file.path)
            assert(fieldNames(footer) == Seq("id", "value"), file.path)
        }
        assertRows(path)
    }
  }

  test("materializePartitionColumns keeps the partition columns in native writes") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createPartitionedTable(
          path,
          "delta.writePartitionColumnsToParquet" -> "false",
          "delta.enableMaterializePartitionColumnsFeature" -> "true")
        writeRows(path)

        footers(path).foreach {
          case (file, footer) =>
            assert(writtenByVelox(footer), file.path)
            assert(fieldNames(footer) == Seq("id", "value", "part"), file.path)
        }
        assertRows(path)
    }
  }

  test("Parquet format version 2.12.0 is written by the vanilla writer") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createPartitionedTable(path, "delta.parquet.format.version" -> "2.12.0")
        writeRows(path)

        footers(path).foreach {
          case (file, footer) => assert(!writtenByVelox(footer), file.path)
        }
        assertRows(path)
    }
  }

  test("id column mapping is written by the vanilla writer, with Parquet field IDs") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createPartitionedTable(path, "delta.columnMapping.mode" -> "id")
        writeRows(path)

        footers(path).foreach {
          case (file, footer) =>
            assert(!writtenByVelox(footer), file.path)
            assert(
              footer.getFileMetaData.getSchema.getFields.asScala.forall(_.getId != null),
              file.path)
        }
        assertRows(path)
    }
  }

  test("a write with a struct of only VOID fields fails with Delta's error") {
    withTable("void_struct") {
      val schema = new StructType().add("a", new StructType().add("v", NullType))
      sql(s"CREATE TABLE void_struct (${schema.toDDL}) USING delta")
      val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(null))), schema)
      val e = intercept[DeltaAnalysisException] {
        df.write.format("delta").mode("append").saveAsTable("void_struct")
      }
      assert(e.getCondition == "DELTA_CANNOT_WRITE_EMPTY_SCHEMA.STRUCT_ALL_VOID_FIELDS")
    }
  }
}
