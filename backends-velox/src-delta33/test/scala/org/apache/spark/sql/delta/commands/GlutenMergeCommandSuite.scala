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
package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/** Test suite for GlutenMergeCommand with Deletion Vector support. */
class GlutenMergeCommandSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  import testImplicits._

  test("MERGE with matched update - small update") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table
        val source = spark
          .range(10)
          .selectExpr("id", "id * 10 as new_value")
        source.createOrReplaceTempView("source")

        // MERGE with update (10% - should use DV)
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET value = source.new_value
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 100)

        // Check updated values
        val updatedRows = result.filter($"id" < 10).collect()
        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 10, s"Expected value ${id * 10}, got $value")
        }

        // Verify deletion vector was created
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV > 0, "Expected deletion vectors to be created")
    }
  }

  test("MERGE with matched delete - small delete") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table
        val source = spark.range(10).selectExpr("id")
        source.createOrReplaceTempView("source")

        // MERGE with delete (10% - should use DV)
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN MATCHED THEN DELETE
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 90)
        assert(result.filter($"id" < 10).count() == 0)

        // Verify deletion vector was created
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV > 0, "Expected deletion vectors to be created")
    }
  }

  test("MERGE with not matched insert") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(50)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table with new rows
        val source = spark
          .range(50, 100)
          .selectExpr("id", "id * 2 as value")
        source.createOrReplaceTempView("source")

        // MERGE with insert
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 100)

        // Check inserted values
        val insertedRows = result.filter($"id" >= 50).collect()
        assert(insertedRows.length == 50)
        insertedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 2, s"Expected value ${id * 2}, got $value")
        }
    }
  }

  test("MERGE with all three clauses") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table
        val source = spark
          .range(0, 150)
          .selectExpr("id", "id * 10 as new_value", "id % 3 as action_type")
        source.createOrReplaceTempView("source")

        // MERGE with update, delete, and insert
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN MATCHED AND source.action_type = 0 THEN UPDATE SET value = source.new_value
        WHEN MATCHED AND source.action_type = 1 THEN DELETE
        WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.new_value)
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)

        // Check updated rows (action_type = 0)
        val updatedRows = result.filter($"id" < 100 && $"id" % 3 === 0).collect()
        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 10, s"Expected value ${id * 10}, got $value")
        }

        // Check deleted rows (action_type = 1)
        val deletedCount = result.filter($"id" < 100 && $"id" % 3 === 1).count()
        assert(deletedCount == 0, "Expected deleted rows to be removed")

        // Check inserted rows (id >= 100)
        val insertedRows = result.filter($"id" >= 100).collect()
        assert(insertedRows.length == 50)
    }
  }

  test("MERGE with conditional matched clauses") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value", "id % 10 as category")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table
        val source = spark
          .range(50)
          .selectExpr("id", "id * 10 as new_value")
        source.createOrReplaceTempView("source")

        // MERGE with conditional update
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN MATCHED AND target.category < 5 THEN UPDATE SET value = source.new_value
        WHEN MATCHED AND target.category >= 5 THEN DELETE
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)

        // Check conditionally updated rows
        val updatedRows = result.filter($"id" < 50 && $"category" < 5).collect()
        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 10, s"Expected value ${id * 10}, got $value")
        }

        // Check conditionally deleted rows
        val deletedCount = result.filter($"id" < 50 && $"category" >= 5).count()
        assert(deletedCount == 0, "Expected conditionally deleted rows to be removed")
    }
  }

  test("MERGE with no matching rows") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        val deltaLog = DeltaLog.forTable(spark, path)
        val versionBefore = deltaLog.snapshot.version

        // Create source table with no matches
        val source = spark.range(200, 300).selectExpr("id", "id * 2 as value")
        source.createOrReplaceTempView("source")

        // MERGE with no matches (only inserts)
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
      """)

        // Verify new version created (inserts happened)
        val versionAfter = deltaLog.update().version
        assert(versionAfter > versionBefore, "Expected new version for inserts")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 200)
    }
  }

  test("MERGE with partitioned table") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create partitioned target table
        spark
          .range(100)
          .selectExpr("id", "id % 10 as partition", "id * 2 as value")
          .write
          .format("delta")
          .partitionBy("partition")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table
        val source = spark
          .range(10)
          .selectExpr("id", "5 as partition", "id * 10 as new_value")
        source.createOrReplaceTempView("source")

        // MERGE on specific partition
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id AND target.partition = source.partition
        WHEN MATCHED THEN UPDATE SET value = source.new_value
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 100)

        // Check updated partition
        val updatedRows = result.filter($"partition" === 5 && $"id" < 10).collect()
        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(2)
            assert(value == id * 10, s"Expected value ${id * 10}, got $value")
        }
    }
  }

  test("MERGE with large matched update - file rewrite") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(1000)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table with large overlap (60%)
        val source = spark
          .range(600)
          .selectExpr("id", "id * 10 as new_value")
        source.createOrReplaceTempView("source")

        // MERGE with large update (should rewrite)
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET value = source.new_value
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 1000)

        // Verify file was rewritten (no DV on original file)
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV == 0, "Expected no deletion vectors for large updates")
    }
  }

  test("MERGE metrics collection") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table
        val source = spark
          .range(0, 150)
          .selectExpr("id", "id * 10 as new_value", "id % 2 as action_type")
        source.createOrReplaceTempView("source")

        // MERGE with metrics
        val result = sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN MATCHED AND source.action_type = 0 THEN UPDATE SET value = source.new_value
        WHEN MATCHED AND source.action_type = 1 THEN DELETE
        WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.new_value)
      """)

        // Verify metrics are returned
        val metrics = result.collect()
        assert(metrics.length > 0)

        // Verify metric values
        val row = metrics(0)
        assert(row.getLong(0) > 0, "Expected numRemovedFiles > 0")
    }
  }

  test("MERGE with DVs disabled") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Explicitly disable deletion vectors
        sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")

        // Create source table
        val source = spark.range(10).selectExpr("id", "id * 10 as new_value")
        source.createOrReplaceTempView("source")

        // MERGE (should rewrite)
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET value = source.new_value
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 100)

        // Verify no deletion vectors
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV == 0, "Expected no deletion vectors when disabled")
    }
  }

  test("MERGE with NULL values") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table with NULLs
        spark
          .range(100)
          .selectExpr("id", "CASE WHEN id % 10 = 0 THEN NULL ELSE id * 2 END as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table
        val source = spark.range(10).selectExpr("id", "999 as new_value")
        source.createOrReplaceTempView("source")

        // MERGE to update NULL values
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN MATCHED AND target.value IS NULL THEN UPDATE SET value = source.new_value
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.filter($"value".isNull).count() == 0)
        assert(result.filter($"value" === 999).count() == 1) // Only id=0 was NULL
    }
  }

  test("MERGE with complex join condition") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(100)
          .selectExpr("id", "id % 10 as category", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Create source table
        val source = spark
          .range(50)
          .selectExpr("id", "id % 10 as category", "id * 10 as new_value")
        source.createOrReplaceTempView("source")

        // MERGE with complex condition
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id AND target.category = source.category
        WHEN MATCHED THEN UPDATE SET value = source.new_value
      """)

        // Verify results
        val result = spark.read.format("delta").load(path)
        val updatedRows = result.filter($"id" < 50).collect()

        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(2)
            assert(value == id * 10, s"Expected value ${id * 10}, got $value")
        }
    }
  }

  test("MERGE with time travel") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create target table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        val deltaLog = DeltaLog.forTable(spark, path)
        val version0 = deltaLog.snapshot.version

        // Create source table
        val source = spark.range(10).selectExpr("id", "id * 10 as new_value")
        source.createOrReplaceTempView("source")

        // MERGE
        sql(s"""
        MERGE INTO delta.`$path` AS target
        USING source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET value = source.new_value
      """)

        // Read at version 0 (before merge)
        val resultV0 = spark.read.format("delta").option("versionAsOf", version0).load(path)
        val oldValues = resultV0.filter($"id" < 10).collect()
        oldValues.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 2, s"Expected old value ${id * 2}, got $value")
        }

        // Read current version (after merge)
        val resultCurrent = spark.read.format("delta").load(path)
        val newValues = resultCurrent.filter($"id" < 10).collect()
        newValues.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 10, s"Expected new value ${id * 10}, got $value")
        }
    }
  }
}

// Made with Bob
