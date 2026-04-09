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

/** Test suite for GlutenUpdateCommand with Deletion Vector support. */
class GlutenUpdateCommandSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  import testImplicits._

  test("UPDATE with deletion vectors - small update") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with 1000 rows
        spark
          .range(1000)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update 10 rows (1% - should use DV)
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE id < 10")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 1000)

        // Check updated values
        val updatedRows = result.filter($"id" < 10).collect()
        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 2 + 100, s"Expected value ${id * 2 + 100}, got $value")
        }

        // Verify deletion vector was created
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV > 0, "Expected deletion vectors to be created")
    }
  }

  test("UPDATE with file rewrite - large update") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with 1000 rows
        spark
          .range(1000)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update 600 rows (60% - should rewrite)
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE id < 600")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 1000)

        // Check updated values
        val updatedRows = result.filter($"id" < 600).collect()
        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 2 + 100, s"Expected value ${id * 2 + 100}, got $value")
        }

        // Verify file was rewritten (no DV on original file)
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV == 0, "Expected no deletion vectors for large updates")
    }
  }

  test("UPDATE multiple columns") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with multiple columns
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value1", "id * 3 as value2")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update multiple columns
        sql(s"UPDATE delta.`$path` SET value1 = value1 + 10, value2 = value2 + 20 WHERE id < 10")

        // Verify results
        val result = spark.read.format("delta").load(path)
        val updatedRows = result.filter($"id" < 10).collect()

        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value1 = row.getLong(1)
            val value2 = row.getLong(2)
            assert(value1 == id * 2 + 10, s"Expected value1 ${id * 2 + 10}, got $value1")
            assert(value2 == id * 3 + 20, s"Expected value2 ${id * 3 + 20}, got $value2")
        }
    }
  }

  test("UPDATE with complex condition") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(1000)
          .selectExpr("id", "id % 10 as category", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update with complex condition
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE category = 5 AND value < 100")

        // Verify results
        val result = spark.read.format("delta").load(path)
        val updatedRows = result.filter($"category" === 5 && $"id" * 2 < 100).collect()

        updatedRows.foreach {
          row =>
            val value = row.getLong(2)
            assert(value >= 100, s"Expected value >= 100, got $value")
        }
    }
  }

  test("UPDATE with no matching rows") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
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

        // Update with no matches
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE id > 1000")

        // Verify no changes
        val versionAfter = deltaLog.snapshot.version
        assert(versionBefore == versionAfter, "Expected no new version for no-op update")
    }
  }

  test("UPDATE all rows") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update all rows
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE id >= 0")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 100)

        result.collect().foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 2 + 100, s"Expected value ${id * 2 + 100}, got $value")
        }
    }
  }

  test("UPDATE with partitioned table") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create partitioned table
        spark
          .range(1000)
          .selectExpr("id", "id % 10 as partition", "id * 2 as value")
          .write
          .format("delta")
          .partitionBy("partition")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update specific partition
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE partition = 5")

        // Verify results
        val result = spark.read.format("delta").load(path)
        val updatedRows = result.filter($"partition" === 5).collect()

        updatedRows.foreach {
          row =>
            val value = row.getLong(2)
            val id = row.getLong(0)
            assert(value == id * 2 + 100, s"Expected value ${id * 2 + 100}, got $value")
        }

        // Verify non-updated partitions unchanged
        val unchangedRows = result.filter($"partition" =!= 5).collect()
        unchangedRows.foreach {
          row =>
            val value = row.getLong(2)
            val id = row.getLong(0)
            assert(value == id * 2, s"Expected value ${id * 2}, got $value")
        }
    }
  }

  test("UPDATE with existing deletion vectors") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(1000)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // First update (creates DV)
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE id < 10")

        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot1 = deltaLog.snapshot
        val dvCount1 = snapshot1.allFiles.filter(_.deletionVector != null).count()

        // Second update (merges with existing DV)
        sql(s"UPDATE delta.`$path` SET value = value + 200 WHERE id >= 10 AND id < 20")

        val snapshot2 = deltaLog.update()
        val dvCount2 = snapshot2.allFiles.filter(_.deletionVector != null).count()

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 1000)

        // Check first batch of updates
        val firstBatch = result.filter($"id" < 10).collect()
        firstBatch.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 2 + 100, s"Expected value ${id * 2 + 100}, got $value")
        }

        // Check second batch of updates
        val secondBatch = result.filter($"id" >= 10 && $"id" < 20).collect()
        secondBatch.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 2 + 200, s"Expected value ${id * 2 + 200}, got $value")
        }
    }
  }

  test("UPDATE metrics collection") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(1000)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update with metrics
        val result = sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE id < 10")

        // Verify metrics are returned
        val metrics = result.collect()
        assert(metrics.length > 0)

        // Verify metric values
        val row = metrics(0)
        assert(row.getLong(0) > 0, "Expected numRemovedFiles > 0")
        assert(row.getLong(1) == 10, "Expected numUpdatedRows = 10")
    }
  }

  test("UPDATE with DVs disabled") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(1000)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Explicitly disable deletion vectors
        sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")

        // Update (should rewrite)
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE id < 10")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 1000)

        // Verify no deletion vectors
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV == 0, "Expected no deletion vectors when disabled")
    }
  }

  test("UPDATE with NULL values") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with NULLs
        spark
          .range(100)
          .selectExpr("id", "CASE WHEN id % 10 = 0 THEN NULL ELSE id * 2 END as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update NULL values
        sql(s"UPDATE delta.`$path` SET value = 999 WHERE value IS NULL")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.filter($"value".isNull).count() == 0)
        assert(result.filter($"value" === 999).count() == 10)
    }
  }

  test("UPDATE with string columns") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with strings
        spark
          .range(100)
          .selectExpr("id", "CONCAT('user_', id) as name", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update with string condition
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE name LIKE 'user_1%'")

        // Verify results
        val result = spark.read.format("delta").load(path)
        val updatedRows = result.filter($"name".startsWith("user_1")).collect()

        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(2)
            assert(value == id * 2 + 100, s"Expected value ${id * 2 + 100}, got $value")
        }
    }
  }

  test("UPDATE with computed expressions") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(100)
          .selectExpr("id", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Update with computed expression
        sql(s"UPDATE delta.`$path` SET value = value * 2 + id WHERE id < 10")

        // Verify results
        val result = spark.read.format("delta").load(path)
        val updatedRows = result.filter($"id" < 10).collect()

        updatedRows.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            val expected = (id * 2) * 2 + id
            assert(value == expected, s"Expected value $expected, got $value")
        }
    }
  }

  test("UPDATE with time travel") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
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

        // Update some rows
        sql(s"UPDATE delta.`$path` SET value = value + 100 WHERE id < 10")

        // Read at version 0 (before update)
        val resultV0 = spark.read.format("delta").option("versionAsOf", version0).load(path)
        val oldValues = resultV0.filter($"id" < 10).collect()
        oldValues.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 2, s"Expected old value ${id * 2}, got $value")
        }

        // Read current version (after update)
        val resultCurrent = spark.read.format("delta").load(path)
        val newValues = resultCurrent.filter($"id" < 10).collect()
        newValues.foreach {
          row =>
            val id = row.getLong(0)
            val value = row.getLong(1)
            assert(value == id * 2 + 100, s"Expected new value ${id * 2 + 100}, got $value")
        }
    }
  }
}

// Made with Bob
