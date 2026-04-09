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

/** Test suite for GlutenDeleteCommand with Deletion Vector support. */
class GlutenDeleteCommandSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  import testImplicits._

  test("DELETE with deletion vectors - small delete") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with 1000 rows
        spark
          .range(1000)
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Delete 10 rows (1% - should use DV)
        sql(s"DELETE FROM delta.`$path` WHERE id < 10")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 990)
        assert(result.filter($"id" < 10).count() == 0)

        // Verify deletion vector was created
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV > 0, "Expected deletion vectors to be created")
    }
  }

  test("DELETE with file rewrite - large delete") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with 1000 rows
        spark
          .range(1000)
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Delete 600 rows (60% - should rewrite)
        sql(s"DELETE FROM delta.`$path` WHERE id < 600")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 400)
        assert(result.filter($"id" < 600).count() == 0)

        // Verify file was rewritten (no DV)
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV == 0, "Expected no deletion vectors for large deletes")
    }
  }

  test("DELETE with condition on multiple columns") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with multiple columns
        spark
          .range(1000)
          .selectExpr("id", "id % 10 as category", "id * 2 as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Delete with complex condition
        sql(s"DELETE FROM delta.`$path` WHERE category = 5 AND value < 100")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.filter($"category" === 5 && $"value" < 100).count() == 0)
    }
  }

  test("DELETE with no matching rows") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(100)
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        val deltaLog = DeltaLog.forTable(spark, path)
        val versionBefore = deltaLog.snapshot.version

        // Delete with no matches
        sql(s"DELETE FROM delta.`$path` WHERE id > 1000")

        // Verify no changes
        val versionAfter = deltaLog.snapshot.version
        assert(versionBefore == versionAfter, "Expected no new version for no-op delete")
    }
  }

  test("DELETE all rows") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(100)
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Delete all rows
        sql(s"DELETE FROM delta.`$path` WHERE id >= 0")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 0)
    }
  }

  test("DELETE with partitioned table") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create partitioned table
        spark
          .range(1000)
          .selectExpr("id", "id % 10 as partition")
          .write
          .format("delta")
          .partitionBy("partition")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Delete from specific partition
        sql(s"DELETE FROM delta.`$path` WHERE partition = 5")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.filter($"partition" === 5).count() == 0)
        assert(result.count() == 900)
    }
  }

  test("DELETE with existing deletion vectors") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(1000)
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // First delete (creates DV)
        sql(s"DELETE FROM delta.`$path` WHERE id < 10")

        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot1 = deltaLog.snapshot
        val dvCount1 = snapshot1.allFiles.filter(_.deletionVector != null).count()

        // Second delete (merges with existing DV)
        sql(s"DELETE FROM delta.`$path` WHERE id >= 10 AND id < 20")

        val snapshot2 = deltaLog.update()
        val dvCount2 = snapshot2.allFiles.filter(_.deletionVector != null).count()

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 980)
        assert(result.filter($"id" < 20).count() == 0)

        // Verify DV was merged (same file, updated DV)
        assert(dvCount2 >= dvCount1, "Expected deletion vectors to be merged")
    }
  }

  test("DELETE metrics collection") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(1000)
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Delete with metrics
        val result = sql(s"DELETE FROM delta.`$path` WHERE id < 10")

        // Verify metrics are returned
        val metrics = result.collect()
        assert(metrics.length > 0)

        // Verify metric values
        val row = metrics(0)
        assert(row.getLong(0) > 0, "Expected numRemovedFiles > 0")
        assert(row.getLong(1) == 10, "Expected numDeletedRows = 10")
    }
  }

  test("DELETE with DVs disabled") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(1000)
          .write
          .format("delta")
          .save(path)

        // Explicitly disable deletion vectors
        sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")

        // Delete (should rewrite)
        sql(s"DELETE FROM delta.`$path` WHERE id < 10")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 990)

        // Verify no deletion vectors
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV == 0, "Expected no deletion vectors when disabled")
    }
  }

  test("DELETE threshold behavior") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with known size
        spark
          .range(1000)
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Delete exactly at threshold (50%)
        sql(s"DELETE FROM delta.`$path` WHERE id < 500")

        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot

        // At threshold, should use DV (not rewrite)
        val filesWithDV = snapshot.allFiles.filter(_.deletionVector != null).count()
        assert(filesWithDV > 0, "Expected deletion vectors at threshold")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.count() == 500)
    }
  }

  test("DELETE with NULL values") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with NULLs
        spark
          .range(100)
          .selectExpr("id", "CASE WHEN id % 10 = 0 THEN NULL ELSE id END as value")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Delete NULL values
        sql(s"DELETE FROM delta.`$path` WHERE value IS NULL")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.filter($"value".isNull).count() == 0)
        assert(result.count() == 90)
    }
  }

  test("DELETE with string columns") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table with strings
        spark
          .range(100)
          .selectExpr("id", "CONCAT('user_', id) as name")
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Delete with string condition
        sql(s"DELETE FROM delta.`$path` WHERE name LIKE 'user_1%'")

        // Verify results
        val result = spark.read.format("delta").load(path)
        assert(result.filter($"name".startsWith("user_1")).count() == 0)
    }
  }

  test("DELETE concurrent operations") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(1000)
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        // Simulate concurrent deletes
        val thread1 = new Thread(
          () => {
            sql(s"DELETE FROM delta.`$path` WHERE id < 100")
          })

        val thread2 = new Thread(
          () => {
            sql(s"DELETE FROM delta.`$path` WHERE id >= 900")
          })

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        // Verify results (one should succeed, one might fail)
        val result = spark.read.format("delta").load(path)
        assert(result.count() <= 900, "Expected some rows deleted")
    }
  }

  test("DELETE with time travel") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath

        // Create table
        spark
          .range(100)
          .write
          .format("delta")
          .save(path)

        // Enable deletion vectors
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

        val deltaLog = DeltaLog.forTable(spark, path)
        val version0 = deltaLog.snapshot.version

        // Delete some rows
        sql(s"DELETE FROM delta.`$path` WHERE id < 10")

        // Read at version 0 (before delete)
        val resultV0 = spark.read.format("delta").option("versionAsOf", version0).load(path)
        assert(resultV0.count() == 100)

        // Read current version (after delete)
        val resultCurrent = spark.read.format("delta").load(path)
        assert(resultCurrent.count() == 90)
    }
  }
}

// Made with Bob
