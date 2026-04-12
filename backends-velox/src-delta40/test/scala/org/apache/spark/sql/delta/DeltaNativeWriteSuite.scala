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
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{GlutenDeltaLeafRunnableCommand, GlutenDeltaLeafV2CommandExec}
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.CopyOnWriteArrayList

import scala.jdk.CollectionConverters._

class DeltaNativeWriteSuite extends DeltaSQLCommandTest {

  import testImplicits._

  private def hasGlutenDeltaWriteCommand(plan: SparkPlan): Boolean = {
    plan
      .collectFirst {
        case ExecutedCommandExec(_: GlutenDeltaLeafRunnableCommand) => true
        case _: GlutenDeltaLeafV2CommandExec => true
      }
      .getOrElse(false)
  }

  private def collectExecutedPlans(action: => Unit): Seq[SparkPlan] = {
    val plans = new CopyOnWriteArrayList[SparkPlan]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plans.add(qe.executedPlan)
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)
    try {
      action
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

  test("native delta delete command should be offloaded") {
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

  test("native delta update command should be offloaded") {
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

  test("native delta CTAS command should be offloaded") {
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

  test("native delta RTAS command should be offloaded") {
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

  test("native delta save command should be offloaded") {
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

  test("native delta append save command should be offloaded") {
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

  test("native delta partitioned save command should be offloaded") {
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

  test("delta save command should not be offloaded when native write is disabled") {
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
