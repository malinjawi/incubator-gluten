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
package org.apache.spark.sql.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryBaseTable}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DateType, LongType, StringType}

import org.scalatest.time.SpanSugar._

import java.sql.Date
import java.util.Collections

class GlutenWriteDistributionAndOrderingSuite
  extends WriteDistributionAndOrderingSuite
  with GlutenSQLTestsBaseTrait {
  import testImplicits._

  override def sparkConf: SparkConf = {
    // Native SQL configs
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "5")
  }

  // Replaces the Spark test of the same name, which stops the query right after
  // processAllAvailable(). Spark guarantees that call only for micro-batch sources. In continuous
  // mode it returns once two epochs report the same source offsets, and when the tasks start late
  // those can be empty epochs committed before the rows are read. Wait for the rows to be
  // committed before stopping instead, as Spark's own continuous tests do (SPARK-28247).
  testGluten("continuous mode allows unspecified distribution and empty ordering") {
    // Same table as the Spark suite, whose fields are private.
    val ident = Identifier.of(Array("ns1"), "test_table")
    val tableNameAsString = "testcat." + ident.toString
    val columns = Array(
      Column.create("id", LongType),
      Column.create("data", StringType),
      Column.create("day", DateType))
    catalog.createTable(
      ident,
      columns,
      Array.empty[Transform],
      Collections.emptyMap[String, String])

    withTempDir {
      checkpointDir =>
        val inputData = ContinuousMemoryStream[(Long, String, Date)]
        val inputDF = inputData.toDF().toDF("id", "data", "day")

        val writer = inputDF.writeStream
          .trigger(Trigger.Continuous(100))
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .outputMode("append")

        val query = writer.toTable(tableNameAsString)
        try {
          inputData.addData(
            (1, "a", Date.valueOf("2021-01-01")),
            (2, "b", Date.valueOf("2022-02-02")))

          // Read the table's rows directly: a Spark job would wait for the task slots the
          // running continuous query holds.
          eventually(timeout(60.seconds)) {
            assert(catalog.loadTable(ident).asInstanceOf[InMemoryBaseTable].rows.size == 2)
          }
        } finally {
          query.stop()
        }

        checkAnswer(
          spark.table(tableNameAsString),
          Row(1, "a", Date.valueOf("2021-01-01")) ::
            Row(2, "b", Date.valueOf("2022-02-02")) :: Nil)
    }
  }
}
