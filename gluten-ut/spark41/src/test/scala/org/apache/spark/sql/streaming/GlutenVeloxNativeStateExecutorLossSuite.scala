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
package org.apache.spark.sql.streaming

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.streaming.state.VeloxNativeStateStoreProvider

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.operators.stateful.{VeloxNativeStreamingCountExec, VeloxNativeStreamingDeduplicateExec}
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

class GlutenVeloxNativeStateExecutorLossSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var sparkHome: File = _
  private var previousSparkTestHome: Option[String] = None

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkHome = minimalSparkHome()
    previousSparkTestHome = Option(System.getProperty("spark.test.home"))
    System.setProperty("spark.test.home", sparkHome.getCanonicalPath)
    spark = SparkSession
      .builder()
      .master("local-cluster[2,1,4096]")
      .appName("GlutenVeloxNativeStateExecutorLossSuite")
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
      if (sparkHome != null) {
        deleteRecursively(sparkHome)
        sparkHome = null
      }
      previousSparkTestHome match {
        case Some(value) => System.setProperty("spark.test.home", value)
        case None => System.clearProperty("spark.test.home")
      }
      previousSparkTestHome = None
    } finally {
      super.afterAll()
    }
  }

  test("Velox native state survives repeated executor halts after native commit") {
    withTempDir("velox-native-state-executor-loss") {
      dir =>
        val inputDir = new File(dir, "input")
        val checkpointDir = new File(dir, "checkpoint")
        val outputDir = new File(dir, "output")
        val restartCycles = positiveIntTestParam(
          "gluten.native.streaming.state.executorLossRestartCycles",
          "GLUTEN_NATIVE_STREAMING_STATE_EXECUTOR_LOSS_RESTART_CYCLES",
          1)
        Files.createDirectories(inputDir.toPath)
        Files.createDirectories(checkpointDir.toPath)
        Files.createDirectories(outputDir.toPath)

        var expected = Set.empty[Int]
        (0 to restartCycles).foreach {
          cycle =>
            val inputValues = if (cycle == 0) {
              expected ++= Set(1, 2)
              Seq(1, 1, 2)
            } else {
              val newValue = cycle + 2
              expected += newValue
              Seq(cycle + 1, newValue)
            }

            val failureMarker = new File(dir, s"halted-after-native-state-commit-$cycle")
            writeJsonFile(inputDir, s"batch-$cycle.json", inputValues)

            val query = startExecutorHaltQuery(inputDir, checkpointDir, outputDir, failureMarker)
            try {
              assert(query.awaitTermination(TimeUnit.MINUTES.toMillis(2)))
              assertNativeDeduplicatePlan(query)
            } finally {
              query.stop()
            }

            assert(failureMarker.isFile)
            assertOutput(outputDir, expected)
            assertNativeStateRows(checkpointDir, expected)
            assertNoNativeTempFiles(checkpointDir)
        }

        assert(nativeSnapshotFiles(checkpointDir).nonEmpty)
    }
  }

  test("Velox native count state survives repeated executor halts after native commit") {
    withTempDir("velox-native-count-state-executor-loss") {
      dir =>
        val inputDir = new File(dir, "input")
        val checkpointDir = new File(dir, "checkpoint")
        val restartCycles = positiveIntTestParam(
          "gluten.native.streaming.state.executorLossRestartCycles",
          "GLUTEN_NATIVE_STREAMING_STATE_EXECUTOR_LOSS_RESTART_CYCLES",
          1)
        Files.createDirectories(inputDir.toPath)
        Files.createDirectories(checkpointDir.toPath)

        var expectedCounts = Map.empty[Int, Long]
        (0 to restartCycles).foreach {
          cycle =>
            val inputValues = if (cycle == 0) {
              Seq(1, 1, 2)
            } else {
              val newValue = cycle + 2
              Seq(1, newValue, newValue)
            }
            expectedCounts = incrementCounts(expectedCounts, inputValues)

            val failureMarker = new File(dir, s"halted-after-native-count-commit-$cycle")
            writeJsonFile(inputDir, s"count-batch-$cycle.json", inputValues)

            val query =
              startExecutorHaltCountQuery(inputDir, checkpointDir, failureMarker)
            try {
              assert(query.awaitTermination(TimeUnit.MINUTES.toMillis(2)))
              assertNativeCountPlan(query)
            } finally {
              query.stop()
            }

            assert(failureMarker.isFile)
            assertNativeCountStateRows(checkpointDir, expectedCounts)
            assertNoNativeTempFiles(checkpointDir)
        }

        assert(nativeSnapshotFiles(checkpointDir).nonEmpty)
    }
  }

  private def sparkConf: SparkConf = {
    val conf = new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.home", sparkHome.getCanonicalPath)
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set(SQLConf.STATE_STORE_PROVIDER_CLASS.key, classOf[VeloxNativeStateStoreProvider].getName)
      .set(GlutenConfig.NATIVE_STREAMING_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_STATEFUL_DEDUPLICATE_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_STATEFUL_AGGREGATION_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key, "true")
      .set(VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key, "true")
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set("spark.executor.extraClassPath", testClasspath)
      .set("spark.executor.extraJavaOptions", executorJavaOptions)
      .set("spark.executorEnv.SPARK_HOME", sparkHome.getCanonicalPath)
      .set("spark.executorEnv.SPARK_SCALA_VERSION", "2.13")
      .set("spark.executorEnv.SPARK_TESTING", "1")
      .set("spark.executor.instances", "2")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "1g")
      .set("spark.task.maxFailures", "4")
      .set("spark.speculation", "false")
      .set("spark.dynamicAllocation.enabled", "false")
    Option(System.getProperty("java.library.path"))
      .filter(_.nonEmpty)
      .foreach(path => conf.set("spark.executor.extraLibraryPath", path))
    conf
  }

  private def startExecutorHaltQuery(
      inputDir: File,
      checkpointDir: File,
      outputDir: File,
      failureMarker: File): StreamingQuery = {
    val sparkSession = spark
    import sparkSession.implicits._

    deduplicatedInput(inputDir)
      .mapPartitions {
        values =>
          val buffered = values.toArray
          val context = TaskContext.get()
          if (context != null && context.attemptNumber() == 0) {
            Option(failureMarker.getParentFile).foreach(_.mkdirs())
            if (failureMarker.createNewFile()) {
              Runtime.getRuntime.halt(87)
            }
          }
          buffered.iterator
      }
      .toDF("value")
      .writeStream
      .format("parquet")
      .option("checkpointLocation", checkpointDir.getCanonicalPath)
      .option("path", outputDir.getCanonicalPath)
      .trigger(Trigger.AvailableNow())
      .start()
  }

  private def startExecutorHaltCountQuery(
      inputDir: File,
      checkpointDir: File,
      failureMarker: File): StreamingQuery = {
    val sparkSession = spark
    import sparkSession.implicits._

    countedInput(inputDir)
      .mapPartitions {
        values =>
          val buffered = values.toArray
          val context = TaskContext.get()
          if (context != null && context.attemptNumber() == 0) {
            Option(failureMarker.getParentFile).foreach(_.mkdirs())
            if (failureMarker.createNewFile()) {
              Runtime.getRuntime.halt(87)
            }
          }
          buffered.iterator
      }
      .toDF("value", "count")
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "1000")
      .option("checkpointLocation", checkpointDir.getCanonicalPath)
      .trigger(Trigger.AvailableNow())
      .start()
  }

  private def deduplicatedInput(inputDir: File) = {
    val sparkSession = spark
    import sparkSession.implicits._

    val schema = new StructType().add("value", IntegerType, nullable = false)
    sparkSession.readStream
      .schema(schema)
      .json(inputDir.getCanonicalPath)
      .select(col("value").cast("int").as("value"))
      .dropDuplicates("value")
      .as[Int]
  }

  private def countedInput(inputDir: File) = {
    val sparkSession = spark
    import sparkSession.implicits._

    val schema = new StructType().add("value", IntegerType, nullable = false)
    sparkSession.readStream
      .schema(schema)
      .json(inputDir.getCanonicalPath)
      .select(col("value").cast("int").as("value"))
      .groupBy("value")
      .count()
      .select(col("value").cast("int").as("value"), col("count").cast("long").as("count"))
      .as[(Int, Long)]
  }

  private def assertNativeDeduplicatePlan(query: StreamingQuery): Unit = {
    val plan = lastExecutedPlan(query)
    assert(
      plan.collect { case _: VeloxNativeStreamingDeduplicateExec => true }.nonEmpty,
      s"Expected Velox native streaming deduplicate exec:\n$plan"
    )
  }

  private def assertNativeCountPlan(query: StreamingQuery): Unit = {
    val plan = lastExecutedPlan(query)
    assert(
      plan.collect { case _: VeloxNativeStreamingCountExec => true }.nonEmpty,
      s"Expected Velox native streaming count aggregation exec:\n$plan"
    )
  }

  private def lastExecutedPlan(query: StreamingQuery): SparkPlan = {
    query
      .asInstanceOf[StreamingQueryWrapper]
      .streamingQuery
      .lastExecution
      .executedPlan
  }

  private def assertOutput(outputDir: File, expected: Set[Int]): Unit = {
    val oldGlutenEnabled = spark.conf.getOption(GlutenConfig.GLUTEN_ENABLED.key)
    spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, "false")
    try {
      val actual = spark.read
        .parquet(outputDir.getCanonicalPath)
        .select("value")
        .collect()
        .map(_.getInt(0))
        .toSet
      assert(actual == expected)
    } finally {
      oldGlutenEnabled match {
        case Some(value) => spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, value)
        case None => spark.conf.unset(GlutenConfig.GLUTEN_ENABLED.key)
      }
    }
  }

  private def assertNativeStateRows(checkpointDir: File, expected: Set[Int]): Unit = {
    val actual = spark.read
      .format("statestore")
      .option("path", checkpointDir.getCanonicalPath)
      .load()
      .selectExpr("key.value as value")
      .collect()
      .map(_.getInt(0))
      .toSet

    assert(actual == expected)
  }

  private def assertNativeCountStateRows(checkpointDir: File, expected: Map[Int, Long]): Unit = {
    val actual = spark.read
      .format("statestore")
      .option("path", checkpointDir.getCanonicalPath)
      .load()
      .selectExpr("key.value as value", "value.count as count")
      .collect()
      .map(row => row.getInt(0) -> row.getLong(1))
      .toMap

    assert(actual == expected)
  }

  private def assertNoNativeTempFiles(checkpointDir: File): Unit = {
    val tempFiles = nativeCheckpointFiles(checkpointDir).filter(_.endsWith(".tmp"))
    assert(tempFiles.isEmpty, s"Found native temporary state files: ${tempFiles.mkString(", ")}")
  }

  private def nativeSnapshotFiles(checkpointDir: File): Seq[String] = {
    nativeCheckpointFiles(checkpointDir).filter(_.endsWith(".snapshot"))
  }

  private def nativeCheckpointFiles(checkpointDir: File): Seq[String] = {
    if (!checkpointDir.exists()) {
      Seq.empty
    } else {
      val paths = Files.walk(checkpointDir.toPath)
      try {
        paths.iterator().asScala.map(_.toFile).filter(_.isFile).map(_.getName).toSeq
      } finally {
        paths.close()
      }
    }
  }

  private def writeJsonFile(inputDir: File, name: String, values: Seq[Int]): Unit = {
    val file = new File(inputDir, name)
    val writer = new PrintWriter(file, StandardCharsets.UTF_8.name())
    try {
      values.foreach {
        value =>
          writer.write(s"""{"value":$value}""")
          writer.write(System.lineSeparator())
      }
    } finally {
      writer.close()
    }
  }

  private def withTempDir(prefix: String)(body: File => Unit): Unit = {
    val dir = Files.createTempDirectory(prefix).toFile
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

  private def testClasspath: String = {
    Option(System.getProperty("surefire.test.class.path"))
      .filter(_.nonEmpty)
      .getOrElse(System.getProperty("java.class.path"))
  }

  private def executorJavaOptions: String = {
    Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Djdk.reflect.useDirectMethodHandle=false",
      "-Dio.netty.tryReflectionSetAccessible=true"
    ).mkString(" ")
  }

  private def incrementCounts(current: Map[Int, Long], values: Seq[Int]): Map[Int, Long] = {
    values.foldLeft(current) {
      case (counts, value) => counts.updated(value, counts.getOrElse(value, 0L) + 1L)
    }
  }

  private def positiveIntTestParam(property: String, env: String, defaultValue: Int): Int = {
    sys.props
      .get(property)
      .orElse(sys.env.get(env))
      .map {
        value =>
          value.toIntOption.filter(_ > 0).getOrElse {
            fail(s"$property/$env must be a positive integer, got '$value'")
          }
      }
      .getOrElse(defaultValue)
  }

  private def minimalSparkHome(): File = {
    val home = Files.createTempDirectory("gluten-native-state-local-cluster-spark-home").toFile
    Files.createDirectories(new File(home, "jars").toPath)
    Files.createDirectories(new File(home, "conf").toPath)
    Files.createDirectories(new File(home, "launcher/target/scala-2.13").toPath)
    home
  }
}
