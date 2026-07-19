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
package org.apache.gluten.execution.kafka

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.kafka010.GlutenStreamKafkaSourceUtil
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.io.File
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.MessageDigest
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

object VeloxKafkaOffsetHandoffRuntimeProofApp {
  private val ProofPassedMarker = "GLUTEN_KAFKA_OFFSET_HANDOFF_RUNTIME_PROOF_PASS"
  private val AsyncProofPassedMarker = "GLUTEN_KAFKA_OFFSET_HANDOFF_ASYNC_RUNTIME_PROOF_PASS"
  private val ClasspathProofPassedMarker =
    "GLUTEN_KAFKA_OFFSET_HANDOFF_CLASSPATH_RUNTIME_PROOF_PASS"
  private val AsyncExecutionClassName =
    "org.apache.spark.sql.execution.streaming.runtime.AsyncProgressTrackingMicroBatchExecution"
  private val RuntimeOverlayClasses = Seq(
    "org.apache.spark.sql.execution.streaming.runtime.MicroBatchExecution",
    AsyncExecutionClassName,
    "org.apache.spark.sql.execution.streaming.runtime.GlutenKafkaOffsetLogHandoffHook$"
  )

  def main(args: Array[String]): Unit = {
    val options = parseArgs(args)
    val classpathOnly = optionEnabled(options, "classpath-only")
    val bootstrapServers =
      if (classpathOnly) {
        options.getOrElse("bootstrap-servers", "")
      } else {
        required(options, "bootstrap-servers")
      }
    val expectedOverlay = new File(required(options, "overlay-jar")).getCanonicalFile
    val topic = options
      .get("topic")
      .filter(_.nonEmpty)
      .getOrElse(s"gluten-offset-handoff-runtime-${UUID.randomUUID().toString.replace("-", "")}")
    val restartCycles = options.get("restart-cycles").map(_.toInt).filter(_ > 0).getOrElse(1)
    val requireSeparateExecutor = optionEnabled(options, "require-separate-executor")
    val includeAsyncProof = optionEnabled(options, "include-async-proof")
    val workDir = options
      .get("work-dir")
      .filter(_.nonEmpty)
      .map(path => new File(path).getCanonicalFile)
      .getOrElse(Files.createTempDirectory("gluten-offset-handoff-runtime-proof").toFile)

    assertDriverOverlayOrigins(expectedOverlay)

    val spark = SparkSession
      .builder()
      .appName("Gluten Kafka offset handoff runtime proof")
      .config(
        "spark.sql.extensions",
        "org.apache.gluten.execution.kafka.GlutenKafkaOffsetLogHandoffExtensions")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "512m")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config(GlutenConfig.NATIVE_STREAMING_ENABLED.key, "true")
      .config(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key, "true")
      .config(
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_NATIVE_OFFSET_PLANNING_ENABLED.key,
        "false")
      .config(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_OFFSET_LOG_HANDOFF_ENABLED.key, "true")
      .config(GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key, "true")
      .config(GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key, "true")
      .config(GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key, "true")
      .config(GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key, "-1")
      .config(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key, "-1")
      .config(GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key, "10000")
      .config(SQLConf.ANSI_ENABLED.key, "false")
      .getOrCreate()

    try {
      assertExecutorOverlayOrigins(spark, expectedOverlay, requireSeparateExecutor)
      if (classpathOnly) {
        println(ClasspathProofPassedMarker)
      } else {
        GlutenStreamKafkaSourceUtil.resetOffsetLogHandoffRuntimeHookCounters()

        val checkpoint = new File(workDir, "checkpoint").getCanonicalPath
        val output = new File(workDir, "output").getCanonicalPath
        withTopic(bootstrapServers, topic) {
          produce(bootstrapServers, topic, 0 until 3)
          runAvailableNowQuery(spark, bootstrapServers, topic, checkpoint, output)
          assertOutput(spark, output, (0 until 3).map(offset => s"value-$offset"))
          var manifestCount = assertPersistedHandoffManifests(checkpoint, topic, minCount = 1)

          var expected = (0 until 3).map(offset => s"value-$offset")
          var nextOffset = 3
          (0 until restartCycles).foreach {
            _ =>
              val offsets = nextOffset until (nextOffset + 2)
              produce(bootstrapServers, topic, offsets)
              expected = expected ++ offsets.map(offset => s"value-$offset")
              nextOffset += 2

              runAvailableNowQuery(spark, bootstrapServers, topic, checkpoint, output)
              assertOutput(spark, output, expected)
              manifestCount = assertPersistedHandoffManifests(
                checkpoint,
                topic,
                minCount = manifestCount + 1)
          }
        }

        assertRuntimeHooksFired()
        if (includeAsyncProof) {
          GlutenStreamKafkaSourceUtil.resetOffsetLogHandoffRuntimeHookCounters()
          val asyncTopic = s"$topic-async"
          val asyncCheckpoint = new File(workDir, "async-checkpoint").getCanonicalPath
          val asyncOutput = new File(workDir, "async-output").getCanonicalPath
          withTopic(bootstrapServers, asyncTopic) {
            produce(bootstrapServers, asyncTopic, 0 until 3)
            runAsyncProcessingTimeQuery(
              spark,
              bootstrapServers,
              asyncTopic,
              asyncCheckpoint,
              asyncOutput)
            var expected = (0 until 3).map(offset => s"value-$offset")
            assertOutput(spark, asyncOutput, expected)
            val manifestCount =
              assertPersistedHandoffManifests(asyncCheckpoint, asyncTopic, minCount = 1)

            produce(bootstrapServers, asyncTopic, 3 until 5)
            expected = expected ++ (3 until 5).map(offset => s"value-$offset")
            runAsyncProcessingTimeQuery(
              spark,
              bootstrapServers,
              asyncTopic,
              asyncCheckpoint,
              asyncOutput)
            assertOutput(spark, asyncOutput, expected)
            assertPersistedHandoffManifests(
              asyncCheckpoint,
              asyncTopic,
              minCount = manifestCount + 1)
          }
          assertRuntimeHooksFired()
          println(AsyncProofPassedMarker)
        }
        println(ProofPassedMarker)
      }
    } finally {
      spark.stop()
    }
  }

  private def parseArgs(args: Array[String]): Map[String, String] = {
    val builder = Map.newBuilder[String, String]
    var index = 0
    while (index < args.length) {
      val rawKey = args(index)
      if (!rawKey.startsWith("--") || index + 1 >= args.length) {
        throw new IllegalArgumentException(
          s"Expected --key value arguments, got: ${args.mkString(" ")}")
      }
      builder += rawKey.stripPrefix("--") -> args(index + 1)
      index += 2
    }
    builder.result()
  }

  private def required(options: Map[String, String], key: String): String =
    options
      .get(key)
      .filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"Missing required --$key argument"))

  private def optionEnabled(options: Map[String, String], key: String): Boolean =
    options
      .get(key)
      .exists(value => value.equalsIgnoreCase("true") || value == "1")

  private def assertDriverOverlayOrigins(expectedOverlay: File): Unit = {
    RuntimeOverlayClasses.foreach {
      className =>
        val actual = classOrigin(className)
        requireSameOverlay("driver", className, actual, expectedOverlay)
    }
  }

  private def assertExecutorOverlayOrigins(
      spark: SparkSession,
      expectedOverlay: File,
      requireSeparateExecutor: Boolean): Unit = {
    val origins = spark.sparkContext
      .parallelize(0 until math.max(2, spark.sparkContext.defaultParallelism), 2)
      .mapPartitions {
        _ =>
          val executorId = org.apache.spark.SparkEnv.get.executorId
          val host = InetAddress.getLocalHost.getHostName
          RuntimeOverlayClasses.iterator.map {
            className =>
              Seq(
                executorId,
                host,
                className,
                classOrigin(className).map(_.getCanonicalPath).getOrElse("<missing>"))
                .mkString("\t")
          }
      }
      .collect()
      .map {
        raw =>
          val parts = raw.split("\t", 4)
          if (parts.length != 4) {
            throw new IllegalStateException(s"Malformed executor class-origin row: $raw")
          }
          (parts(0), parts(1), parts(2), parts(3))
      }

    if (requireSeparateExecutor && !origins.exists(_._1 != "driver")) {
      val seen = origins.map(origin => s"${origin._1}@${origin._2}").distinct.sorted
      throw new IllegalStateException(
        "Expected at least one non-driver executor for Kafka offset handoff runtime proof; " +
          s"observed ${seen.mkString("[", ",", "]")}")
    }

    origins.foreach {
      case (executorId, host, className, origin) =>
        val actual = if (origin == "<missing>") None else Some(new File(origin).getCanonicalFile)
        requireSameOverlay(
          s"executor $executorId@$host",
          className,
          actual,
          expectedOverlay)
    }
  }

  private def classOrigin(className: String): Option[File] =
    Option(Class.forName(className).getProtectionDomain)
      .flatMap(domain => Option(domain.getCodeSource))
      .flatMap(codeSource => Option(codeSource.getLocation))
      .map(url => new File(url.toURI).getCanonicalFile)

  private def requireSameOverlay(
      where: String,
      className: String,
      actual: Option[File],
      expectedOverlay: File): Unit = {
    val actualFile = actual.getOrElse {
      throw new IllegalStateException(s"$where class origin missing for $className")
    }
    if (actualFile == expectedOverlay) {
      return
    }
    if (!actualFile.isFile || overlaySha256(actualFile) != overlaySha256(expectedOverlay)) {
      throw new IllegalStateException(
        s"$where loaded $className from ${actualFile.getCanonicalPath}, expected " +
          s"${expectedOverlay.getCanonicalPath}")
    }
  }

  private def overlaySha256(file: File): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(Files.readAllBytes(file.toPath))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private def runAvailableNowQuery(
      spark: SparkSession,
      bootstrapServers: String,
      topic: String,
      checkpoint: String,
      output: String): Unit = {
    val query = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "2")
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
      .writeStream
      .format("parquet")
      .option("checkpointLocation", checkpoint)
      .option("path", output)
      .trigger(Trigger.AvailableNow())
      .start()
    awaitAndStop(query)
  }

  private def runAsyncProcessingTimeQuery(
      spark: SparkSession,
      bootstrapServers: String,
      topic: String,
      checkpoint: String,
      output: String): Unit = {
    val query = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "2")
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
      .writeStream
      .format("parquet")
      .option("checkpointLocation", checkpoint)
      .option("path", output)
      .option("asyncProgressTrackingEnabled", "true")
      .option("asyncProgressTrackingCheckpointIntervalMs", "0")
      .option("_asyncProgressTrackingOverrideSinkSupportCheck", "true")
      .trigger(Trigger.ProcessingTime("1 second"))
      .start()
    assertAsyncExecution(query)
    processAllAvailableAndStop(query)
  }

  private def awaitAndStop(query: StreamingQuery): Unit = {
    try {
      if (!query.awaitTermination(TimeUnit.MINUTES.toMillis(1))) {
        throw new IllegalStateException("Timed out waiting for AvailableNow query")
      }
    } finally {
      query.stop()
    }
  }

  private def processAllAvailableAndStop(query: StreamingQuery): Unit = {
    var failure: Throwable = null
    val thread = new Thread(
      () =>
        try {
          query.processAllAvailable()
        } catch {
          case t: Throwable => failure = t
        },
      "gluten-kafka-offset-handoff-async-process-all-available")
    thread.setDaemon(true)
    try {
      thread.start()
      thread.join(TimeUnit.MINUTES.toMillis(1))
      if (thread.isAlive) {
        query.stop()
        thread.join(TimeUnit.SECONDS.toMillis(10))
        throw new IllegalStateException("Timed out waiting for async processing-time query")
      }
      if (failure != null) {
        throw failure
      }
    } finally {
      query.stop()
    }
  }

  private def assertAsyncExecution(query: StreamingQuery): Unit = {
    val executionClassName = query match {
      case wrapper: StreamingQueryWrapper => wrapper.streamingQuery.getClass.getName
      case other => other.getClass.getName
    }
    if (executionClassName != AsyncExecutionClassName) {
      throw new IllegalStateException(
        s"Expected async progress execution $AsyncExecutionClassName, got $executionClassName")
    }
  }

  private def assertOutput(spark: SparkSession, output: String, expected: Seq[String]): Unit = {
    val actual = spark.read.parquet(output).select("value").collect().map(_.getString(0)).sorted
    if (actual.toSeq != expected.sorted) {
      throw new IllegalStateException(
        s"Unexpected output. actual=${actual.mkString("[", ",", "]")} " +
          s"expected=${expected.sorted.mkString("[", ",", "]")}")
    }
  }

  private def assertPersistedHandoffManifests(
      checkpoint: String,
      topic: String,
      minCount: Int): Int = {
    val manifests = persistedHandoffManifestFiles(checkpoint)
    if (manifests.size < minCount) {
      throw new IllegalStateException(
        s"Expected at least $minCount persisted Kafka offset handoff manifests under " +
          s"$checkpoint, found ${manifests.size}")
    }

    val parsed = manifests.map {
      manifest =>
        val json = new String(Files.readAllBytes(manifest.toPath), StandardCharsets.UTF_8)
        GlutenStreamKafkaSourceUtil.parseOffsetLogHandoffManifestJson(json)
    }
    if (!parsed.exists(_.partitionRanges.nonEmpty)) {
      throw new IllegalStateException(
        s"Persisted Kafka offset handoff manifests under $checkpoint did not contain " +
          s"any planned partition ranges")
    }
    if (!parsed.exists(_.endPartitionOffsets.keySet.exists(_.topic() == topic))) {
      throw new IllegalStateException(
        s"Persisted Kafka offset handoff manifests under $checkpoint did not cover topic $topic")
    }
    manifests.size
  }

  private def persistedHandoffManifestFiles(checkpoint: String): Seq[File] = {
    val checkpointPath = new File(checkpoint).toPath
    if (!Files.exists(checkpointPath)) {
      return Seq.empty
    }

    val files = Files.walk(checkpointPath)
    try {
      files
        .iterator()
        .asScala
        .filter(
          path =>
            Files.isRegularFile(path) &&
              path.getFileName.toString.endsWith(".json") &&
              Option(path.getParent)
                .exists(_.getFileName.toString == "gluten-native-kafka-offset-handoff"))
        .map(_.toFile.getCanonicalFile)
        .toSeq
        .sortBy(_.getCanonicalPath)
    } finally {
      files.close()
    }
  }

  private def assertRuntimeHooksFired(): Unit = {
    if (GlutenStreamKafkaSourceUtil.offsetLogHandoffWriteHookInvocations <= 0) {
      throw new IllegalStateException(
        "Spark offset-log handoff overlay did not prepare any Kafka handoff manifests")
    }
    if (GlutenStreamKafkaSourceUtil.offsetLogHandoffReplayHookInvocations <= 0) {
      throw new IllegalStateException(
        "Spark offset-log handoff overlay did not validate any Kafka handoff manifests")
    }
  }

  private def withTopic(bootstrapServers: String, topic: String)(body: => Unit): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val adminClient = AdminClient.create(props)
    try {
      if (adminClient.listTopics().names().get().contains(topic)) {
        adminClient.deleteTopics(Seq(topic).asJava).all().get()
      }
      adminClient.createTopics(Seq(new NewTopic(topic, 1, 1.toShort)).asJava).all().get()
      body
    } finally {
      try {
        if (adminClient.listTopics().names().get().contains(topic)) {
          adminClient.deleteTopics(Seq(topic).asJava).all().get()
        }
      } finally {
        adminClient.close()
      }
    }
  }

  private def produce(bootstrapServers: String, topic: String, offsets: Range): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    try {
      offsets.foreach {
        offset =>
          producer.send(new ProducerRecord[String, String](
            topic,
            s"key-$offset",
            s"value-$offset")).get()
      }
      producer.flush()
    } finally {
      producer.close()
    }
  }
}
