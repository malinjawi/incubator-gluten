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
import org.apache.gluten.execution.{MicroBatchScanExecTransformer, VeloxWholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.kafka010.GlutenStreamKafkaSourceUtil
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

class VeloxKafkaNativeStreamingLiveSuite extends VeloxWholeStageTransformerSuite {
  override protected val fileFormat: String = "parquet"
  override protected val resourcePath: String = ""
  override val threadExcludeList: Set[String] = Set(
    "netty.*",
    "globalEventExecutor.*",
    "threadDeathWatcher.*",
    "rpc-client.*",
    "rpc-server.*",
    "shuffle-chunk-fetch-handler.*",
    "shuffle-client.*",
    "shuffle-server.*",
    "org.apache.hadoop.fs.FileSystem\\$Statistics\\$StatisticsDataReferenceCleaner",
    "broadcast-exchange.*",
    "process reaper",
    "block-manager-ask-thread-pool-.*",
    "dispatcher-BlockManagerMaster",
    "shuffle-boss.*",
    "rpc-boss.*",
    "files-client.*",
    "Cleaner-\\d+"
  )

  override protected def afterAll(): Unit = {
    try {
      stopActiveStreams()
    } finally {
      super.afterAll()
    }
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set(GlutenConfig.NATIVE_STREAMING_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_NATIVE_OFFSET_PLANNING_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_OFFSET_LOG_HANDOFF_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_PROJECT_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_FILTER_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key, "-1")
      .set(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key, "-1")
      .set(GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key, "10000")
      .set(SQLConf.ANSI_ENABLED.key, "false")
  }

  test("native Kafka finite offsets survive Spark checkpoint restart without duplicate output") {
    assume(
      sys.env.get("GLUTEN_KAFKA_SPARK_LIVE_TEST").contains("1"),
      "Set GLUTEN_KAFKA_SPARK_LIVE_TEST=1 and provide a live Kafka broker to run this suite.")

    val bootstrapServers =
      sys.env.getOrElse("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
    val restartCycles = positiveIntEnv("GLUTEN_KAFKA_SPARK_LIVE_RESTART_CYCLES", 1)
    val topic = sys.env
      .get("GLUTEN_KAFKA_TEST_TOPIC")
      .filter(_.nonEmpty)
      .getOrElse(s"gluten-native-kafka-spark-${UUID.randomUUID().toString.replace("-", "")}")
    GlutenStreamKafkaSourceUtil.resetOffsetLogHandoffRuntimeHookCounters()

    withTempDir {
      dir =>
        val checkpoint = s"${dir.getCanonicalPath}/checkpoint"
        val output = s"${dir.getCanonicalPath}/output"

        withTopic(bootstrapServers, topic) {
          produce(bootstrapServers, topic, 0 until 3)

          withStartedQuery(startQuery(bootstrapServers, topic, checkpoint, output)) {
            first =>
              assert(first.awaitTermination(TimeUnit.MINUTES.toMillis(1)))
              assertNativeKafkaScan(first)
              assertOutput(output, Seq("value-0", "value-1", "value-2"))
          }

          var expected = (0 until 3).map(offset => s"value-$offset")
          var nextOffset = 3

          (0 until restartCycles).foreach {
            _ =>
              val batchOffsets = nextOffset until (nextOffset + 2)
              produce(bootstrapServers, topic, batchOffsets)
              expected = expected ++ batchOffsets.map(offset => s"value-$offset")
              nextOffset += 2

              withStartedQuery(startQuery(bootstrapServers, topic, checkpoint, output)) {
                restarted =>
                  assert(restarted.awaitTermination(TimeUnit.MINUTES.toMillis(1)))
                  assertNativeKafkaScan(restarted)
                  assertOutput(output, expected)
              }
          }
          assertOffsetLogHandoffRuntimeHookBoundary()
        }
    }
  }

  test("native Kafka multi-partition finite offsets survive checkpoint restart") {
    assume(
      sys.env.get("GLUTEN_KAFKA_SPARK_LIVE_TEST").contains("1"),
      "Set GLUTEN_KAFKA_SPARK_LIVE_TEST=1 and provide a live Kafka broker to run this suite.")

    val bootstrapServers =
      sys.env.getOrElse("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
    val restartCycles =
      positiveIntEnv("GLUTEN_KAFKA_SPARK_LIVE_MULTI_PARTITION_RESTART_CYCLES", 1)
    val topic =
      s"gluten-native-kafka-spark-mp-${UUID.randomUUID().toString.replace("-", "")}"
    GlutenStreamKafkaSourceUtil.resetOffsetLogHandoffRuntimeHookCounters()

    withTempDir {
      dir =>
        val checkpoint = s"${dir.getCanonicalPath}/checkpoint"
        val output = s"${dir.getCanonicalPath}/output"

        withTopic(bootstrapServers, topic, partitions = 2) {
          producePartition(bootstrapServers, topic, partition = 0, 0 until 3)
          producePartition(bootstrapServers, topic, partition = 1, 0 until 2)

          withStartedQuery(startQuery(bootstrapServers, topic, checkpoint, output)) {
            first =>
              assert(first.awaitTermination(TimeUnit.MINUTES.toMillis(1)))
              assertNativeKafkaScan(first)
              assertOutput(output, partitionValues(0 -> (0 until 3), 1 -> (0 until 2)))
          }

          var nextPartition0Offset = 3
          var nextPartition1Offset = 2

          (0 until restartCycles).foreach {
            _ =>
              val partition0Offsets = nextPartition0Offset until (nextPartition0Offset + 2)
              val partition1Offsets = nextPartition1Offset until (nextPartition1Offset + 2)
              producePartition(bootstrapServers, topic, partition = 0, partition0Offsets)
              producePartition(bootstrapServers, topic, partition = 1, partition1Offsets)
              nextPartition0Offset += 2
              nextPartition1Offset += 2

              withStartedQuery(startQuery(bootstrapServers, topic, checkpoint, output)) {
                restarted =>
                  assert(restarted.awaitTermination(TimeUnit.MINUTES.toMillis(1)))
                  assertNativeKafkaScan(restarted)
                  assertOutput(
                    output,
                    partitionValues(
                      0 -> (0 until nextPartition0Offset),
                      1 -> (0 until nextPartition1Offset)))
              }
          }
          assertOffsetLogHandoffRuntimeHookBoundary()
        }
    }
  }

  test("native Kafka subscribePattern discovers evolved topics across checkpoint restart") {
    assume(
      sys.env.get("GLUTEN_KAFKA_SPARK_LIVE_TEST").contains("1"),
      "Set GLUTEN_KAFKA_SPARK_LIVE_TEST=1 and provide a live Kafka broker to run this suite.")

    val bootstrapServers =
      sys.env.getOrElse("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
    val topicPrefix =
      s"gluten-native-kafka-spark-pattern-${UUID.randomUUID().toString.replace("-", "")}"
    val topicA = s"$topicPrefix-a"
    val topicB = s"$topicPrefix-b"
    val unmatchedTopic = s"$topicPrefix-unmatched"
    val topicPattern = s"$topicPrefix-[ab]"
    GlutenStreamKafkaSourceUtil.resetOffsetLogHandoffRuntimeHookCounters()

    withTempDir {
      dir =>
        val checkpoint = s"${dir.getCanonicalPath}/checkpoint"
        val output = s"${dir.getCanonicalPath}/output"

        withTopics(bootstrapServers, Seq(topicA, unmatchedTopic)) {
          try {
            produceValues(bootstrapServers, topicA, "a", 0 until 3)
            produceValues(bootstrapServers, unmatchedTopic, "miss", 0 until 2)

            withStartedQuery(startPatternQuery(
              bootstrapServers,
              topicPattern,
              checkpoint,
              output)) {
              first =>
                assert(first.awaitTermination(TimeUnit.MINUTES.toMillis(1)))
                assertNativeKafkaScan(first)
                assertOutput(output, Seq("a-0", "a-1", "a-2"))
            }

            createTopic(bootstrapServers, topicB)
            produceValues(bootstrapServers, topicB, "b", 0 until 2)

            withStartedQuery(startPatternQuery(
              bootstrapServers,
              topicPattern,
              checkpoint,
              output)) {
              restarted =>
                assert(restarted.awaitTermination(TimeUnit.MINUTES.toMillis(1)))
                assertNativeKafkaScan(restarted)
                assertOutput(output, Seq("a-0", "a-1", "a-2", "b-0", "b-1"))
            }
            assertOffsetLogHandoffRuntimeHookBoundary()
          } finally {
            deleteTopics(bootstrapServers, Seq(topicB))
          }
        }
    }
  }

  private def startQuery(
      bootstrapServers: String,
      topic: String,
      checkpoint: String,
      output: String): StreamingQuery = {
    readKafkaValues(bootstrapServers, topic, maxOffsetsPerTrigger = "2")
      .writeStream
      .format("parquet")
      .option("checkpointLocation", checkpoint)
      .option("path", output)
      .trigger(Trigger.AvailableNow())
      .start()
  }

  private def startPatternQuery(
      bootstrapServers: String,
      topicPattern: String,
      checkpoint: String,
      output: String): StreamingQuery = {
    readKafkaPatternValues(bootstrapServers, topicPattern, maxOffsetsPerTrigger = "100")
      .writeStream
      .format("parquet")
      .option("checkpointLocation", checkpoint)
      .option("path", output)
      .trigger(Trigger.AvailableNow())
      .start()
  }

  private def withStartedQuery(query: StreamingQuery)(body: StreamingQuery => Unit): Unit = {
    try {
      body(query)
    } finally {
      stopQuery(query)
    }
  }

  private def stopActiveStreams(): Unit = {
    spark.streams.active.foreach(stopQuery)
  }

  private def stopQuery(query: StreamingQuery): Unit = {
    if (query != null) {
      query.stop()
    }
  }

  private def readKafkaValues(
      bootstrapServers: String,
      topic: String,
      maxOffsetsPerTrigger: String) = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
  }

  private def readKafkaPatternValues(
      bootstrapServers: String,
      topicPattern: String,
      maxOffsetsPerTrigger: String) = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribePattern", topicPattern)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
  }

  private def assertNativeKafkaScan(query: StreamingQuery): Unit = {
    val transformers = lastExecutedPlan(query).collect {
      case transformer: MicroBatchScanExecTransformer => transformer
    }
    assert(transformers.nonEmpty, lastExecutedPlan(query).treeString)
  }

  private def assertOutput(output: String, expected: Seq[String]): Unit = {
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      val actual = spark.read.parquet(output).select("value").collect().map(_.getString(0)).sorted
      assert(actual.toSeq == expected.sorted)
    }
  }

  private def assertOffsetLogHandoffRuntimeHookBoundary(): Unit = {
    if (expectsOffsetLogHandoffOverlay) {
      assert(
        GlutenStreamKafkaSourceUtil.offsetLogHandoffWriteHookInvocations > 0,
        "Spark 4.1 offset-log handoff overlay did not invoke the pre-WAL write hook")
      assert(
        GlutenStreamKafkaSourceUtil.offsetLogHandoffReplayHookInvocations > 0,
        "Spark 4.1 offset-log handoff overlay did not invoke the checkpoint replay hook"
      )
    } else {
      assert(
        GlutenStreamKafkaSourceUtil.offsetLogHandoffWriteHookInvocations == 0,
        "Non-overlay Spark live run unexpectedly invoked the pre-WAL write hook")
      assert(
        GlutenStreamKafkaSourceUtil.offsetLogHandoffReplayHookInvocations == 0,
        "Non-overlay Spark live run unexpectedly invoked the checkpoint replay hook")
    }
  }

  private def expectsOffsetLogHandoffOverlay: Boolean = {
    sys.env.get("GLUTEN_KAFKA_SPARK_TEST_USE_OFFSET_HANDOFF_OVERLAY").contains("1") ||
    sys.env.get("GLUTEN_SPARK41_OFFSET_HANDOFF_OVERLAY").contains("1") ||
    sys.env.get("GLUTEN_KAFKA_SPARK_TEST_OFFSET_HANDOFF_OVERLAY_JAR").exists(_.nonEmpty)
  }

  private def lastExecutedPlan(query: StreamingQuery): SparkPlan = {
    val streamingQuery = query.getClass.getMethod("streamingQuery").invoke(query)
    streamingQuery
      .getClass
      .getMethod("lastExecution")
      .invoke(streamingQuery)
      .asInstanceOf[QueryExecution]
      .executedPlan
  }

  private def withTopic(
      bootstrapServers: String,
      topic: String,
      partitions: Int = 1)(body: => Unit): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val adminClient = AdminClient.create(props)
    try {
      if (adminClient.listTopics().names().get().contains(topic)) {
        adminClient.deleteTopics(Seq(topic).asJava).all().get()
      }
      adminClient.createTopics(Seq(new NewTopic(topic, partitions, 1.toShort)).asJava).all().get()
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

  private def withTopics(
      bootstrapServers: String,
      topics: Seq[String],
      partitions: Int = 1)(body: => Unit): Unit = {
    topics.foreach(createTopic(bootstrapServers, _, partitions))
    try {
      body
    } finally {
      deleteTopics(bootstrapServers, topics)
    }
  }

  private def createTopic(
      bootstrapServers: String,
      topic: String,
      partitions: Int = 1): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val adminClient = AdminClient.create(props)
    try {
      if (adminClient.listTopics().names().get().contains(topic)) {
        adminClient.deleteTopics(Seq(topic).asJava).all().get()
      }
      adminClient.createTopics(Seq(new NewTopic(topic, partitions, 1.toShort)).asJava).all().get()
    } finally {
      adminClient.close()
    }
  }

  private def deleteTopics(bootstrapServers: String, topics: Seq[String]): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val adminClient = AdminClient.create(props)
    try {
      val existing = adminClient.listTopics().names().get()
      val toDelete = topics.filter(existing.contains)
      if (toDelete.nonEmpty) {
        adminClient.deleteTopics(toDelete.asJava).all().get()
      }
    } finally {
      adminClient.close()
    }
  }

  private def produce(bootstrapServers: String, topic: String, offsets: Range): Unit = {
    produceRecords(
      bootstrapServers,
      offsets.map(
        offset =>
          new ProducerRecord[String, String](
            topic,
            s"key-$offset",
            s"value-$offset")))
  }

  private def producePartition(
      bootstrapServers: String,
      topic: String,
      partition: Int,
      offsets: Range): Unit = {
    produceRecords(
      bootstrapServers,
      offsets.map(
        offset =>
          new ProducerRecord[String, String](
            topic,
            partition,
            s"key-p$partition-$offset",
            partitionValue(partition, offset))))
  }

  private def produceValues(
      bootstrapServers: String,
      topic: String,
      prefix: String,
      offsets: Range): Unit = {
    produceRecords(
      bootstrapServers,
      offsets.map(
        offset =>
          new ProducerRecord[String, String](
            topic,
            s"key-$prefix-$offset",
            s"$prefix-$offset")))
  }

  private def produceRecords(
      bootstrapServers: String,
      records: Iterable[ProducerRecord[String, String]]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    try {
      records.foreach(record => producer.send(record).get())
      producer.flush()
    } finally {
      producer.close()
    }
  }

  private def partitionValues(ranges: (Int, Range)*): Seq[String] = {
    ranges.flatMap { case (partition, offsets) => offsets.map(partitionValue(partition, _)) }
  }

  private def partitionValue(partition: Int, offset: Int): String = {
    s"value-p$partition-$offset"
  }

  private def positiveIntEnv(name: String, defaultValue: Int): Int = {
    sys.env.get(name).filter(_.nonEmpty).map(_.toInt).filter(_ > 0).getOrElse(defaultValue)
  }
}

class VeloxKafkaNativeStreamingRetryLiveSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2,2]")
      .appName("VeloxKafkaNativeStreamingRetryLiveSuite")
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
    } finally {
      super.afterAll()
    }
  }

  test("native Kafka finite offsets survive Spark task retry") {
    assume(
      sys.env.get("GLUTEN_KAFKA_SPARK_LIVE_TEST").contains("1"),
      "Set GLUTEN_KAFKA_SPARK_LIVE_TEST=1 and provide a live Kafka broker to run this suite.")

    val bootstrapServers =
      sys.env.getOrElse("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
    val topic =
      s"gluten-native-kafka-spark-fail-${UUID.randomUUID().toString.replace("-", "")}"

    withTempDir {
      dir =>
        val checkpoint = s"${dir.getCanonicalPath}/checkpoint"
        val output = s"${dir.getCanonicalPath}/output"
        val failureMarker = s"${dir.getCanonicalPath}/failed-task-attempt"

        withTopic(bootstrapServers, topic) {
          produce(bootstrapServers, topic, 0 until 3)

          val query =
            startTaskRetryQuery(bootstrapServers, topic, checkpoint, output, failureMarker)
          assert(query.awaitTermination(TimeUnit.MINUTES.toMillis(1)))
          assertNativeKafkaScan(query)

          assert(new File(failureMarker).isFile)
          assertOutput(output, Seq("value-0", "value-1", "value-2"))
        }
    }
  }

  private def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set(GlutenConfig.NATIVE_STREAMING_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_PROJECT_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_FILTER_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key, "-1")
      .set(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key, "-1")
      .set(GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key, "10000")
      .set(SQLConf.ANSI_ENABLED.key, "false")
  }

  private def startTaskRetryQuery(
      bootstrapServers: String,
      topic: String,
      checkpoint: String,
      output: String,
      failureMarker: String): StreamingQuery = {
    val sparkSession = spark
    import sparkSession.implicits._

    readKafkaValues(bootstrapServers, topic, maxOffsetsPerTrigger = "100")
      .as[String]
      .mapPartitions {
        values =>
          new Iterator[String] {
            override def hasNext: Boolean = values.hasNext

            override def next(): String = {
              val value = values.next()
              val context = TaskContext.get()
              if (context != null && context.attemptNumber() == 0) {
                val marker = new File(failureMarker)
                Option(marker.getParentFile).foreach(_.mkdirs())
                if (marker.createNewFile()) {
                  throw new RuntimeException("injected task failure after native Kafka scan")
                }
              }
              value
            }
          }
      }
      .toDF("value")
      .writeStream
      .format("parquet")
      .option("checkpointLocation", checkpoint)
      .option("path", output)
      .trigger(Trigger.AvailableNow())
      .start()
  }

  private def readKafkaValues(
      bootstrapServers: String,
      topic: String,
      maxOffsetsPerTrigger: String) = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
  }

  private def assertNativeKafkaScan(query: StreamingQuery): Unit = {
    val transformers = lastExecutedPlan(query).collect {
      case transformer: MicroBatchScanExecTransformer => transformer
    }
    assert(transformers.nonEmpty, lastExecutedPlan(query).treeString)
  }

  private def assertOutput(output: String, expected: Seq[String]): Unit = {
    val oldGlutenEnabled = spark.conf.getOption(GlutenConfig.GLUTEN_ENABLED.key)
    spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, "false")
    try {
      val actual = spark.read.parquet(output).select("value").collect().map(_.getString(0)).sorted
      assert(actual.toSeq == expected.sorted)
    } finally {
      oldGlutenEnabled match {
        case Some(value) => spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, value)
        case None => spark.conf.unset(GlutenConfig.GLUTEN_ENABLED.key)
      }
    }
  }

  private def lastExecutedPlan(query: StreamingQuery): SparkPlan = {
    val streamingQuery = query.getClass.getMethod("streamingQuery").invoke(query)
    streamingQuery
      .getClass
      .getMethod("lastExecution")
      .invoke(streamingQuery)
      .asInstanceOf[QueryExecution]
      .executedPlan
  }

  private def withTopic(
      bootstrapServers: String,
      topic: String,
      partitions: Int = 1)(body: => Unit): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val adminClient = AdminClient.create(props)
    try {
      if (adminClient.listTopics().names().get().contains(topic)) {
        adminClient.deleteTopics(Seq(topic).asJava).all().get()
      }
      adminClient.createTopics(Seq(new NewTopic(topic, partitions, 1.toShort)).asJava).all().get()
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
    produceRecords(
      bootstrapServers,
      offsets.map(
        offset =>
          new ProducerRecord[String, String](
            topic,
            s"key-$offset",
            s"value-$offset")))
  }

  private def produceRecords(
      bootstrapServers: String,
      records: Iterable[ProducerRecord[String, String]]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    try {
      records.foreach(record => producer.send(record).get())
      producer.flush()
    } finally {
      producer.close()
    }
  }

  private def withTempDir(body: File => Unit): Unit = {
    val dir = Files.createTempDirectory("velox-kafka-native-retry").toFile
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
}

class VeloxKafkaNativeStreamingExecutorLossLiveSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var sparkHome: File = _
  private var previousSparkTestHome: Option[String] = None

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (sys.env.get("GLUTEN_KAFKA_SPARK_LIVE_TEST").contains("1")) {
      sparkHome = minimalSparkHome()
      previousSparkTestHome = Option(System.getProperty("spark.test.home"))
      System.setProperty("spark.test.home", sparkHome.getCanonicalPath)
      spark = SparkSession
        .builder()
        .master("local-cluster[2,1,4096]")
        .appName("VeloxKafkaNativeStreamingExecutorLossLiveSuite")
        .config(sparkConf)
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
    }
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
        previousSparkTestHome match {
          case Some(value) => System.setProperty("spark.test.home", value)
          case None => System.clearProperty("spark.test.home")
        }
        previousSparkTestHome = None
      }
    } finally {
      super.afterAll()
    }
  }

  test("native Kafka finite offsets survive executor process halt after native scan") {
    assume(
      sys.env.get("GLUTEN_KAFKA_SPARK_LIVE_TEST").contains("1"),
      "Set GLUTEN_KAFKA_SPARK_LIVE_TEST=1 and provide a live Kafka broker to run this suite.")

    val bootstrapServers =
      sys.env.getOrElse("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
    val topic =
      s"gluten-native-kafka-spark-executor-halt-${UUID.randomUUID().toString.replace("-", "")}"

    withTempDir {
      dir =>
        val checkpoint = s"${dir.getCanonicalPath}/checkpoint"
        val output = s"${dir.getCanonicalPath}/output"
        val failureMarker = s"${dir.getCanonicalPath}/halted-executor-attempt"

        withTopic(bootstrapServers, topic) {
          produce(bootstrapServers, topic, 0 until 3)

          val query =
            startExecutorHaltQuery(bootstrapServers, topic, checkpoint, output, failureMarker)
          assert(query.awaitTermination(TimeUnit.MINUTES.toMillis(2)))
          assertNativeKafkaScan(query)
          assert(new File(failureMarker).isFile)
          assertOutput(output, Seq("value-0", "value-1", "value-2"))
        }
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
      .set(GlutenConfig.NATIVE_STREAMING_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key, "true")
      .set(GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_PROJECT_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_FILTER_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key, "-1")
      .set(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key, "-1")
      .set(GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key, "10000")
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set("spark.executor.extraClassPath", testClasspath)
      .set("spark.executor.extraJavaOptions", executorJavaOptions)
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
      bootstrapServers: String,
      topic: String,
      checkpoint: String,
      output: String,
      failureMarker: String): StreamingQuery = {
    val sparkSession = spark
    import sparkSession.implicits._

    readKafkaValues(bootstrapServers, topic, maxOffsetsPerTrigger = "100")
      .as[String]
      .mapPartitions {
        values =>
          new Iterator[String] {
            override def hasNext: Boolean = values.hasNext

            override def next(): String = {
              val value = values.next()
              val context = TaskContext.get()
              if (context != null && context.attemptNumber() == 0) {
                val marker = new File(failureMarker)
                Option(marker.getParentFile).foreach(_.mkdirs())
                if (marker.createNewFile()) {
                  System.err.println(
                    "Injected executor halt after native Kafka scan yielded a row")
                  Runtime.getRuntime.halt(87)
                }
              }
              value
            }
          }
      }
      .toDF("value")
      .writeStream
      .format("parquet")
      .option("checkpointLocation", checkpoint)
      .option("path", output)
      .trigger(Trigger.AvailableNow())
      .start()
  }

  private def readKafkaValues(
      bootstrapServers: String,
      topic: String,
      maxOffsetsPerTrigger: String) = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
  }

  private def assertNativeKafkaScan(query: StreamingQuery): Unit = {
    val transformers = lastExecutedPlan(query).collect {
      case transformer: MicroBatchScanExecTransformer => transformer
    }
    assert(transformers.nonEmpty, lastExecutedPlan(query).treeString)
  }

  private def assertOutput(output: String, expected: Seq[String]): Unit = {
    val oldGlutenEnabled = spark.conf.getOption(GlutenConfig.GLUTEN_ENABLED.key)
    spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, "false")
    try {
      val actual = spark.read.parquet(output).select("value").collect().map(_.getString(0)).sorted
      assert(actual.toSeq == expected.sorted)
    } finally {
      oldGlutenEnabled match {
        case Some(value) => spark.conf.set(GlutenConfig.GLUTEN_ENABLED.key, value)
        case None => spark.conf.unset(GlutenConfig.GLUTEN_ENABLED.key)
      }
    }
  }

  private def lastExecutedPlan(query: StreamingQuery): SparkPlan = {
    val streamingQuery = query.getClass.getMethod("streamingQuery").invoke(query)
    streamingQuery
      .getClass
      .getMethod("lastExecution")
      .invoke(streamingQuery)
      .asInstanceOf[QueryExecution]
      .executedPlan
  }

  private def withTopic(
      bootstrapServers: String,
      topic: String,
      partitions: Int = 1)(body: => Unit): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val adminClient = AdminClient.create(props)
    try {
      if (adminClient.listTopics().names().get().contains(topic)) {
        adminClient.deleteTopics(Seq(topic).asJava).all().get()
      }
      adminClient.createTopics(Seq(new NewTopic(topic, partitions, 1.toShort)).asJava).all().get()
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
    produceRecords(
      bootstrapServers,
      offsets.map(
        offset =>
          new ProducerRecord[String, String](
            topic,
            s"key-$offset",
            s"value-$offset")))
  }

  private def produceRecords(
      bootstrapServers: String,
      records: Iterable[ProducerRecord[String, String]]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    try {
      records.foreach(record => producer.send(record).get())
      producer.flush()
    } finally {
      producer.close()
    }
  }

  private def withTempDir(body: File => Unit): Unit = {
    val dir = Files.createTempDirectory("velox-kafka-native-executor-loss").toFile
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

  private def minimalSparkHome(): File = {
    val home = Files.createTempDirectory("gluten-kafka-local-cluster-spark-home").toFile
    Files.createDirectories(new File(home, "jars").toPath)
    Files.createDirectories(new File(home, "conf").toPath)
    Files.createDirectories(new File(home, "launcher/target/scala-2.13").toPath)
    home
  }
}
