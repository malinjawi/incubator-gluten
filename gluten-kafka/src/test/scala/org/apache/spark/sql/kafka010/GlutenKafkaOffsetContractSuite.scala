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
package org.apache.spark.sql.kafka010

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.MicroBatchScanExecTransformer
import org.apache.gluten.execution.kafka.GlutenKafkaOffsetLogHandoffRule
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SparkDataStream, SupportsAdmissionControl}
import org.apache.spark.sql.execution.datasources.v2.{StreamingDataSourceV2Relation, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.runtime.SerializedOffset
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.substrait.proto.ReadRel
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import java.util

import scala.collection.JavaConverters._

class GlutenKafkaOffsetContractSuite extends AnyFunSuite {
  private val NativeStreamingFullKey =
    "spark.gluten.sql.streaming.native.full.enabled"
  private val NativeOffsetPlanningKey =
    "spark.gluten.sql.streaming.native.sources.kafka.nativeOffsetPlanning.enabled"
  private val OffsetLogHandoffKey =
    "spark.gluten.sql.streaming.native.sources.kafka.offsetLogHandoff.enabled"

  test("native Kafka schema contract matches Spark Kafka source schema") {
    assert(
      MicroBatchScanExecTransformer.SparkKafkaReadSchema ==
        KafkaRecordToRowConverter.kafkaSchema(includeHeaders = true))
  }

  test("native Kafka micro-batch split requires finite monotonic offsets") {
    val splitInfo =
      GlutenStreamKafkaSourceUtil.genSplitInfo(
        kafkaInputPartition(fromOffset = 0L, untilOffset = 3L))
    val protobuf = splitInfo.toProtobuf.asInstanceOf[ReadRel.StreamKafka]
    assert(protobuf.getTopicPartition.getTopic == "native-offset-contract")
    assert(protobuf.getTopicPartition.getPartition == 0)
    assert(protobuf.getStartOffset == 0L)
    assert(protobuf.getEndOffset == 3L)
    assert(protobuf.getPollTimeoutMs == 1000L)

    val negativeOffsetError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.genSplitInfo(
        kafkaInputPartition(fromOffset = -1L, untilOffset = 3L))
    }
    assert(negativeOffsetError.getMessage.contains("finite non-negative offsets"))

    val reversedOffsetError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.genSplitInfo(
        kafkaInputPartition(fromOffset = 3L, untilOffset = 2L))
    }
    assert(reversedOffsetError.getMessage.contains("untilOffset >= fromOffset"))
  }

  test("native Kafka offset planning validation is disabled by default") {
    SQLConf.get.setConfString(
      NativeOffsetPlanningKey,
      "false")

    val splitInfo =
      GlutenStreamKafkaSourceUtil.genSplitInfo(
        kafkaInputPartition(fromOffset = 1L, untilOffset = 4L))
    val protobuf = splitInfo.toProtobuf.asInstanceOf[ReadRel.StreamKafka]
    assert(protobuf.getStartOffset == 1L)
    assert(protobuf.getEndOffset == 4L)
  }

  test("native Kafka offset planning validation fails closed without Velox planner") {
    SQLConf.get.setConfString(
      NativeOffsetPlanningKey,
      "true")
    try {
      val error = intercept[GlutenNotSupportException] {
        GlutenStreamKafkaSourceUtil.genSplitInfo(
          kafkaInputPartition(fromOffset = 1L, untilOffset = 4L))
      }
      assert(error.getMessage.contains("Native Kafka offset planning validation"))
    } finally {
      SQLConf.get.setConfString(
        NativeOffsetPlanningKey,
        "false")
    }
  }

  test("native Kafka micro-batch split rejects invalid topic partition and timeout") {
    val emptyTopicError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.genSplitInfo(
        kafkaInputPartition(fromOffset = 0L, untilOffset = 3L, topic = ""))
    }
    assert(emptyTopicError.getMessage.contains("non-empty topic"))

    val negativePartitionError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.genSplitInfo(
        kafkaInputPartition(fromOffset = 0L, untilOffset = 3L, partition = -1))
    }
    assert(negativePartitionError.getMessage.contains("non-negative partition"))

    val negativePollTimeoutError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.genSplitInfo(
        kafkaInputPartition(fromOffset = 0L, untilOffset = 3L, pollTimeoutMs = -1L))
    }
    assert(negativePollTimeoutError.getMessage.contains("non-negative poll timeout"))
  }

  test("native Kafka offset-log handoff manifest mirrors Spark offset logs") {
    val topic = "native-offset-handoff"
    val manifest =
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        kafkaOffset(topic -> (0, 1L)),
        kafkaOffset(topic -> (0, 4L), topic -> (1, 3L)),
        Seq(
          kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L),
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 1L, untilOffset = 4L))
      )

    assert(manifest.sparkEndOffsetJson == kafkaOffset(topic -> (0, 4L), topic -> (1, 3L)).json)
    assert(manifest.partitionRanges.map(range => range.topic -> range.partition) ==
      Seq(topic -> 0, topic -> 1))
    assert(manifest.nativeDiscoveredTopicPartitions.isEmpty)
    assert(formatTopicPartitions(manifest.replayedTopicPartitions) == Seq(topic -> 0))
    assert(formatTopicPartitions(manifest.newlyDiscoveredTopicPartitions) == Seq(topic -> 1))

    val existingPartition = manifest.partitionRanges.find(_.partition == 0).get
    assert(existingPartition.startOffset == 1L)
    assert(existingPartition.endOffset == 4L)
    assert(existingPartition.startOffsetFromSparkLog.contains(1L))
    assert(!existingPartition.discoveredInBatch)

    val discoveredPartition = manifest.partitionRanges.find(_.partition == 1).get
    assert(discoveredPartition.startOffset == 0L)
    assert(discoveredPartition.endOffset == 3L)
    assert(discoveredPartition.startOffsetFromSparkLog.isEmpty)
    assert(discoveredPartition.discoveredInBatch)

    val parsedManifest =
      GlutenStreamKafkaSourceUtil.parseOffsetLogHandoffManifestJson(manifest.persistedJson)
    assert(parsedManifest == manifest)
    assert(parsedManifest.persistedJson == manifest.persistedJson)
    assert(manifest.persistedJson.contains("\"version\":1"))
    assert(manifest.persistedJson.contains("\"offsetLogOwner\":\"spark-offset-log\""))
    assert(manifest.persistedJson.contains("\"nativeOffsetCommitsEnabled\":false"))
    assert(manifest.persistedJson.contains("\"sparkStartOffsetJson\""))
    assert(manifest.persistedJson.contains("\"sparkEndOffsetJson\""))
    assert(manifest.persistedJson.contains("\"partitionRanges\""))
  }

  test("native Kafka offset-log handoff validates native partition discovery witness") {
    val topic = "native-offset-handoff-discovery"
    val topicPartition0 = new TopicPartition(topic, 0)
    val topicPartition1 = new TopicPartition(topic, 1)
    val nativeDiscovered = Set(topicPartition0, topicPartition1)

    val manifest =
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        kafkaOffset(topic -> (0, 2L)),
        kafkaOffset(topic -> (0, 5L), topic -> (1, 3L)),
        Seq(
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
          kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L)),
        Some(nativeDiscovered)
      )

    assert(manifest.nativeDiscoveredTopicPartitions.contains(nativeDiscovered))
    assert(formatTopicPartitions(manifest.replayedTopicPartitions) == Seq(topic -> 0))
    assert(formatTopicPartitions(manifest.newlyDiscoveredTopicPartitions) == Seq(topic -> 1))

    val missingSparkEndLogPartitionError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        kafkaOffset(topic -> (0, 2L)),
        kafkaOffset(topic -> (0, 5L)),
        Seq(kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L)),
        Some(nativeDiscovered)
      )
    }
    assert(
      missingSparkEndLogPartitionError.getMessage.contains(
        "native-discovered partitions absent from Spark end offset log"))

    val missingNativeDiscoveryPartitionError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        kafkaOffset(topic -> (0, 2L)),
        kafkaOffset(topic -> (0, 5L), topic -> (1, 3L)),
        Seq(
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
          kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L)),
        Some(Set(topicPartition0))
      )
    }
    assert(
      missingNativeDiscoveryPartitionError.getMessage.contains(
        "Spark end offset log partitions absent from native discovery"))
  }

  test("full native Kafka offset-log handoff requires native discovery witness") {
    SQLConf.get.setConfString(NativeStreamingFullKey, "true")
    SQLConf.get.setConfString(NativeOffsetPlanningKey, "true")
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    try {
      val topic = "native-offset-handoff-full-native-discovery"
      val start = kafkaOffset(topic -> (0, 2L))
      val end = kafkaOffset(topic -> (0, 5L))
      val partitions = Seq(
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L))

      val missingWitnessError = intercept[GlutenNotSupportException] {
        GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(start, end, partitions)
      }
      assert(
        missingWitnessError.getMessage.contains(
          "requires a native broker-discovery witness"))

      val nativeDiscovered = Some(Set(new TopicPartition(topic, 0)))
      val manifest =
        GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
          start,
          end,
          partitions,
          nativeDiscovered)
      assert(manifest.nativeDiscoveredTopicPartitions == nativeDiscovered)
    } finally {
      SQLConf.get.setConfString(NativeStreamingFullKey, "false")
      SQLConf.get.setConfString(NativeOffsetPlanningKey, "false")
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
    }
  }

  test("native Kafka persisted offset-log handoff manifest validates replay contract") {
    val topic = "native-offset-handoff-replay"
    val start = kafkaOffset(topic -> (0, 2L))
    val end = kafkaOffset(topic -> (0, 5L), topic -> (1, 3L))
    val partitions = Seq(
      kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
      kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L))
    val nativeDiscovered = Some(Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))
    val manifest =
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        start,
        end,
        partitions,
        nativeDiscovered)

    val replayed =
      GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffReplay(
        manifest.persistedJson,
        start,
        end,
        partitions,
        nativeDiscovered)
    assert(replayed == manifest)

    val nativeCommitError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.parseOffsetLogHandoffManifestJson(
        manifest.persistedJson.replace(
          "\"nativeOffsetCommitsEnabled\":false",
          "\"nativeOffsetCommitsEnabled\":true"))
    }
    assert(nativeCommitError.getMessage.contains("native offset commits must stay disabled"))

    val persistedOffsetShapeError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.parseOffsetLogHandoffManifestJson(
        manifest.persistedJson.replace(
          s""""startPartitionOffsets":[{"topic":"$topic","partition":0,"offset":2}]""",
          s""""startPartitionOffsets":[{"topic":"$topic","partition":0,"offset":1}]"""
        ))
    }
    assert(
      persistedOffsetShapeError.getMessage.contains(
        "startPartitionOffsets disagree with sparkStartOffsetJson"))

    val replayEndDriftError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffReplay(
        manifest.persistedJson,
        start,
        kafkaOffset(topic -> (0, 6L), topic -> (1, 3L)),
        Seq(
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 6L),
          kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L)),
        nativeDiscovered
      )
    }
    assert(replayEndDriftError.getMessage.contains("replay end offset log drift"))

    val replayPartitionDriftError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffReplay(
        manifest.persistedJson,
        start,
        end,
        Seq(
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
          kafkaInputPartition(topic = topic, partition = 1, fromOffset = 1L, untilOffset = 3L)),
        nativeDiscovered
      )
    }
    assert(replayPartitionDriftError.getMessage.contains("replay partition range drift"))
  }

  test("native Kafka offset-log handoff persists manifest and fails closed on replay drift") {
    val metadataPath = Files.createTempDirectory("gluten-kafka-offset-handoff").toFile
    try {
      val topic = "native-offset-handoff-persisted"
      val start = kafkaOffset(topic -> (0, 2L))
      val end = kafkaOffset(topic -> (0, 5L), topic -> (1, 3L))
      val partitions = Seq(
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L))
      val nativeDiscovered =
        Some(Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))
      val manifest =
        GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
          start,
          end,
          partitions,
          nativeDiscovered)

      val firstReplay = GlutenStreamKafkaSourceUtil.prepareOffsetLogHandoffReplay(
        metadataPath.getCanonicalPath,
        manifest)
      assert(!firstReplay.replayedExistingManifest)
      assert(firstReplay.manifest == manifest)
      val path = firstReplay.manifestPath
      assert(path.getName.endsWith(".json"))
      assert(path.toString.contains("gluten-native-kafka-offset-handoff"))
      assert(
        GlutenStreamKafkaSourceUtil.readPersistedOffsetLogHandoffManifest(
          metadataPath.getCanonicalPath,
          manifest) == manifest)
      assert(
        GlutenStreamKafkaSourceUtil.readPersistedOffsetLogHandoffManifestForEndOffset(
          metadataPath.getCanonicalPath,
          end) == manifest)
      val loadedReplay =
        GlutenStreamKafkaSourceUtil.loadPersistedOffsetLogHandoffManifestForEndOffset(
          metadataPath.getCanonicalPath,
          end)
      assert(loadedReplay.exists(_.replayedExistingManifest))
      assert(loadedReplay.map(_.manifest).contains(manifest))
      assert(
        GlutenStreamKafkaSourceUtil.validatePersistedOffsetLogHandoffManifest(
          metadataPath.getCanonicalPath,
          start,
          end,
          partitions,
          nativeDiscovered) == manifest)
      val secondReplay = GlutenStreamKafkaSourceUtil.prepareOffsetLogHandoffReplay(
        metadataPath.getCanonicalPath,
        manifest)
      assert(secondReplay.replayedExistingManifest)
      assert(secondReplay.manifestPath == path)
      assert(secondReplay.manifest == manifest)

      val driftError = intercept[GlutenNotSupportException] {
        val driftedManifest = GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
          kafkaOffset(topic -> (0, 1L)),
          end,
          Seq(
            kafkaInputPartition(topic = topic, partition = 0, fromOffset = 1L, untilOffset = 5L),
            kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L)),
          nativeDiscovered
        )
        GlutenStreamKafkaSourceUtil.prepareOffsetLogHandoffReplay(
          metadataPath.getCanonicalPath,
          driftedManifest)
      }
      assert(driftError.getMessage.contains("replay start offset log drift"))
    } finally {
      org.apache.spark.util.Utils.deleteRecursively(metadataPath)
    }
  }

  test("native Kafka stream wrapper enforces persisted handoff replay witness") {
    val metadataPath = Files.createTempDirectory("gluten-kafka-stream-wrapper").toFile
    try {
      val topic = "native-offset-handoff-wrapper"
      val start = kafkaOffset(topic -> (0, 2L))
      val end = kafkaOffset(topic -> (0, 5L), topic -> (1, 3L))
      val partitions = Array[InputPartition](
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L))
      val nativeDiscovered =
        Some(Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))
      val delegate = new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, partitions)
      var discoveryCalls = 0
      val wrapper =
        new GlutenNativeKafkaMicroBatchStream(
          delegate,
          Some {
            (_, discoveredEnd) =>
              discoveryCalls += 1
              assert(discoveredEnd == end)
              assert(delegate.planCalls == 0)
              nativeDiscovered
          })

      assert(
        wrapper
          .planInputPartitions(SerializedOffset(start.json), SerializedOffset(end.json))
          .toSeq == partitions.toSeq)
      assert(discoveryCalls == 1)
      assert(delegate.planCalls == 1)
      assert(delegate.lastStart == start)
      assert(delegate.lastEnd == end)
      assert(
        GlutenStreamKafkaSourceUtil
          .loadPersistedOffsetLogHandoffManifestForEndOffset(metadataPath.getCanonicalPath, end)
          .exists(_.replayedExistingManifest))

      assert(wrapper.planInputPartitions(start, end).toSeq == partitions.toSeq)
      assert(delegate.planCalls == 1)

      delegate.plannedPartitions = Array[InputPartition](
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = topic, partition = 1, fromOffset = 1L, untilOffset = 3L))
      assert(wrapper.planInputPartitions(start, end).toSeq == partitions.toSeq)
      assert(delegate.planCalls == 1)
    } finally {
      org.apache.spark.util.Utils.deleteRecursively(metadataPath)
    }
  }

  test("native Kafka runtime offset-log write hook prepares and reuses planned partitions") {
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    val metadataPath = Files.createTempDirectory("gluten-kafka-runtime-write-hook").toFile
    try {
      val topic = "native-offset-handoff-runtime-write"
      val start = kafkaOffset(topic -> (0, 2L))
      val end = kafkaOffset(topic -> (0, 5L), topic -> (1, 3L))
      val partitions = Array[InputPartition](
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L))
      val nativeDiscovered =
        Some(Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))
      val delegate = new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, partitions)
      val wrapper =
        new GlutenNativeKafkaMicroBatchStream(delegate, Some((_, _) => nativeDiscovered))
      val sources = Seq[SparkDataStream](wrapper)
      val startOffsets = Map[SparkDataStream, Offset](wrapper -> start)
      val endOffsets = Map[SparkDataStream, Offset](wrapper -> end)

      GlutenStreamKafkaSourceUtil.prepareOffsetLogHandoffBeforeSparkOffsetLogAdd(
        sources,
        startOffsets,
        endOffsets)
      assert(delegate.planCalls == 1)
      assert(
        GlutenStreamKafkaSourceUtil
          .loadPersistedOffsetLogHandoffManifestForEndOffset(metadataPath.getCanonicalPath, end)
          .exists(_.replayedExistingManifest))

      delegate.plannedPartitions = Array[InputPartition](
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = topic, partition = 1, fromOffset = 1L, untilOffset = 3L))
      assert(wrapper.planInputPartitions(start, end).toSeq == partitions.toSeq)
      assert(delegate.planCalls == 1)
    } finally {
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
      org.apache.spark.util.Utils.deleteRecursively(metadataPath)
    }
  }

  test("native Kafka stream wrapper prepares handoff during latestOffset before Spark WAL") {
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    val metadataPath = Files.createTempDirectory("gluten-kafka-pre-wal-latest-offset").toFile
    try {
      val topic = "native-offset-handoff-pre-wal"
      val start = kafkaOffset(topic -> (0, 2L))
      val end = kafkaOffset(topic -> (0, 5L), topic -> (1, 3L))
      val partitions = Array[InputPartition](
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L))
      val nativeDiscovered =
        Some(Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))
      val delegate =
        new FakeAdmissionKafkaMicroBatchStream(metadataPath.getCanonicalPath, partitions, end)
      val wrapper =
        new GlutenNativeKafkaMicroBatchStream(delegate, Some((_, _) => nativeDiscovered))

      assert(wrapper.latestOffset(start, ReadLimit.allAvailable()) == end)
      assert(delegate.planCalls == 1)
      assert(
        GlutenStreamKafkaSourceUtil
          .loadPersistedOffsetLogHandoffManifestForEndOffset(metadataPath.getCanonicalPath, end)
          .exists(_.replayedExistingManifest))

      delegate.plannedPartitions = Array[InputPartition](
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = topic, partition = 1, fromOffset = 1L, untilOffset = 3L))
      assert(wrapper.planInputPartitions(start, end).toSeq == partitions.toSeq)
      assert(delegate.planCalls == 1)
    } finally {
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
      org.apache.spark.util.Utils.deleteRecursively(metadataPath)
    }
  }

  test("native Kafka runtime offset-log replay hook requires and validates manifest") {
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    val metadataPath = Files.createTempDirectory("gluten-kafka-runtime-replay-hook").toFile
    try {
      val topic = "native-offset-handoff-runtime-replay"
      val start = kafkaOffset(topic -> (0, 2L))
      val end = kafkaOffset(topic -> (0, 5L), topic -> (1, 3L))
      val partitions = Array[InputPartition](
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L))
      val nativeDiscovered =
        Some(Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))
      val missingDelegate =
        new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, partitions)
      val missingWrapper =
        new GlutenNativeKafkaMicroBatchStream(missingDelegate, Some((_, _) => nativeDiscovered))
      val missingError = intercept[GlutenNotSupportException] {
        GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffAfterSparkOffsetLogReplay(
          Seq[SparkDataStream](missingWrapper),
          Map[SparkDataStream, Offset](missingWrapper -> start),
          Map[SparkDataStream, Offset](missingWrapper -> end))
      }
      assert(missingError.getMessage.contains("manifest is missing"))

      val writeDelegate =
        new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, partitions)
      val writeWrapper =
        new GlutenNativeKafkaMicroBatchStream(writeDelegate, Some((_, _) => nativeDiscovered))
      GlutenStreamKafkaSourceUtil.prepareOffsetLogHandoffBeforeSparkOffsetLogAdd(
        Seq[SparkDataStream](writeWrapper),
        Map[SparkDataStream, Offset](writeWrapper -> start),
        Map[SparkDataStream, Offset](writeWrapper -> end))

      val replayDelegate =
        new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, partitions)
      val replayWrapper =
        new GlutenNativeKafkaMicroBatchStream(replayDelegate, Some((_, _) => nativeDiscovered))
      GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffAfterSparkOffsetLogReplay(
        Seq[SparkDataStream](replayWrapper),
        Map[SparkDataStream, Offset](replayWrapper -> start),
        Map[SparkDataStream, Offset](replayWrapper -> end))
      assert(replayDelegate.planCalls == 1)

      val noOpReplayDelegate =
        new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, Array.empty[InputPartition])
      val noOpReplayWrapper =
        new GlutenNativeKafkaMicroBatchStream(
          noOpReplayDelegate,
          Some((_, _) => Some(Set.empty[TopicPartition])))
      GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffAfterSparkOffsetLogReplay(
        Seq[SparkDataStream](noOpReplayWrapper),
        Map[SparkDataStream, Offset](noOpReplayWrapper -> end),
        Map[SparkDataStream, Offset](noOpReplayWrapper -> end))
      assert(noOpReplayDelegate.planCalls == 1)

      val driftDelegate =
        new FakeKafkaMicroBatchStream(
          metadataPath.getCanonicalPath,
          Array[InputPartition](
            kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
            kafkaInputPartition(topic = topic, partition = 1, fromOffset = 1L, untilOffset = 3L))
        )
      val driftWrapper =
        new GlutenNativeKafkaMicroBatchStream(driftDelegate, Some((_, _) => nativeDiscovered))
      val driftError = intercept[GlutenNotSupportException] {
        GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffAfterSparkOffsetLogReplay(
          Seq[SparkDataStream](driftWrapper),
          Map[SparkDataStream, Offset](driftWrapper -> start),
          Map[SparkDataStream, Offset](driftWrapper -> end))
      }
      assert(driftError.getMessage.contains("replay partition range drift"))
    } finally {
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
      org.apache.spark.util.Utils.deleteRecursively(metadataPath)
    }
  }

  test("native Kafka replay reuses persisted native discovery witness") {
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    val metadataPath =
      Files.createTempDirectory("gluten-kafka-runtime-replay-native-witness").toFile
    try {
      val topic = "native-offset-handoff-runtime-replay-witness"
      val start = kafkaOffset(topic -> (0, 2L))
      val end = kafkaOffset(topic -> (0, 5L), topic -> (1, 3L))
      val partitions = Array[InputPartition](
        kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = topic, partition = 1, fromOffset = 0L, untilOffset = 3L))
      val persistedDiscovery =
        Some(Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1)))
      val writeDelegate =
        new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, partitions)
      val writeWrapper =
        new GlutenNativeKafkaMicroBatchStream(writeDelegate, Some((_, _) => persistedDiscovery))

      GlutenStreamKafkaSourceUtil.prepareOffsetLogHandoffBeforeSparkOffsetLogAdd(
        Seq[SparkDataStream](writeWrapper),
        Map[SparkDataStream, Offset](writeWrapper -> start),
        Map[SparkDataStream, Offset](writeWrapper -> end))

      val replayDelegate =
        new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, partitions)
      var liveDiscoveryCalls = 0
      val replayWrapper =
        new GlutenNativeKafkaMicroBatchStream(
          replayDelegate,
          Some(
            (_: MicroBatchStream, _: Offset) => {
              liveDiscoveryCalls += 1
              Some(
                Set(
                  new TopicPartition(topic, 0),
                  new TopicPartition(topic, 1),
                  new TopicPartition(s"$topic-evolved", 0)))
            })
        )

      GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffAfterSparkOffsetLogReplay(
        Seq[SparkDataStream](replayWrapper),
        Map[SparkDataStream, Offset](replayWrapper -> start),
        Map[SparkDataStream, Offset](replayWrapper -> end))
      assert(replayDelegate.planCalls == 1)
      assert(liveDiscoveryCalls == 0)
    } finally {
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
      org.apache.spark.util.Utils.deleteRecursively(metadataPath)
    }
  }

  test("native Kafka persisted pattern witness rejects topic evolution absent from discovery") {
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    val metadataPath = Files.createTempDirectory("gluten-kafka-pattern-replay-witness").toFile
    try {
      val baseTopic = "native-offset-handoff-pattern-replay-a"
      val evolvedTopic = "native-offset-handoff-pattern-replay-b"
      val baseStart = kafkaOffset(baseTopic -> (0, 2L))
      val baseEnd = kafkaOffset(baseTopic -> (0, 5L))
      val basePartitions = Array[InputPartition](
        kafkaInputPartition(topic = baseTopic, partition = 0, fromOffset = 2L, untilOffset = 5L))
      val persistedDiscovery = Some(Set(new TopicPartition(baseTopic, 0)))
      val writeDelegate =
        new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, basePartitions)
      val writeWrapper =
        new GlutenNativeKafkaMicroBatchStream(writeDelegate, Some((_, _) => persistedDiscovery))

      GlutenStreamKafkaSourceUtil.prepareOffsetLogHandoffBeforeSparkOffsetLogAdd(
        Seq[SparkDataStream](writeWrapper),
        Map[SparkDataStream, Offset](writeWrapper -> baseStart),
        Map[SparkDataStream, Offset](writeWrapper -> baseEnd))

      val persistedManifest =
        GlutenStreamKafkaSourceUtil.readPersistedOffsetLogHandoffManifestForEndOffset(
          metadataPath.getCanonicalPath,
          baseEnd)
      val evolvedEnd = kafkaOffset(baseTopic -> (0, 5L), evolvedTopic -> (0, 3L))
      val evolvedPartitions = Array[InputPartition](
        kafkaInputPartition(topic = baseTopic, partition = 0, fromOffset = 2L, untilOffset = 5L),
        kafkaInputPartition(topic = evolvedTopic, partition = 0, fromOffset = 0L, untilOffset = 3L)
      )

      val staleWitnessError = intercept[GlutenNotSupportException] {
        GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffReplay(
          persistedManifest.persistedJson,
          baseStart,
          evolvedEnd,
          evolvedPartitions,
          persistedDiscovery)
      }
      assert(
        staleWitnessError.getMessage.contains(
          "Spark end offset log partitions absent from native discovery"))
      assert(staleWitnessError.getMessage.contains(evolvedTopic))

      val replayDelegate =
        new FakeKafkaMicroBatchStream(metadataPath.getCanonicalPath, evolvedPartitions)
      var liveDiscoveryCalls = 0
      val replayWrapper =
        new GlutenNativeKafkaMicroBatchStream(
          replayDelegate,
          Some(
            (_, _) => {
              liveDiscoveryCalls += 1
              Some(Set(new TopicPartition(baseTopic, 0), new TopicPartition(evolvedTopic, 0)))
            }))
      val replayError = intercept[GlutenNotSupportException] {
        GlutenStreamKafkaSourceUtil.validateOffsetLogHandoffAfterSparkOffsetLogReplay(
          Seq[SparkDataStream](replayWrapper),
          Map[SparkDataStream, Offset](replayWrapper -> baseStart),
          Map[SparkDataStream, Offset](replayWrapper -> evolvedEnd))
      }
      assert(replayError.getMessage.contains("manifest is missing"))
      assert(liveDiscoveryCalls == 0)
      assert(replayDelegate.planCalls == 0)
    } finally {
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
      org.apache.spark.util.Utils.deleteRecursively(metadataPath)
    }
  }

  test("native Kafka offset-log handoff rule wraps concrete Kafka streaming scan relation") {
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    try {
      val topic = "native-offset-handoff-logical-rule"
      val metadataPath = s"/tmp/$topic"
      val options = new CaseInsensitiveStringMap(
        Map(
          "kafka.bootstrap.servers" -> "localhost:9092",
          "subscribe" -> topic).asJava)
      val table = new KafkaSourceProvider().getTable(options)
      val scan = table.asInstanceOf[SupportsRead].newScanBuilder(options).build()
      val output = scan.readSchema().fields.map {
        field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
      }.toSeq
      val delegate =
        new FakeKafkaMicroBatchStream(metadataPath, Array.empty[InputPartition])
      val relation = StreamingDataSourceV2Relation(
        table,
        output,
        None,
        None,
        options,
        metadataPath,
        None)
      val scanRelation = StreamingDataSourceV2ScanRelation(
        relation,
        scan,
        output,
        delegate,
        Some(kafkaOffset(topic -> (0, 1L))),
        Some(kafkaOffset(topic -> (0, 2L))))

      val rewritten = GlutenKafkaOffsetLogHandoffRule()
        .apply(scanRelation)
        .asInstanceOf[StreamingDataSourceV2ScanRelation]
      assert(rewritten.stream.isInstanceOf[GlutenNativeKafkaMicroBatchStream])
      assert(rewritten.startOffset == scanRelation.startOffset)
      assert(rewritten.endOffset == scanRelation.endOffset)

      val noOffsetRelation = scanRelation.copy(startOffset = None, endOffset = None)
      assert(GlutenKafkaOffsetLogHandoffRule().apply(noOffsetRelation) eq noOffsetRelation)
    } finally {
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
    }
  }

  test("native Kafka concrete scan-relation handoff preserves subscribePattern guard") {
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    try {
      val options = new CaseInsensitiveStringMap(
        Map(
          "kafka.BOOTSTRAP.SERVERS" -> "localhost:9092",
          "subscribePattern" -> "native-offset-handoff-concrete-pattern-.*").asJava)
      val table = new KafkaSourceProvider().getTable(options)
      val scan = table.asInstanceOf[SupportsRead].newScanBuilder(options).build()
      val output = scan.readSchema().fields.map {
        field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
      }.toSeq
      val delegate =
        new FakeKafkaMicroBatchStream("/tmp/native-offset-handoff-concrete-pattern", Array.empty)
      val relation = StreamingDataSourceV2Relation(
        table,
        output,
        None,
        None,
        options,
        "/tmp/native-offset-handoff-concrete-pattern",
        None)
      val scanRelation = StreamingDataSourceV2ScanRelation(
        relation,
        scan,
        output,
        delegate,
        Some(kafkaOffset("native-offset-handoff-concrete-pattern-a" -> (0, 1L))),
        Some(kafkaOffset("native-offset-handoff-concrete-pattern-a" -> (0, 2L)))
      )

      val error = intercept[GlutenNotSupportException] {
        GlutenKafkaOffsetLogHandoffRule().apply(scanRelation)
      }
      assert(error.getMessage.contains("subscribePattern offset-log handoff requires native Kafka"))
      assert(error.getMessage.contains("regex topic discovery is native-owned"))

      SQLConf.get.setConfString(NativeOffsetPlanningKey, "true")
      val missingPlannerError = intercept[GlutenNotSupportException] {
        GlutenKafkaOffsetLogHandoffRule().apply(scanRelation)
      }
      assert(missingPlannerError.getMessage.contains("pattern partition discovery requires"))
      assert(missingPlannerError.getMessage.contains("Velox planner"))
    } finally {
      SQLConf.get.setConfString(NativeOffsetPlanningKey, "false")
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
    }
  }

  test("native Kafka offset-log handoff rule wraps Kafka streaming table before source creation") {
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    try {
      val topic = "native-offset-handoff-table-rule"
      val options = new CaseInsensitiveStringMap(
        Map(
          "kafka.bootstrap.servers" -> "localhost:9092",
          "subscribe" -> topic).asJava)
      val table = new KafkaSourceProvider().getTable(options)
      val scan = table.asInstanceOf[SupportsRead].newScanBuilder(options).build()
      val output = scan.readSchema().fields.map {
        field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
      }.toSeq
      val relation = StreamingRelationV2(
        None,
        "kafka",
        table,
        options,
        output,
        None,
        None,
        None)

      val rewritten = GlutenKafkaOffsetLogHandoffRule()
        .apply(relation)
        .asInstanceOf[StreamingRelationV2]
      assert(rewritten.table ne table)

      val wrappedScan = rewritten.table
        .asInstanceOf[SupportsRead]
        .newScanBuilder(options)
        .build()
      assert(wrappedScan.getClass.getName.contains("GlutenKafkaOffsetLogHandoffScan"))
      assert(MicroBatchScanExecTransformer.supportsBatchScan(wrappedScan))
      assert(
        GlutenStreamKafkaSourceUtil.getFileFormat(wrappedScan) == ReadFileFormat.KafkaReadFormat)

      val rewrittenAgain = GlutenKafkaOffsetLogHandoffRule()
        .apply(rewritten)
        .asInstanceOf[StreamingRelationV2]
      assert(rewrittenAgain.table eq rewritten.table)
    } finally {
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
    }
  }

  test("native Kafka offset-log handoff requires native subscribePattern discovery") {
    SQLConf.get.setConfString(OffsetLogHandoffKey, "true")
    try {
      val options = new CaseInsensitiveStringMap(
        Map(
          "kafka.BOOTSTRAP.SERVERS" -> "localhost:9092",
          "subscribePattern" -> "native-offset-handoff-pattern-.*").asJava)
      val table = new KafkaSourceProvider().getTable(options)
      val scan = table.asInstanceOf[SupportsRead].newScanBuilder(options).build()
      val output = scan.readSchema().fields.map {
        field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
      }.toSeq
      val relation = StreamingRelationV2(
        None,
        "kafka",
        table,
        options,
        output,
        None,
        None,
        None)

      val rewritten = GlutenKafkaOffsetLogHandoffRule()
        .apply(relation)
        .asInstanceOf[StreamingRelationV2]
      val error = intercept[GlutenNotSupportException] {
        rewritten.table
          .asInstanceOf[SupportsRead]
          .newScanBuilder(options)
          .build()
      }
      assert(error.getMessage.contains("subscribePattern offset-log handoff requires native Kafka"))
      assert(error.getMessage.contains("regex topic discovery is native-owned"))

      SQLConf.get.setConfString(NativeOffsetPlanningKey, "true")
      val missingPlannerError = intercept[GlutenNotSupportException] {
        rewritten.table
          .asInstanceOf[SupportsRead]
          .newScanBuilder(options)
          .build()
      }
      assert(missingPlannerError.getMessage.contains("pattern partition discovery requires"))
      assert(missingPlannerError.getMessage.contains("Velox planner"))
    } finally {
      SQLConf.get.setConfString(NativeOffsetPlanningKey, "false")
      SQLConf.get.setConfString(OffsetLogHandoffKey, "false")
    }
  }

  test("native Kafka offset-log handoff manifest rejects partition and offset drift") {
    val topic = "native-offset-handoff-drift"
    val start = kafkaOffset(topic -> (0, 1L), topic -> (1, 2L))
    val end = kafkaOffset(topic -> (0, 4L), topic -> (1, 5L))

    val missingPartitionError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        start,
        end,
        Seq(kafkaInputPartition(topic = topic, partition = 0, fromOffset = 1L, untilOffset = 4L)))
    }
    assert(missingPartitionError.getMessage.contains("did not plan partitions"))

    val noOpPartitionOmitted = GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
      kafkaOffset(topic -> (0, 1L), topic -> (1, 2L)),
      kafkaOffset(topic -> (0, 4L), topic -> (1, 2L)),
      Seq(kafkaInputPartition(topic = topic, partition = 0, fromOffset = 1L, untilOffset = 4L))
    )
    assert(noOpPartitionOmitted.partitionRanges.map(_.partition) == Seq(0))
    val parsedNoOpPartitionOmitted =
      GlutenStreamKafkaSourceUtil.parseOffsetLogHandoffManifestJson(
        noOpPartitionOmitted.persistedJson)
    assert(parsedNoOpPartitionOmitted == noOpPartitionOmitted)

    val duplicatePartitionError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        kafkaOffset(topic -> (0, 1L)),
        kafkaOffset(topic -> (0, 4L)),
        Seq(
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 1L, untilOffset = 3L),
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 3L, untilOffset = 4L))
      )
    }
    assert(duplicatePartitionError.getMessage.contains("duplicate ranges"))

    val unexpectedPartitionError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        kafkaOffset(topic -> (0, 1L)),
        kafkaOffset(topic -> (0, 4L)),
        Seq(
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 1L, untilOffset = 4L),
          kafkaInputPartition(topic = topic, partition = 2, fromOffset = 0L, untilOffset = 1L))
      )
    }
    assert(unexpectedPartitionError.getMessage.contains("absent from Spark end offset log"))

    val removedPartitionError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        kafkaOffset(topic -> (0, 1L), topic -> (2, 2L)),
        kafkaOffset(topic -> (0, 4L)),
        Seq(kafkaInputPartition(topic = topic, partition = 0, fromOffset = 1L, untilOffset = 4L)))
    }
    assert(removedPartitionError.getMessage.contains("missing from Spark end offset log"))

    val startDriftError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        start,
        end,
        Seq(
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 2L, untilOffset = 4L),
          kafkaInputPartition(topic = topic, partition = 1, fromOffset = 2L, untilOffset = 5L))
      )
    }
    assert(startDriftError.getMessage.contains("start offset drift"))

    val endDriftError = intercept[GlutenNotSupportException] {
      GlutenStreamKafkaSourceUtil.buildOffsetLogHandoffManifest(
        start,
        end,
        Seq(
          kafkaInputPartition(topic = topic, partition = 0, fromOffset = 1L, untilOffset = 6L),
          kafkaInputPartition(topic = topic, partition = 1, fromOffset = 2L, untilOffset = 5L))
      )
    }
    assert(endDriftError.getMessage.contains("end offset drift"))
  }

  private def kafkaInputPartition(
      fromOffset: Long,
      untilOffset: Long,
      topic: String = "native-offset-contract",
      partition: Int = 0,
      pollTimeoutMs: Long = 1000L): KafkaBatchInputPartition = {
    KafkaBatchInputPartition(
      KafkaOffsetRange(new TopicPartition(topic, partition), fromOffset, untilOffset),
      new util.HashMap[String, Object](),
      pollTimeoutMs,
      failOnDataLoss = true,
      includeHeaders = false
    )
  }

  private def kafkaOffset(offsets: (String, (Int, Long))*): KafkaSourceOffset = {
    KafkaSourceOffset(offsets.map {
      case (topic, (partition, offset)) =>
        new TopicPartition(topic, partition) -> offset
    }.toMap)
  }

  private def formatTopicPartitions(topicPartitions: Seq[TopicPartition]): Seq[(String, Int)] = {
    topicPartitions.map(topicPartition => topicPartition.topic() -> topicPartition.partition())
  }

  private class FakeKafkaMicroBatchStream(
      private val metadataPath: String,
      var plannedPartitions: Array[InputPartition])
    extends MicroBatchStream {
    var planCalls: Int = 0
    var lastStart: Offset = _
    var lastEnd: Offset = _

    override def initialOffset(): Offset = KafkaSourceOffset(Map.empty[TopicPartition, Long])

    override def latestOffset(): Offset = KafkaSourceOffset(Map.empty[TopicPartition, Long])

    override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
      planCalls += 1
      lastStart = start
      lastEnd = end
      plannedPartitions
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      new PartitionReaderFactory {
        override def createReader(partition: InputPartition): PartitionReader[InternalRow] = null
      }
    }

    override def deserializeOffset(json: String): Offset =
      KafkaSourceOffset(SerializedOffset(json))

    override def commit(end: Offset): Unit = {}

    override def stop(): Unit = {}
  }

  private class FakeAdmissionKafkaMicroBatchStream(
      private val metadataPath: String,
      plannedPartitions: Array[InputPartition],
      var nextOffset: Offset)
    extends FakeKafkaMicroBatchStream(metadataPath, plannedPartitions)
    with SupportsAdmissionControl {

    override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = nextOffset

    override def getDefaultReadLimit(): ReadLimit = ReadLimit.allAvailable()

    override def reportLatestOffset(): Offset = nextOffset
  }
}
