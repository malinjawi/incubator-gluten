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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.substrait.rel.{SplitInfo, StreamKafkaSourceBuilder}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, SparkDataStream}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.common.TopicPartition

import java.lang.reflect.Field
import java.lang.reflect.InvocationTargetException
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{ArrayList => JArrayList, LinkedHashMap => JLinkedHashMap, Locale, Map => JMap}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object GlutenStreamKafkaSourceUtil {
  private val NativeKafkaOffsetPlannerClassName =
    "org.apache.spark.sql.kafka010.VeloxNativeKafkaOffsetPlanner"
  private val KafkaScanClassName = "org.apache.spark.sql.kafka010.KafkaSourceProvider$KafkaScan"
  private val KafkaOffsetLogHandoffScanClassName =
    "org.apache.gluten.execution.kafka.GlutenKafkaOffsetLogHandoffScan"
  private val OffsetLogHandoffManifestVersion = 1
  private val OffsetLogHandoffManifestSource = "kafka"
  private val OffsetLogHandoffOwner = "spark-offset-log"
  private val OffsetLogHandoffDirName = "gluten-native-kafka-offset-handoff"
  private val NativeOffsetCommitsEnabled = false
  private val KafkaOptionPrefix = "kafka."
  private val BootstrapServersOption = "bootstrap.servers"
  private val JsonMapper = new ObjectMapper()
  private val OffsetLogHandoffWriteHookInvocations = new AtomicLong(0)
  private val OffsetLogHandoffReplayHookInvocations = new AtomicLong(0)

  case class OffsetLogHandoffPartition(
      topic: String,
      partition: Int,
      startOffset: Long,
      endOffset: Long,
      startOffsetFromSparkLog: Option[Long],
      discoveredInBatch: Boolean)

  case class OffsetLogHandoffManifest(
      startPartitionOffsets: Map[TopicPartition, Long],
      endPartitionOffsets: Map[TopicPartition, Long],
      partitionRanges: Seq[OffsetLogHandoffPartition],
      nativeDiscoveredTopicPartitions: Option[Set[TopicPartition]]) {
    def sparkStartOffsetJson: String = KafkaSourceOffset(startPartitionOffsets).json
    def sparkEndOffsetJson: String = KafkaSourceOffset(endPartitionOffsets).json
    def persistedJson: String = offsetLogHandoffManifestJson(this)

    def replayedTopicPartitions: Seq[TopicPartition] =
      sortTopicPartitions(startPartitionOffsets.keySet.intersect(endPartitionOffsets.keySet))

    def newlyDiscoveredTopicPartitions: Seq[TopicPartition] =
      sortTopicPartitions(endPartitionOffsets.keySet.diff(startPartitionOffsets.keySet))
  }

  case class OffsetLogHandoffReplay(
      metadataPath: String,
      manifestPath: Path,
      manifest: OffsetLogHandoffManifest,
      replayedExistingManifest: Boolean)

  def resetOffsetLogHandoffRuntimeHookCounters(): Unit = {
    OffsetLogHandoffWriteHookInvocations.set(0)
    OffsetLogHandoffReplayHookInvocations.set(0)
  }

  def offsetLogHandoffWriteHookInvocations: Long =
    OffsetLogHandoffWriteHookInvocations.get()

  def offsetLogHandoffReplayHookInvocations: Long =
    OffsetLogHandoffReplayHookInvocations.get()

  def planInputPartitions(
      stream: MicroBatchStream,
      start: Offset,
      end: Offset): Seq[InputPartition] = {
    stream match {
      case native: GlutenNativeKafkaMicroBatchStream =>
        native.planInputPartitions(start, end).toSeq
      case other =>
        val partitions = other.planInputPartitions(start, end)
        validateOffsetLogHandoffIfEnabled(other, start, end, partitions)
        partitions.toSeq
    }
  }

  def wrapKafkaMicroBatchStreamIfNeeded(stream: MicroBatchStream): MicroBatchStream = {
    wrapKafkaMicroBatchStreamIfNeeded(stream, None)
  }

  def wrapKafkaMicroBatchStreamIfNeeded(
      stream: MicroBatchStream,
      subscribePattern: Option[String]): MicroBatchStream = {
    if (!GlutenConfig.get.enableNativeStreamingKafkaSourceOffsetLogHandoff) {
      return stream
    }
    stream match {
      case native: GlutenNativeKafkaMicroBatchStream => native
      case other =>
        val nativeDiscoveryOverride = subscribePattern.map {
          pattern => (delegate: MicroBatchStream, _: Offset) =>
            discoverNativeTopicPartitionsForPatternForHandoff(
              pattern,
              kafkaExecutorParams(delegate))
        }
        new GlutenNativeKafkaMicroBatchStream(other, nativeDiscoveryOverride)
    }
  }

  def prepareOffsetLogHandoffBeforeSparkOffsetLogAdd(
      sources: Seq[SparkDataStream],
      startOffsets: scala.collection.Map[SparkDataStream, Offset],
      endOffsets: scala.collection.Map[SparkDataStream, Offset]): Unit = {
    if (!GlutenConfig.get.enableNativeStreamingKafkaSourceOffsetLogHandoff) {
      return
    }
    var handledStreams = 0
    sources.foreach {
      case stream: GlutenNativeKafkaMicroBatchStream =>
        endOffsets.get(stream).foreach {
          end =>
            stream.prepareOffsetLogHandoffForOffsetLogWrite(
              startOffsets.getOrElse(stream, stream.initialOffset()),
              end)
            handledStreams += 1
        }
      case _ =>
    }
    if (handledStreams > 0) {
      OffsetLogHandoffWriteHookInvocations.addAndGet(handledStreams)
    }
  }

  def validateOffsetLogHandoffAfterSparkOffsetLogReplay(
      sources: Seq[SparkDataStream],
      startOffsets: scala.collection.Map[SparkDataStream, Offset],
      endOffsets: scala.collection.Map[SparkDataStream, Offset]): Unit = {
    if (!GlutenConfig.get.enableNativeStreamingKafkaSourceOffsetLogHandoff) {
      return
    }
    var handledStreams = 0
    sources.foreach {
      case stream: GlutenNativeKafkaMicroBatchStream =>
        endOffsets.get(stream).foreach {
          end =>
            stream.validateOffsetLogHandoffForOffsetLogReplay(
              startOffsets.getOrElse(stream, stream.initialOffset()),
              end)
            handledStreams += 1
        }
      case _ =>
    }
    if (handledStreams > 0) {
      OffsetLogHandoffReplayHookInvocations.addAndGet(handledStreams)
    }
  }

  def genSplitInfo(inputPartition: InputPartition): SplitInfo = inputPartition match {
    case batch: KafkaBatchInputPartition =>
      validateKafkaBatchInputPartition(batch)
      validateNativeOffsetPlanningIfEnabled(batch)
      StreamKafkaSourceBuilder.makeStreamKafkaBatch(
        batch.offsetRange.topicPartition.topic(),
        batch.offsetRange.topicPartition.partition(),
        batch.offsetRange.fromOffset,
        batch.offsetRange.untilOffset,
        batch.pollTimeoutMs,
        batch.failOnDataLoss,
        batch.includeHeaders,
        batch.executorKafkaParams
      )
    case _ =>
      throw new UnsupportedOperationException("Only support kafka KafkaBatchInputPartition.")
  }

  def getFileFormat(scan: Scan): ReadFileFormat = {
    val scanClassName = scan.getClass.getName
    if (
      scanClassName == KafkaScanClassName ||
      scanClassName == KafkaOffsetLogHandoffScanClassName
    ) {
      ReadFileFormat.KafkaReadFormat
    } else {
      throw new GlutenNotSupportException(
        s"Only support KafkaScan, got $scanClassName.")
    }
  }

  def buildOffsetLogHandoffManifest(
      start: Offset,
      end: Offset,
      inputPartitions: Seq[InputPartition]): OffsetLogHandoffManifest = {
    buildOffsetLogHandoffManifest(start, end, inputPartitions, None)
  }

  def buildOffsetLogHandoffManifest(
      start: Offset,
      end: Offset,
      inputPartitions: Seq[InputPartition],
      nativeDiscoveredTopicPartitions: Option[Set[TopicPartition]]): OffsetLogHandoffManifest = {
    val startOffsets = kafkaPartitionOffsets(start, "start")
    val endOffsets = kafkaPartitionOffsets(end, "end")
    requireNativeDiscoveryWitnessForFullNativeHandoff(
      endOffsets,
      nativeDiscoveredTopicPartitions)
    val plannedPartitions = inputPartitions.map {
      case batch: KafkaBatchInputPartition =>
        validateKafkaBatchInputPartition(batch)
        batch
      case other =>
        throw new GlutenNotSupportException(
          s"Native Kafka offset-log handoff requires Spark Kafka input partitions, got " +
            s"${other.getClass.getName}")
    }

    val plannedByTopicPartition = plannedPartitions.groupBy(_.offsetRange.topicPartition)
    val duplicateTopicPartitions = plannedByTopicPartition.collect {
      case (topicPartition, ranges) if ranges.size > 1 => topicPartition
    }
    if (duplicateTopicPartitions.nonEmpty) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff planned duplicate ranges for " +
          formatTopicPartitions(duplicateTopicPartitions))
    }

    val plannedTopicPartitions = plannedByTopicPartition.keySet
    val requiredPlannedTopicPartitions = endOffsets.collect {
      case (topicPartition, endOffset)
          if startOffsets.getOrElse(topicPartition, 0L) != endOffset =>
        topicPartition
    }.toSet
    val missingFromPlan = requiredPlannedTopicPartitions.diff(plannedTopicPartitions)
    if (missingFromPlan.nonEmpty) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff did not plan partitions with changed Spark offsets in " +
          s"the end offset " +
          s"log: ${formatTopicPartitions(missingFromPlan)}")
    }

    val unexpectedInPlan = plannedTopicPartitions.diff(endOffsets.keySet)
    if (unexpectedInPlan.nonEmpty) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff planned partitions absent from Spark end offset log: " +
          formatTopicPartitions(unexpectedInPlan))
    }

    val removedFromEnd = startOffsets.keySet.diff(endOffsets.keySet)
    if (removedFromEnd.nonEmpty) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff found partitions present in Spark start offset log but " +
          s"missing from Spark end offset log: ${formatTopicPartitions(removedFromEnd)}")
    }

    nativeDiscoveredTopicPartitions.foreach {
      discovered => validateNativeDiscoveredTopicPartitions(discovered, endOffsets.keySet)
    }

    val partitionRanges = plannedPartitions
      .sortBy {
        batch =>
          val topicPartition = batch.offsetRange.topicPartition
          (topicPartition.topic(), topicPartition.partition())
      }
      .map {
        batch =>
          val range = batch.offsetRange
          val topicPartition = range.topicPartition
          val sparkEndOffset = endOffsets(topicPartition)
          if (range.untilOffset != sparkEndOffset) {
            throw new GlutenNotSupportException(
              s"Native Kafka offset-log handoff end offset drift for $topicPartition: " +
                s"planned untilOffset=${range.untilOffset}, Spark end offset log=$sparkEndOffset")
          }

          val sparkStartOffset = startOffsets.get(topicPartition)
          sparkStartOffset.foreach {
            loggedStart =>
              if (range.fromOffset != loggedStart) {
                throw new GlutenNotSupportException(
                  s"Native Kafka offset-log handoff start offset drift for $topicPartition: " +
                    s"planned fromOffset=${range.fromOffset}, Spark start offset log=$loggedStart")
              }
          }

          OffsetLogHandoffPartition(
            topic = topicPartition.topic(),
            partition = topicPartition.partition(),
            startOffset = range.fromOffset,
            endOffset = range.untilOffset,
            startOffsetFromSparkLog = sparkStartOffset,
            discoveredInBatch = sparkStartOffset.isEmpty
          )
      }

    OffsetLogHandoffManifest(
      startOffsets,
      endOffsets,
      partitionRanges,
      nativeDiscoveredTopicPartitions)
  }

  def validateNativeDiscoveryForHandoff(
      end: Offset,
      nativeDiscoveredTopicPartitions: Option[Set[TopicPartition]]): Unit = {
    nativeDiscoveredTopicPartitions.foreach {
      discovered =>
        validateNativeDiscoveredTopicPartitions(
          discovered,
          kafkaPartitionOffsets(end, "end").keySet)
    }
  }

  def parseOffsetLogHandoffManifestJson(json: String): OffsetLogHandoffManifest = {
    val root = parseManifestRoot(json)
    val version = requiredInt(root, "version", "manifest")
    if (version != OffsetLogHandoffManifestVersion) {
      throw invalidPersistedManifest(
        s"unsupported version $version; expected $OffsetLogHandoffManifestVersion")
    }

    val source = requiredText(root, "source", "manifest")
    if (source != OffsetLogHandoffManifestSource) {
      throw invalidPersistedManifest(
        s"unsupported source '$source'; expected '$OffsetLogHandoffManifestSource'")
    }

    val owner = requiredText(root, "offsetLogOwner", "manifest")
    if (owner != OffsetLogHandoffOwner) {
      throw invalidPersistedManifest(
        s"unsupported offsetLogOwner '$owner'; expected '$OffsetLogHandoffOwner'")
    }

    val nativeOffsetCommitsEnabled =
      requiredBoolean(root, "nativeOffsetCommitsEnabled", "manifest")
    if (nativeOffsetCommitsEnabled != NativeOffsetCommitsEnabled) {
      throw invalidPersistedManifest(
        "native offset commits must stay disabled in Kafka offset-log handoff manifests")
    }

    val startOffsetsFromSparkJson = kafkaPartitionOffsetsFromJson(
      requiredText(root, "sparkStartOffsetJson", "manifest"),
      "persisted start")
    val endOffsetsFromSparkJson = kafkaPartitionOffsetsFromJson(
      requiredText(root, "sparkEndOffsetJson", "manifest"),
      "persisted end")
    val startOffsetsFromArray =
      readPartitionOffsetArray(requiredArray(root, "startPartitionOffsets", "manifest"))
    val endOffsetsFromArray =
      readPartitionOffsetArray(requiredArray(root, "endPartitionOffsets", "manifest"))

    requireSameOffsets(
      startOffsetsFromArray,
      startOffsetsFromSparkJson,
      "startPartitionOffsets",
      "sparkStartOffsetJson")
    requireSameOffsets(
      endOffsetsFromArray,
      endOffsetsFromSparkJson,
      "endPartitionOffsets",
      "sparkEndOffsetJson")

    val partitionRanges =
      readPartitionRangeArray(requiredArray(root, "partitionRanges", "manifest"))
    validatePersistedPartitionRanges(
      startOffsetsFromSparkJson,
      endOffsetsFromSparkJson,
      partitionRanges)

    val nativeDiscoveredTopicPartitions = nullableField(root, "nativeDiscoveredTopicPartitions")
      .filterNot(_.isNull)
      .map {
        node =>
          readTopicPartitionSeq(
            requiredArray(node, "nativeDiscoveredTopicPartitions"),
            "nativeDiscoveredTopicPartitions")
      }
      .map(_.toSet)
    nativeDiscoveredTopicPartitions.foreach {
      discovered =>
        validateNativeDiscoveredTopicPartitions(discovered, endOffsetsFromSparkJson.keySet)
    }

    val manifest = OffsetLogHandoffManifest(
      startOffsetsFromSparkJson,
      endOffsetsFromSparkJson,
      partitionRanges,
      nativeDiscoveredTopicPartitions)

    requireSameTopicPartitions(
      readTopicPartitionSeq(
        requiredArray(root, "replayedTopicPartitions", "manifest"),
        "replayedTopicPartitions"),
      manifest.replayedTopicPartitions,
      "replayedTopicPartitions"
    )
    requireSameTopicPartitions(
      readTopicPartitionSeq(
        requiredArray(root, "newlyDiscoveredTopicPartitions", "manifest"),
        "newlyDiscoveredTopicPartitions"),
      manifest.newlyDiscoveredTopicPartitions,
      "newlyDiscoveredTopicPartitions"
    )

    manifest
  }

  def validateOffsetLogHandoffReplay(
      persistedManifestJson: String,
      start: Offset,
      end: Offset,
      inputPartitions: Seq[InputPartition]): OffsetLogHandoffManifest = {
    validateOffsetLogHandoffReplay(persistedManifestJson, start, end, inputPartitions, None)
  }

  def validateOffsetLogHandoffReplay(
      persistedManifestJson: String,
      start: Offset,
      end: Offset,
      inputPartitions: Seq[InputPartition],
      nativeDiscoveredTopicPartitions: Option[Set[TopicPartition]]): OffsetLogHandoffManifest = {
    val persisted = parseOffsetLogHandoffManifestJson(persistedManifestJson)
    val replayed = buildOffsetLogHandoffManifest(
      start,
      end,
      inputPartitions,
      nativeDiscoveredTopicPartitions)
    validateManifestReplayMatches(persisted, replayed)
    persisted
  }

  def persistOffsetLogHandoffManifest(
      metadataPath: String,
      manifest: OffsetLogHandoffManifest): Path = {
    val path = offsetLogHandoffManifestPath(metadataPath, manifest)
    val fs = path.getFileSystem(hadoopConf())
    if (fs.exists(path)) {
      validateManifestReplayMatches(readOffsetLogHandoffManifest(path), manifest)
      return path
    }

    val tmpPath = new Path(
      path.getParent,
      s".${path.getName}.${UUID.randomUUID().toString}.tmp")
    fs.mkdirs(path.getParent)
    val output = fs.create(tmpPath, false)
    try {
      output.write(manifest.persistedJson.getBytes(StandardCharsets.UTF_8))
    } finally {
      output.close()
    }

    if (!fs.rename(tmpPath, path)) {
      if (fs.exists(path)) {
        fs.delete(tmpPath, false)
        validateManifestReplayMatches(readOffsetLogHandoffManifest(path), manifest)
      } else {
        fs.delete(tmpPath, false)
        throw new GlutenNotSupportException(
          s"Native Kafka offset-log handoff could not persist manifest to $path")
      }
    }
    validateManifestReplayMatches(readOffsetLogHandoffManifest(path), manifest)
    path
  }

  def prepareOffsetLogHandoffReplay(
      metadataPath: String,
      manifest: OffsetLogHandoffManifest): OffsetLogHandoffReplay = {
    loadPersistedOffsetLogHandoffManifest(metadataPath, manifest.endPartitionOffsets) match {
      case Some(replay) =>
        validateManifestReplayMatches(replay.manifest, manifest)
        replay
      case None =>
        val path = persistOffsetLogHandoffManifest(metadataPath, manifest)
        OffsetLogHandoffReplay(
          metadataPath,
          path,
          readOffsetLogHandoffManifest(path),
          replayedExistingManifest = false)
    }
  }

  def prepareOffsetLogHandoff(
      stream: MicroBatchStream,
      start: Offset,
      end: Offset,
      inputPartitions: Seq[InputPartition],
      nativeDiscoveredTopicPartitions: Option[Set[TopicPartition]]): OffsetLogHandoffReplay = {
    val manifest = buildOffsetLogHandoffManifest(
      start,
      end,
      inputPartitions,
      nativeDiscoveredTopicPartitions)
    val metadataPath = kafkaMicroBatchMetadataPath(stream)
    prepareOffsetLogHandoffReplay(metadataPath, manifest)
  }

  def readPersistedOffsetLogHandoffManifest(
      metadataPath: String,
      manifest: OffsetLogHandoffManifest): OffsetLogHandoffManifest = {
    readOffsetLogHandoffManifest(offsetLogHandoffManifestPath(metadataPath, manifest))
  }

  def readPersistedOffsetLogHandoffManifestForEndOffset(
      metadataPath: String,
      end: Offset): OffsetLogHandoffManifest = {
    readPersistedOffsetLogHandoffManifest(
      metadataPath,
      kafkaPartitionOffsets(end, "end"))
  }

  def readPersistedOffsetLogHandoffManifestForReplay(
      stream: MicroBatchStream,
      end: Offset): OffsetLogHandoffManifest = {
    readPersistedOffsetLogHandoffManifestForEndOffset(
      kafkaMicroBatchMetadataPath(stream),
      end)
  }

  def readPersistedOffsetLogHandoffManifest(
      metadataPath: String,
      endPartitionOffsets: Map[TopicPartition, Long]): OffsetLogHandoffManifest = {
    loadPersistedOffsetLogHandoffManifest(metadataPath, endPartitionOffsets)
      .map(_.manifest)
      .getOrElse {
        throw new GlutenNotSupportException(
          s"Native Kafka offset-log handoff manifest is missing at " +
            s"${offsetLogHandoffManifestPath(metadataPath, endPartitionOffsets)}")
      }
  }

  def loadPersistedOffsetLogHandoffManifestForEndOffset(
      metadataPath: String,
      end: Offset): Option[OffsetLogHandoffReplay] = {
    loadPersistedOffsetLogHandoffManifest(
      metadataPath,
      kafkaPartitionOffsets(end, "end"))
  }

  def validatePersistedOffsetLogHandoffManifest(
      metadataPath: String,
      start: Offset,
      end: Offset,
      inputPartitions: Seq[InputPartition]): OffsetLogHandoffManifest = {
    validatePersistedOffsetLogHandoffManifest(metadataPath, start, end, inputPartitions, None)
  }

  def validatePersistedOffsetLogHandoffManifest(
      metadataPath: String,
      start: Offset,
      end: Offset,
      inputPartitions: Seq[InputPartition],
      nativeDiscoveredTopicPartitions: Option[Set[TopicPartition]]): OffsetLogHandoffManifest = {
    val startOffsets = kafkaPartitionOffsets(start, "start")
    val endOffsets = kafkaPartitionOffsets(end, "end")
    if (startOffsets == endOffsets) {
      return readPersistedOffsetLogHandoffManifest(metadataPath, endOffsets)
    }

    val replayed = buildOffsetLogHandoffManifest(
      start,
      end,
      inputPartitions,
      nativeDiscoveredTopicPartitions)
    val persisted = readPersistedOffsetLogHandoffManifest(metadataPath, replayed)
    validateManifestReplayMatches(persisted, replayed)
    persisted
  }

  def validatePersistedOffsetLogHandoff(
      stream: MicroBatchStream,
      start: Offset,
      end: Offset,
      inputPartitions: Seq[InputPartition],
      nativeDiscoveredTopicPartitions: Option[Set[TopicPartition]]): OffsetLogHandoffManifest = {
    validatePersistedOffsetLogHandoffManifest(
      kafkaMicroBatchMetadataPath(stream),
      start,
      end,
      inputPartitions,
      nativeDiscoveredTopicPartitions)
  }

  private def validateOffsetLogHandoffIfEnabled(
      stream: MicroBatchStream,
      start: Offset,
      end: Offset,
      inputPartitions: Seq[InputPartition]): Unit = {
    if (!GlutenConfig.get.enableNativeStreamingKafkaSourceOffsetLogHandoff) {
      return
    }

    val manifest = buildOffsetLogHandoffManifest(
      start,
      end,
      inputPartitions,
      discoverNativeTopicPartitionsForHandoff(inputPartitions))
    val replay = prepareOffsetLogHandoff(
      stream,
      start,
      end,
      inputPartitions,
      manifest.nativeDiscoveredTopicPartitions)
    validatePersistedOffsetLogHandoffManifest(
      replay.metadataPath,
      start,
      end,
      inputPartitions,
      manifest.nativeDiscoveredTopicPartitions)
  }

  private def kafkaPartitionOffsets(offset: Offset, label: String): Map[TopicPartition, Long] = {
    Option(offset)
      .map {
        nonNullOffset =>
          try {
            KafkaSourceOffset(nonNullOffset).partitionToOffsets
          } catch {
            case NonFatal(e) =>
              throw new GlutenNotSupportException(
                s"Native Kafka offset-log handoff requires a Spark Kafka $label offset, got " +
                  s"${nonNullOffset.getClass.getName}",
                e)
          }
      }
      .getOrElse(Map.empty)
  }

  private def kafkaPartitionOffsetsFromJson(
      json: String,
      label: String): Map[TopicPartition, Long] = {
    try {
      JsonUtils.partitionOffsets(json)
    } catch {
      case NonFatal(e) =>
        throw new GlutenNotSupportException(
          s"Native Kafka offset-log handoff manifest replay requires a Spark Kafka $label " +
            s"offset JSON",
          e)
    }
  }

  private def offsetLogHandoffManifestJson(manifest: OffsetLogHandoffManifest): String = {
    val root = new JLinkedHashMap[String, Object]()
    root.put("version", Int.box(OffsetLogHandoffManifestVersion))
    root.put("source", OffsetLogHandoffManifestSource)
    root.put("offsetLogOwner", OffsetLogHandoffOwner)
    root.put("nativeOffsetCommitsEnabled", Boolean.box(NativeOffsetCommitsEnabled))
    root.put("sparkStartOffsetJson", manifest.sparkStartOffsetJson)
    root.put("sparkEndOffsetJson", manifest.sparkEndOffsetJson)
    root.put("startPartitionOffsets", partitionOffsetsJsonArray(manifest.startPartitionOffsets))
    root.put("endPartitionOffsets", partitionOffsetsJsonArray(manifest.endPartitionOffsets))
    root.put("partitionRanges", partitionRangesJsonArray(manifest.partitionRanges))
    root.put("replayedTopicPartitions", topicPartitionsJsonArray(manifest.replayedTopicPartitions))
    root.put(
      "newlyDiscoveredTopicPartitions",
      topicPartitionsJsonArray(manifest.newlyDiscoveredTopicPartitions))
    root.put(
      "nativeDiscoveredTopicPartitions",
      manifest.nativeDiscoveredTopicPartitions
        .map(topicPartitionsJsonArray)
        .orNull)
    JsonMapper.writeValueAsString(root)
  }

  private def partitionOffsetsJsonArray(
      partitionOffsets: Map[TopicPartition, Long]): JArrayList[Object] = {
    val values = new JArrayList[Object]()
    sortTopicPartitions(partitionOffsets.keySet).foreach {
      topicPartition =>
        val json = topicPartitionJson(topicPartition)
        json.put("offset", Long.box(partitionOffsets(topicPartition)))
        values.add(json)
    }
    values
  }

  private def partitionRangesJsonArray(
      partitionRanges: Seq[OffsetLogHandoffPartition]): JArrayList[Object] = {
    val values = new JArrayList[Object]()
    partitionRanges.foreach {
      range =>
        val json = new JLinkedHashMap[String, Object]()
        json.put("topic", range.topic)
        json.put("partition", Int.box(range.partition))
        json.put("startOffset", Long.box(range.startOffset))
        json.put("endOffset", Long.box(range.endOffset))
        json.put("startOffsetFromSparkLog", range.startOffsetFromSparkLog.map(Long.box).orNull)
        json.put("discoveredInBatch", Boolean.box(range.discoveredInBatch))
        values.add(json)
    }
    values
  }

  private def topicPartitionsJsonArray(
      topicPartitions: Iterable[TopicPartition]): JArrayList[Object] = {
    val values = new JArrayList[Object]()
    sortTopicPartitions(topicPartitions).foreach {
      topicPartition => values.add(topicPartitionJson(topicPartition))
    }
    values
  }

  private def topicPartitionJson(topicPartition: TopicPartition): JLinkedHashMap[String, Object] = {
    val json = new JLinkedHashMap[String, Object]()
    json.put("topic", topicPartition.topic())
    json.put("partition", Int.box(topicPartition.partition()))
    json
  }

  private def parseManifestRoot(json: String): JsonNode = {
    try {
      val root = JsonMapper.readTree(json)
      if (root == null || !root.isObject) {
        throw invalidPersistedManifest("root must be a JSON object")
      }
      root
    } catch {
      case e: GlutenNotSupportException => throw e
      case NonFatal(e) =>
        throw new GlutenNotSupportException(
          "Native Kafka offset-log handoff manifest replay failed to parse persisted manifest",
          e)
    }
  }

  private def requiredField(root: JsonNode, field: String, context: String): JsonNode = {
    val value = root.get(field)
    if (value == null || value.isNull) {
      throw invalidPersistedManifest(s"$context is missing required field '$field'")
    }
    value
  }

  private def nullableField(root: JsonNode, field: String): Option[JsonNode] = {
    Option(root.get(field))
  }

  private def requiredText(root: JsonNode, field: String, context: String): String = {
    val value = requiredField(root, field, context)
    if (!value.isTextual) {
      throw invalidPersistedManifest(s"$context field '$field' must be a string")
    }
    value.asText()
  }

  private def requiredBoolean(root: JsonNode, field: String, context: String): Boolean = {
    val value = requiredField(root, field, context)
    if (!value.isBoolean) {
      throw invalidPersistedManifest(s"$context field '$field' must be a boolean")
    }
    value.asBoolean()
  }

  private def requiredInt(root: JsonNode, field: String, context: String): Int = {
    val value = requiredField(root, field, context)
    if (!value.isIntegralNumber || !value.canConvertToInt) {
      throw invalidPersistedManifest(s"$context field '$field' must be an integer")
    }
    value.asInt()
  }

  private def requiredLong(root: JsonNode, field: String, context: String): Long = {
    val value = requiredField(root, field, context)
    if (!value.isIntegralNumber) {
      throw invalidPersistedManifest(s"$context field '$field' must be a long")
    }
    value.asLong()
  }

  private def requiredArray(root: JsonNode, field: String, context: String): JsonNode = {
    val value = requiredField(root, field, context)
    requiredArray(value, s"$context field '$field'")
  }

  private def requiredArray(node: JsonNode, context: String): JsonNode = {
    if (!node.isArray) {
      throw invalidPersistedManifest(s"$context must be an array")
    }
    node
  }

  private def readTopicPartitionSeq(node: JsonNode, context: String): Seq[TopicPartition] = {
    requiredArray(node, context)
      .elements()
      .asScala
      .zipWithIndex
      .map {
        case (entry, index) => readTopicPartition(entry, s"$context[$index]")
      }
      .toSeq
  }

  private def readTopicPartition(node: JsonNode, context: String): TopicPartition = {
    if (!node.isObject) {
      throw invalidPersistedManifest(s"$context must be an object")
    }
    val topic = requiredText(node, "topic", context)
    val partition = requiredInt(node, "partition", context)
    if (topic.isEmpty) {
      throw invalidPersistedManifest(s"$context requires a non-empty topic")
    }
    if (partition < 0) {
      throw invalidPersistedManifest(s"$context requires a non-negative partition")
    }
    new TopicPartition(topic, partition)
  }

  private def readPartitionOffsetArray(node: JsonNode): Map[TopicPartition, Long] = {
    val offsets = requiredArray(node, "partition offset array")
      .elements()
      .asScala
      .zipWithIndex
      .map {
        case (entry, index) =>
          val context = s"partition offset array[$index]"
          val topicPartition = readTopicPartition(entry, context)
          topicPartition -> requiredLong(entry, "offset", context)
      }
      .toSeq
    val duplicateTopicPartitions = offsets
      .groupBy(_._1)
      .collect { case (topicPartition, values) if values.size > 1 => topicPartition }
    if (duplicateTopicPartitions.nonEmpty) {
      throw invalidPersistedManifest(
        s"partition offset array contains duplicate partitions " +
          formatTopicPartitions(duplicateTopicPartitions))
    }
    offsets.toMap
  }

  private def readPartitionRangeArray(node: JsonNode): Seq[OffsetLogHandoffPartition] = {
    val ranges = requiredArray(node, "partitionRanges")
      .elements()
      .asScala
      .zipWithIndex
      .map {
        case (entry, index) =>
          val context = s"partitionRanges[$index]"
          val topicPartition = readTopicPartition(entry, context)
          val startOffset = requiredLong(entry, "startOffset", context)
          val endOffset = requiredLong(entry, "endOffset", context)
          val startOffsetFromSparkLogNode = Option(entry.get("startOffsetFromSparkLog")).getOrElse(
            throw invalidPersistedManifest(
              s"$context is missing required field 'startOffsetFromSparkLog'"))
          val startOffsetFromSparkLog =
            if (startOffsetFromSparkLogNode.isNull) {
              None
            } else if (startOffsetFromSparkLogNode.isIntegralNumber) {
              Some(startOffsetFromSparkLogNode.asLong())
            } else {
              throw invalidPersistedManifest(
                s"$context field 'startOffsetFromSparkLog' must be null or a long")
            }

          if (startOffset < 0 || endOffset < 0) {
            throw invalidPersistedManifest(
              s"$context requires finite non-negative offsets, got [$startOffset, $endOffset)")
          }
          if (endOffset < startOffset) {
            throw invalidPersistedManifest(
              s"$context requires endOffset >= startOffset, got [$startOffset, $endOffset)")
          }

          OffsetLogHandoffPartition(
            topic = topicPartition.topic(),
            partition = topicPartition.partition(),
            startOffset = startOffset,
            endOffset = endOffset,
            startOffsetFromSparkLog = startOffsetFromSparkLog,
            discoveredInBatch = requiredBoolean(entry, "discoveredInBatch", context)
          )
      }
      .toSeq

    val duplicateTopicPartitions = ranges
      .groupBy(range => new TopicPartition(range.topic, range.partition))
      .collect { case (topicPartition, values) if values.size > 1 => topicPartition }
    if (duplicateTopicPartitions.nonEmpty) {
      throw invalidPersistedManifest(
        s"partitionRanges contains duplicate partitions " +
          formatTopicPartitions(duplicateTopicPartitions))
    }
    ranges
  }

  private def validatePersistedPartitionRanges(
      startOffsets: Map[TopicPartition, Long],
      endOffsets: Map[TopicPartition, Long],
      partitionRanges: Seq[OffsetLogHandoffPartition]): Unit = {
    val partitionRangesByTopicPartition = partitionRanges.map {
      range => new TopicPartition(range.topic, range.partition) -> range
    }.toMap
    val plannedTopicPartitions = partitionRangesByTopicPartition.keySet
    val requiredPlannedTopicPartitions = endOffsets.collect {
      case (topicPartition, endOffset)
          if startOffsets.getOrElse(topicPartition, 0L) != endOffset =>
        topicPartition
    }.toSet

    val missingFromRanges = requiredPlannedTopicPartitions.diff(plannedTopicPartitions)
    if (missingFromRanges.nonEmpty) {
      throw invalidPersistedManifest(
        s"partitionRanges are missing Spark end offset log partitions with changed offsets " +
          formatTopicPartitions(missingFromRanges))
    }

    val unexpectedInRanges = plannedTopicPartitions.diff(endOffsets.keySet)
    if (unexpectedInRanges.nonEmpty) {
      throw invalidPersistedManifest(
        s"partitionRanges contain partitions absent from Spark end offset log " +
          formatTopicPartitions(unexpectedInRanges))
    }

    val removedFromEnd = startOffsets.keySet.diff(endOffsets.keySet)
    if (removedFromEnd.nonEmpty) {
      throw invalidPersistedManifest(
        s"Spark start offset log partitions are missing from Spark end offset log " +
          formatTopicPartitions(removedFromEnd))
    }

    partitionRanges.foreach {
      range =>
        val topicPartition = new TopicPartition(range.topic, range.partition)
        val sparkEndOffset = endOffsets(topicPartition)
        if (range.endOffset != sparkEndOffset) {
          throw invalidPersistedManifest(
            s"partitionRanges end offset drift for $topicPartition: " +
              s"manifest endOffset=${range.endOffset}, Spark end offset log=$sparkEndOffset")
        }

        val sparkStartOffset = startOffsets.get(topicPartition)
        if (range.startOffsetFromSparkLog != sparkStartOffset) {
          throw invalidPersistedManifest(
            s"partitionRanges start offset witness drift for $topicPartition: " +
              s"manifest startOffsetFromSparkLog=${range.startOffsetFromSparkLog}, " +
              s"Spark start offset log=$sparkStartOffset")
        }
        sparkStartOffset.foreach {
          loggedStart =>
            if (range.startOffset != loggedStart) {
              throw invalidPersistedManifest(
                s"partitionRanges start offset drift for $topicPartition: " +
                  s"manifest startOffset=${range.startOffset}, " +
                  s"Spark start offset log=$loggedStart")
            }
        }

        if (range.discoveredInBatch != sparkStartOffset.isEmpty) {
          throw invalidPersistedManifest(
            s"partitionRanges discovery witness drift for $topicPartition: " +
              s"manifest discoveredInBatch=${range.discoveredInBatch}, " +
              s"Spark start offset log present=${sparkStartOffset.nonEmpty}")
        }
    }
  }

  private def requireSameOffsets(
      actual: Map[TopicPartition, Long],
      expected: Map[TopicPartition, Long],
      actualLabel: String,
      expectedLabel: String): Unit = {
    if (actual != expected) {
      throw invalidPersistedManifest(
        s"$actualLabel disagree with $expectedLabel: " +
          s"$actualLabel=${KafkaSourceOffset(actual).json}, " +
          s"$expectedLabel=${KafkaSourceOffset(expected).json}")
    }
  }

  private def requireSameTopicPartitions(
      actual: Seq[TopicPartition],
      expected: Seq[TopicPartition],
      field: String): Unit = {
    if (actual != expected) {
      throw invalidPersistedManifest(
        s"$field disagree with manifest offsets: " +
          s"actual=${formatTopicPartitions(actual)}, expected=${formatTopicPartitions(expected)}")
    }
  }

  private def validateManifestReplayMatches(
      persisted: OffsetLogHandoffManifest,
      replayed: OffsetLogHandoffManifest): Unit = {
    if (persisted.startPartitionOffsets != replayed.startPartitionOffsets) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff replay start offset log drift: " +
          s"persisted=${KafkaSourceOffset(persisted.startPartitionOffsets).json}, " +
          s"replayed=${KafkaSourceOffset(replayed.startPartitionOffsets).json}")
    }
    if (persisted.endPartitionOffsets != replayed.endPartitionOffsets) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff replay end offset log drift: " +
          s"persisted=${KafkaSourceOffset(persisted.endPartitionOffsets).json}, " +
          s"replayed=${KafkaSourceOffset(replayed.endPartitionOffsets).json}")
    }
    if (persisted.partitionRanges != replayed.partitionRanges) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff replay partition range drift: " +
          s"persisted=${formatManifestPartitionRanges(persisted.partitionRanges)}, " +
          s"replayed=${formatManifestPartitionRanges(replayed.partitionRanges)}")
    }
    if (persisted.nativeDiscoveredTopicPartitions != replayed.nativeDiscoveredTopicPartitions) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff replay native discovery drift: " +
          s"persisted=${persisted.nativeDiscoveredTopicPartitions.map(formatTopicPartitions)}, " +
          s"replayed=${replayed.nativeDiscoveredTopicPartitions.map(formatTopicPartitions)}")
    }
  }

  private def formatManifestPartitionRanges(
      partitionRanges: Seq[OffsetLogHandoffPartition]): String = {
    partitionRanges
      .map {
        range =>
          s"${range.topic}-${range.partition}:[${range.startOffset},${range.endOffset})," +
            s"sparkStart=${range.startOffsetFromSparkLog}," +
            s"discovered=${range.discoveredInBatch}"
      }
      .mkString(";")
  }

  private def validateNativeDiscoveredTopicPartitions(
      discovered: Set[TopicPartition],
      sparkEndTopicPartitions: Set[TopicPartition]): Unit = {
    val missingFromSparkEndLog = discovered.diff(sparkEndTopicPartitions)
    if (missingFromSparkEndLog.nonEmpty) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff found native-discovered partitions absent from " +
          s"Spark end offset log: ${formatTopicPartitions(missingFromSparkEndLog)}")
    }

    val missingFromNativeDiscovery = sparkEndTopicPartitions.diff(discovered)
    if (missingFromNativeDiscovery.nonEmpty) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff found Spark end offset log partitions absent from " +
          s"native discovery: ${formatTopicPartitions(missingFromNativeDiscovery)}")
    }
  }

  private def requireNativeDiscoveryWitnessForFullNativeHandoff(
      sparkEndPartitionOffsets: Map[TopicPartition, Long],
      nativeDiscoveredTopicPartitions: Option[Set[TopicPartition]]): Unit = {
    val glutenConf = GlutenConfig.get
    if (
      glutenConf.enableNativeStreamingFull &&
      glutenConf.enableNativeStreamingKafkaSourceOffsetLogHandoff &&
      glutenConf.enableNativeStreamingKafkaSourceNativeOffsetPlanning &&
      sparkEndPartitionOffsets.nonEmpty &&
      nativeDiscoveredTopicPartitions.isEmpty
    ) {
      throw new GlutenNotSupportException(
        "Full native Kafka source ownership requires a native broker-discovery witness in the " +
          "offset-log handoff manifest; Spark/JVM-discovered partitions do not satisfy native " +
          "source ownership.")
    }
  }

  private def invalidPersistedManifest(message: String): GlutenNotSupportException = {
    new GlutenNotSupportException(
      s"Native Kafka offset-log handoff manifest replay rejected persisted manifest: $message")
  }

  private def formatTopicPartitions(topicPartitions: Iterable[TopicPartition]): String = {
    sortTopicPartitions(topicPartitions)
      .map(_.toString)
      .mkString(",")
  }

  private def sortTopicPartitions(
      topicPartitions: Iterable[TopicPartition]): Seq[TopicPartition] = {
    topicPartitions.toSeq
      .sortBy(topicPartition => (topicPartition.topic(), topicPartition.partition()))
  }

  private def validateFiniteOffsetRange(offsetRange: KafkaOffsetRange): Unit = {
    if (offsetRange.fromOffset < 0 || offsetRange.untilOffset < 0) {
      throw new GlutenNotSupportException(
        s"Native Kafka micro-batch scan requires finite non-negative offsets, got " +
          s"${offsetRange.topicPartition} fromOffset=${offsetRange.fromOffset} " +
          s"untilOffset=${offsetRange.untilOffset}")
    }
    if (offsetRange.untilOffset < offsetRange.fromOffset) {
      throw new GlutenNotSupportException(
        s"Native Kafka micro-batch scan requires untilOffset >= fromOffset, got " +
          s"${offsetRange.topicPartition} fromOffset=${offsetRange.fromOffset} " +
          s"untilOffset=${offsetRange.untilOffset}")
    }
  }

  private def validateKafkaBatchInputPartition(batch: KafkaBatchInputPartition): Unit = {
    val topicPartition = batch.offsetRange.topicPartition
    val topic = topicPartition.topic()
    if (topic == null || topic.isEmpty) {
      throw new GlutenNotSupportException(
        s"Native Kafka micro-batch scan requires a non-empty topic, got $topicPartition")
    }
    if (topicPartition.partition() < 0) {
      throw new GlutenNotSupportException(
        s"Native Kafka micro-batch scan requires a non-negative partition, got $topicPartition")
    }
    if (batch.pollTimeoutMs < 0) {
      throw new GlutenNotSupportException(
        s"Native Kafka micro-batch scan requires a non-negative poll timeout, got " +
          s"${batch.pollTimeoutMs} ms for $topicPartition")
    }
    validateFiniteOffsetRange(batch.offsetRange)
  }

  private def validateNativeOffsetPlanningIfEnabled(batch: KafkaBatchInputPartition): Unit = {
    if (!GlutenConfig.get.enableNativeStreamingKafkaSourceNativeOffsetPlanning) {
      return
    }

    try {
      invokeNativeKafkaOffsetPlanner(
        "validateSparkPlannedRange",
        classOf[KafkaBatchInputPartition])(
        batch)
    } catch {
      case e: InvocationTargetException =>
        e.getCause match {
          case unsupported: GlutenNotSupportException =>
            throw unsupported
          case e: UnsatisfiedLinkError =>
            throw new GlutenNotSupportException(
              "Native Kafka offset planning validation requires Velox JNI support",
              e)
          case runtime: RuntimeException =>
            throw runtime
          case error: Error =>
            throw error
          case cause =>
            throw new GlutenNotSupportException(
              "Native Kafka offset planning validation failed before split creation",
              cause)
        }
      case e: ClassNotFoundException =>
        throw new GlutenNotSupportException(
          s"Native Kafka offset planning validation requires Velox planner " +
            s"$NativeKafkaOffsetPlannerClassName on the classpath",
          e)
      case e: NoSuchFieldException =>
        throw new GlutenNotSupportException(
          s"Native Kafka offset planning validation could not call " +
            s"$NativeKafkaOffsetPlannerClassName",
          e)
      case e: NoSuchMethodException =>
        throw new GlutenNotSupportException(
          s"Native Kafka offset planning validation could not call " +
            s"$NativeKafkaOffsetPlannerClassName",
          e)
      case e: IllegalAccessException =>
        throw new GlutenNotSupportException(
          s"Native Kafka offset planning validation could not call " +
            s"$NativeKafkaOffsetPlannerClassName",
          e)
      case e: UnsatisfiedLinkError =>
        throw new GlutenNotSupportException(
          "Native Kafka offset planning validation requires Velox JNI support",
          e)
    }
  }

  private[kafka010] def discoverNativeTopicPartitionsForHandoff(
      stream: MicroBatchStream,
      end: Offset): Option[Set[TopicPartition]] = {
    val endTopicPartitions = kafkaPartitionOffsets(end, "end").keySet
    if (endTopicPartitions.isEmpty) {
      return Some(Set.empty)
    }

    discoverNativeTopicPartitionsForHandoff(
      endTopicPartitions.map(_.topic()).toSeq.distinct.sorted,
      kafkaExecutorParams(stream))
  }

  private[kafka010] def discoverNativeTopicPartitionsForHandoff(
      inputPartitions: Seq[InputPartition]): Option[Set[TopicPartition]] = {
    val batches = inputPartitions.map {
      case batch: KafkaBatchInputPartition => batch
      case other =>
        throw new GlutenNotSupportException(
          s"Native Kafka partition discovery requires Spark Kafka input partitions, got " +
            s"${other.getClass.getName}")
    }

    if (batches.isEmpty) {
      return Some(Set.empty)
    }

    discoverNativeTopicPartitionsForHandoff(
      batches.map(_.offsetRange.topicPartition.topic()).distinct.sorted,
      batches.head.executorKafkaParams)
  }

  private[kafka010] def discoverNativeTopicPartitionsForHandoff(
      topics: Seq[String],
      kafkaParams: JMap[String, Object]): Option[Set[TopicPartition]] = {
    if (topics.isEmpty) {
      return Some(Set.empty)
    }
    if (!GlutenConfig.get.enableNativeStreamingKafkaSourceNativeOffsetPlanning) {
      return None
    }

    try {
      val discovered = invokeNativeKafkaOffsetPlanner(
        "discoverTopicPartitions",
        classOf[Array[String]],
        classOf[JMap[_, _]])(topics.toArray, kafkaParams)
        .asInstanceOf[Array[TopicPartition]]
      Some(discovered.toSet)
    } catch {
      case e: InvocationTargetException =>
        e.getCause match {
          case unsupported: GlutenNotSupportException =>
            throw unsupported
          case runtime: RuntimeException =>
            throw runtime
          case error: Error =>
            throw error
          case cause =>
            throw new GlutenNotSupportException(
              "Native Kafka partition discovery failed before offset-log handoff",
              cause)
        }
      case e: ClassNotFoundException =>
        throw new GlutenNotSupportException(
          s"Native Kafka partition discovery requires Velox planner " +
            s"$NativeKafkaOffsetPlannerClassName on the classpath",
          e)
      case e: NoSuchFieldException =>
        throw new GlutenNotSupportException(
          s"Native Kafka partition discovery could not call $NativeKafkaOffsetPlannerClassName",
          e)
      case e: NoSuchMethodException =>
        throw new GlutenNotSupportException(
          s"Native Kafka partition discovery could not call $NativeKafkaOffsetPlannerClassName",
          e)
      case e: IllegalAccessException =>
        throw new GlutenNotSupportException(
          s"Native Kafka partition discovery could not call $NativeKafkaOffsetPlannerClassName",
          e)
    }
  }

  def validateNativeTopicPatternDiscoveryForHandoff(
      topicPattern: String,
      options: CaseInsensitiveStringMap): Unit = {
    discoverNativeTopicPartitionsForPatternForHandoff(
      topicPattern,
      kafkaParamsFromSourceOptions(options))
  }

  private[kafka010] def discoverNativeTopicPartitionsForPatternForHandoff(
      topicPattern: String,
      kafkaParams: JMap[String, Object]): Option[Set[TopicPartition]] = {
    val pattern = Option(topicPattern).map(_.trim).getOrElse("")
    if (pattern.isEmpty) {
      throw new GlutenNotSupportException(
        "Native Kafka pattern partition discovery requires a non-empty subscribePattern")
    }
    if (!GlutenConfig.get.enableNativeStreamingKafkaSourceNativeOffsetPlanning) {
      throw new GlutenNotSupportException(
        "Native Kafka subscribePattern offset-log handoff requires native Kafka offset " +
          "planning validation so regex topic discovery is native-owned before Spark split " +
          "planning.")
    }

    try {
      val discovered = invokeNativeKafkaOffsetPlanner(
        "discoverTopicPartitionsForPattern",
        classOf[String],
        classOf[JMap[_, _]])(pattern, kafkaParams)
        .asInstanceOf[Array[TopicPartition]]
      Some(discovered.toSet)
    } catch {
      case e: InvocationTargetException =>
        e.getCause match {
          case unsupported: GlutenNotSupportException =>
            throw unsupported
          case runtime: RuntimeException =>
            throw runtime
          case error: Error =>
            throw error
          case cause =>
            throw new GlutenNotSupportException(
              "Native Kafka pattern partition discovery failed before offset-log handoff",
              cause)
        }
      case e: ClassNotFoundException =>
        throw new GlutenNotSupportException(
          s"Native Kafka pattern partition discovery requires Velox planner " +
            s"$NativeKafkaOffsetPlannerClassName on the classpath",
          e)
      case e: NoSuchFieldException =>
        throw new GlutenNotSupportException(
          s"Native Kafka pattern partition discovery could not call " +
            s"$NativeKafkaOffsetPlannerClassName",
          e)
      case e: NoSuchMethodException =>
        throw new GlutenNotSupportException(
          s"Native Kafka pattern partition discovery could not call " +
            s"$NativeKafkaOffsetPlannerClassName",
          e)
      case e: IllegalAccessException =>
        throw new GlutenNotSupportException(
          s"Native Kafka pattern partition discovery could not call " +
            s"$NativeKafkaOffsetPlannerClassName",
          e)
    }
  }

  private def kafkaExecutorParams(stream: MicroBatchStream): JMap[String, Object] = {
    try {
      val field = findDeclaredField(stream.getClass, "executorKafkaParams")
      field.setAccessible(true)
      field.get(stream).asInstanceOf[JMap[String, Object]]
    } catch {
      case e: NoSuchFieldException =>
        throw new GlutenNotSupportException(
          s"Native Kafka partition discovery requires KafkaMicroBatchStream executor params, got " +
            s"${stream.getClass.getName}",
          e)
      case e: IllegalAccessException =>
        throw new GlutenNotSupportException(
          s"Native Kafka partition discovery could not read executor params from " +
            s"${stream.getClass.getName}",
          e)
    }
  }

  private def kafkaParamsFromSourceOptions(
      options: CaseInsensitiveStringMap): JMap[String, Object] = {
    val params = new JLinkedHashMap[String, Object]()
    options.asCaseSensitiveMap().asScala.foreach {
      case (key, value) =>
        val normalized = key.toLowerCase(Locale.ROOT)
        if (normalized.startsWith(KafkaOptionPrefix)) {
          params.put(normalized.substring(KafkaOptionPrefix.length), value)
        }
    }
    if (!params.containsKey(BootstrapServersOption)) {
      throw new GlutenNotSupportException(
        "Native Kafka pattern partition discovery requires kafka.bootstrap.servers")
    }
    params
  }

  private def findDeclaredField(owner: Class[_], fieldName: String): Field = {
    if (owner == null) {
      throw new NoSuchFieldException(fieldName)
    }
    try {
      owner.getDeclaredField(fieldName)
    } catch {
      case _: NoSuchFieldException => findDeclaredField(owner.getSuperclass, fieldName)
    }
  }

  private def invokeNativeKafkaOffsetPlanner(
      methodName: String,
      parameterTypes: Class[_]*)(args: AnyRef*): AnyRef = {
    val plannerClass = Utils.classForName(NativeKafkaOffsetPlannerClassName)
    val method = plannerClass.getMethod(methodName, parameterTypes: _*)
    val planner =
      try {
        plannerClass.getField("MODULE$").get(null)
      } catch {
        case _: NoSuchFieldException => null
      }
    method.invoke(planner, args: _*)
  }

  private def loadPersistedOffsetLogHandoffManifest(
      metadataPath: String,
      endPartitionOffsets: Map[TopicPartition, Long]): Option[OffsetLogHandoffReplay] = {
    val path = offsetLogHandoffManifestPath(metadataPath, endPartitionOffsets)
    val fs = path.getFileSystem(hadoopConf())
    if (!fs.exists(path)) {
      return None
    }

    val manifest = readOffsetLogHandoffManifest(path)
    if (manifest.endPartitionOffsets != endPartitionOffsets) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff manifest end-offset digest drift at $path: " +
          s"manifest=${KafkaSourceOffset(manifest.endPartitionOffsets).json}, " +
          s"requested=${KafkaSourceOffset(endPartitionOffsets).json}")
    }
    Some(OffsetLogHandoffReplay(
      metadataPath,
      path,
      manifest,
      replayedExistingManifest = true))
  }

  private def offsetLogHandoffManifestPath(
      metadataPath: String,
      manifest: OffsetLogHandoffManifest): Path = {
    offsetLogHandoffManifestPath(metadataPath, manifest.endPartitionOffsets)
  }

  private def offsetLogHandoffManifestPath(
      metadataPath: String,
      endPartitionOffsets: Map[TopicPartition, Long]): Path = {
    val metadata = Option(metadataPath)
      .filter(_.nonEmpty)
      .getOrElse {
        throw new GlutenNotSupportException(
          "Native Kafka offset-log handoff requires Spark Kafka stream metadata path")
      }
    new Path(
      new Path(metadata, OffsetLogHandoffDirName),
      s"${partitionOffsetsDigest(endPartitionOffsets)}.json")
  }

  private def readOffsetLogHandoffManifest(path: Path): OffsetLogHandoffManifest = {
    val fs = path.getFileSystem(hadoopConf())
    if (!fs.exists(path)) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset-log handoff manifest is missing at $path")
    }
    val input = fs.open(path)
    try {
      parseOffsetLogHandoffManifestJson(scala.io.Source
        .fromInputStream(input, StandardCharsets.UTF_8.name())
        .mkString)
    } finally {
      input.close()
    }
  }

  private def kafkaMicroBatchMetadataPath(stream: MicroBatchStream): String = {
    try {
      val field = stream.getClass.getDeclaredField("metadataPath")
      field.setAccessible(true)
      field.get(stream).asInstanceOf[String]
    } catch {
      case e: NoSuchFieldException =>
        throw new GlutenNotSupportException(
          s"Native Kafka offset-log handoff requires KafkaMicroBatchStream metadata path, got " +
            s"${stream.getClass.getName}",
          e)
      case e: IllegalAccessException =>
        throw new GlutenNotSupportException(
          s"Native Kafka offset-log handoff could not read metadata path from " +
            s"${stream.getClass.getName}",
          e)
    }
  }

  private def hadoopConf(): Configuration = {
    SparkSession.getActiveSession
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
  }

  private def partitionOffsetsDigest(partitionOffsets: Map[TopicPartition, Long]): String = {
    val canonical = sortTopicPartitions(partitionOffsets.keySet)
      .map {
        topicPartition =>
          s"${topicPartition.topic()}\u0000${topicPartition.partition()}\u0000" +
            s"${partitionOffsets(topicPartition)}"
      }
      .mkString("\u0001")
    MessageDigest
      .getInstance("SHA-256")
      .digest(canonical.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
  }
}
