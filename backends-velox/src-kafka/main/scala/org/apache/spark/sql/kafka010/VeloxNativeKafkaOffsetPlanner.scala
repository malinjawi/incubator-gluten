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

import org.apache.gluten.config.ConfigJniWrapper
import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.kafka.common.TopicPartition

import java.util.{Map => JMap}
import java.util.regex.Pattern

import scala.collection.JavaConverters._

object VeloxNativeKafkaOffsetPlanner {
  private val ResultLength = 8
  private val FlagTrue = 1L
  private val DiscoveryResultEntryLength = 2
  private val DiscoveryMetadataEntryLength = 4
  private val DiscoveryTimeoutMs = 30000L

  case class PlannedRange(
      startOffset: Long,
      endOffset: Long,
      lowWatermark: Long,
      highWatermark: Long,
      skippedMessages: Long,
      advancedStartOffset: Boolean,
      resetStartToHighWatermark: Boolean,
      limitedByMaxOffsetsPerTrigger: Boolean)

  private case class TopicPartitionMetadata(
      topic: String,
      partition: Int,
      failureKind: String,
      error: String)

  def validateSparkPlannedRange(batch: KafkaBatchInputPartition): PlannedRange = {
    val planned = planSparkMicroBatchRange(batch)
    val sparkRange = batch.offsetRange
    if (
      planned.startOffset != sparkRange.fromOffset ||
      planned.endOffset != sparkRange.untilOffset
    ) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset planning validation produced " +
          s"[${planned.startOffset}, ${planned.endOffset}) for " +
          s"${sparkRange.topicPartition}, but Spark planned " +
          s"[${sparkRange.fromOffset}, ${sparkRange.untilOffset}). Spark offset logs remain " +
          s"authoritative until native offset-log ownership is implemented.")
    }
    planned
  }

  def planSparkMicroBatchRange(batch: KafkaBatchInputPartition): PlannedRange = {
    val sparkRange = batch.offsetRange
    val topicPartition = sparkRange.topicPartition
    val params = batch.executorKafkaParams.asScala.toSeq
      .map { case (key, value) => key.toString -> value.toString }
      .sortBy(_._1)
    val maxOffsetsPerTrigger = sparkRange.untilOffset - sparkRange.fromOffset
    if (maxOffsetsPerTrigger < 0) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset planning requires Spark-planned untilOffset >= fromOffset, got " +
          s"${sparkRange.topicPartition} fromOffset=${sparkRange.fromOffset} " +
          s"untilOffset=${sparkRange.untilOffset}")
    }

    decodeResult(
      ConfigJniWrapper.planKafkaMicroBatchRange(
        topicPartition.topic(),
        topicPartition.partition(),
        sparkRange.fromOffset,
        true,
        maxOffsetsPerTrigger,
        batch.pollTimeoutMs,
        batch.failOnDataLoss,
        batch.includeHeaders,
        params.map(_._1).toArray,
        params.map(_._2).toArray
      ))
  }

  def discoverTopicPartitions(batches: Array[KafkaBatchInputPartition]): Array[TopicPartition] = {
    if (batches == null || batches.isEmpty) {
      return Array.empty
    }

    val topics = batches.toSeq
      .map(_.offsetRange.topicPartition.topic())
      .distinct
      .sorted
    discoverTopicPartitions(topics.toArray, batches.head.executorKafkaParams)
  }

  def discoverTopicPartitions(
      topics: Array[String],
      kafkaParams: JMap[String, Object]): Array[TopicPartition] = {
    if (topics == null || topics.isEmpty) {
      return Array.empty
    }
    if (!kafkaParams.containsKey("bootstrap.servers")) {
      throw new GlutenNotSupportException(
        "Native Kafka partition discovery requires bootstrap.servers in executor Kafka params")
    }

    val sortedTopics = topics.toSeq.distinct.sorted
    val params = kafkaParams.asScala.toSeq
      .map { case (key, value) => key.toString -> value.toString }
      .sortBy(_._1)
    decodeDiscoveredTopicPartitions(
      ConfigJniWrapper.discoverKafkaTopicPartitions(
        sortedTopics.toArray,
        DiscoveryTimeoutMs,
        params.map(_._1).toArray,
        params.map(_._2).toArray
      ))
  }

  def discoverTopicPartitionsForPattern(
      topicPattern: String,
      kafkaParams: JMap[String, Object]): Array[TopicPartition] = {
    if (topicPattern == null || topicPattern.isEmpty) {
      throw new GlutenNotSupportException(
        "Native Kafka pattern partition discovery requires a non-empty subscribePattern")
    }
    if (!kafkaParams.containsKey("bootstrap.servers")) {
      throw new GlutenNotSupportException(
        "Native Kafka pattern partition discovery requires bootstrap.servers in Kafka params")
    }

    val compiled = Pattern.compile(topicPattern)
    val params = kafkaParams.asScala.toSeq
      .map { case (key, value) => key.toString -> value.toString }
      .sortBy(_._1)
    val discovered = decodeDiscoveredTopicPartitionMetadata(
      ConfigJniWrapper.listKafkaTopicPartitionMetadata(
        DiscoveryTimeoutMs,
        params.map(_._1).toArray,
        params.map(_._2).toArray
      ))
      .filter(topicPartition => compiled.matcher(topicPartition.topic).matches())
    discovered
      .find(_.error.nonEmpty)
      .foreach {
        topicPartition =>
          throw new GlutenNotSupportException(
            s"Native Kafka pattern partition discovery ${topicPartition.failureKind} " +
              s"metadata failure for topic=${topicPartition.topic} " +
              s"partition=${topicPartition.partition}: ${topicPartition.error}")
      }
    discovered
      .map(topicPartition => new TopicPartition(topicPartition.topic, topicPartition.partition))
      .sortBy(topicPartition => (topicPartition.topic(), topicPartition.partition()))
  }

  private[kafka010] def decodeResult(raw: Array[Long]): PlannedRange = {
    if (raw == null || raw.length != ResultLength) {
      throw new GlutenNotSupportException(
        s"Native Kafka offset planner returned invalid result length " +
          s"${if (raw == null) "null" else raw.length.toString}")
    }
    PlannedRange(
      startOffset = raw(0),
      endOffset = raw(1),
      lowWatermark = raw(2),
      highWatermark = raw(3),
      skippedMessages = raw(4),
      advancedStartOffset = raw(5) == FlagTrue,
      resetStartToHighWatermark = raw(6) == FlagTrue,
      limitedByMaxOffsetsPerTrigger = raw(7) == FlagTrue
    )
  }

  private[kafka010] def decodeDiscoveredTopicPartitions(raw: Array[String])
      : Array[TopicPartition] = {
    if (raw == null || raw.length % DiscoveryResultEntryLength != 0) {
      throw new GlutenNotSupportException(
        s"Native Kafka partition discovery returned invalid result length " +
          s"${if (raw == null) "null" else raw.length.toString}")
    }
    raw
      .grouped(DiscoveryResultEntryLength)
      .map {
        entry =>
          val topic = entry(0)
          val partitionText = entry(1)
          if (topic == null || topic.isEmpty) {
            throw new GlutenNotSupportException(
              "Native Kafka partition discovery returned an empty topic")
          }
          val partition =
            try {
              partitionText.toInt
            } catch {
              case e: NumberFormatException =>
                throw new GlutenNotSupportException(
                  s"Native Kafka partition discovery returned invalid partition " +
                    s"'$partitionText' for topic $topic",
                  e)
            }
          if (partition < 0) {
            throw new GlutenNotSupportException(
              s"Native Kafka partition discovery returned negative partition $partition " +
                s"for topic $topic")
          }
          new TopicPartition(topic, partition)
      }
      .toArray
      .sortBy(topicPartition => (topicPartition.topic(), topicPartition.partition()))
  }

  private def decodeDiscoveredTopicPartitionMetadata(raw: Array[String])
      : Array[TopicPartitionMetadata] = {
    if (raw == null || raw.length % DiscoveryMetadataEntryLength != 0) {
      throw new GlutenNotSupportException(
        s"Native Kafka topic metadata discovery returned invalid result length " +
          s"${if (raw == null) "null" else raw.length.toString}")
    }
    raw
      .grouped(DiscoveryMetadataEntryLength)
      .map {
        entry =>
          val topic = entry(0)
          val partitionText = entry(1)
          val failureKind = entry(2)
          val error = Option(entry(3)).getOrElse("")
          if (topic == null || topic.isEmpty) {
            throw new GlutenNotSupportException(
              "Native Kafka topic metadata discovery returned an empty topic")
          }
          val partition =
            try {
              partitionText.toInt
            } catch {
              case e: NumberFormatException =>
                throw new GlutenNotSupportException(
                  s"Native Kafka topic metadata discovery returned invalid partition " +
                    s"'$partitionText' for topic $topic",
                  e)
            }
          if (partition < 0 && error.isEmpty) {
            throw new GlutenNotSupportException(
              s"Native Kafka topic metadata discovery returned negative partition $partition " +
                s"for topic $topic without an error")
          }
          TopicPartitionMetadata(
            topic,
            partition,
            Option(failureKind).filter(_.nonEmpty).getOrElse("fatal"),
            error)
      }
      .toArray
      .sortBy(topicPartition => (topicPartition.topic, topicPartition.partition))
  }
}
