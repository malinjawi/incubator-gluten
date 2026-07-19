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

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, PartitionOffset, ReadLimit, ReportsSourceMetrics, SupportsAdmissionControl, SupportsRealTimeMode, SupportsTriggerAvailableNow}

import org.apache.kafka.common.TopicPartition

import java.util.Optional

class GlutenNativeKafkaMicroBatchStream private[kafka010] (
    delegate: MicroBatchStream,
    nativeDiscoveryOverride: Option[(MicroBatchStream, Offset) => Option[Set[TopicPartition]]])
  extends MicroBatchStream
  with SupportsTriggerAvailableNow
  with SupportsRealTimeMode
  with ReportsSourceMetrics {

  def this(delegate: MicroBatchStream) = this(delegate, None)

  @volatile private var preparedBatch: Option[PreparedBatch] = None

  override def initialOffset(): Offset = delegate.initialOffset()

  override def latestOffset(): Offset = delegate.latestOffset()

  override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
    val endOffset = admissionDelegate.latestOffset(startOffset, limit)
    prepareOffsetLogHandoffAfterLatestOffset(startOffset, endOffset)
    endOffset
  }

  override def reportLatestOffset(): Offset =
    admissionDelegate.reportLatestOffset()

  override def getDefaultReadLimit(): ReadLimit =
    admissionDelegate.getDefaultReadLimit()

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] =
    prepareOffsetLogHandoff(start, end, requirePersistedManifest = false)

  private[kafka010] def prepareOffsetLogHandoffForOffsetLogWrite(
      start: Offset,
      end: Offset): Unit = {
    prepareOffsetLogHandoff(start, end, requirePersistedManifest = false)
  }

  private[kafka010] def validateOffsetLogHandoffForOffsetLogReplay(
      start: Offset,
      end: Offset): Unit = {
    prepareOffsetLogHandoff(start, end, requirePersistedManifest = true)
  }

  private def prepareOffsetLogHandoffAfterLatestOffset(start: Offset, end: Offset): Unit = {
    if (start == null || end == null || start.json() == end.json()) {
      return
    }
    prepareOffsetLogHandoff(start, end, requirePersistedManifest = false)
  }

  private def prepareOffsetLogHandoff(
      start: Offset,
      end: Offset,
      requirePersistedManifest: Boolean): Array[InputPartition] = synchronized {
    val normalizedStart = deserializeDelegateOffset(start)
    val normalizedEnd = deserializeDelegateOffset(end)
    val key = PreparedBatchKey(normalizedStart.json(), normalizedEnd.json())
    preparedBatch match {
      case Some(prepared) if prepared.key == key =>
        return prepared.partitions
      case _ =>
    }

    val persistedReplayManifest =
      if (requirePersistedManifest) {
        Some(GlutenStreamKafkaSourceUtil.readPersistedOffsetLogHandoffManifestForReplay(
          delegate,
          normalizedEnd))
      } else {
        None
      }
    val nativeDiscovered = persistedReplayManifest
      .map(_.nativeDiscoveredTopicPartitions)
      .getOrElse(
        nativeDiscoveryOverride
          .map(_(delegate, normalizedEnd))
          .getOrElse(GlutenStreamKafkaSourceUtil.discoverNativeTopicPartitionsForHandoff(
            delegate,
            normalizedEnd)))
    if (normalizedStart.json() != normalizedEnd.json()) {
      GlutenStreamKafkaSourceUtil.validateNativeDiscoveryForHandoff(
        normalizedEnd,
        nativeDiscovered)
    }

    val partitions = delegate.planInputPartitions(normalizedStart, normalizedEnd)
    val partitionSeq = partitions.toSeq
    if (requirePersistedManifest) {
      GlutenStreamKafkaSourceUtil.validatePersistedOffsetLogHandoff(
        delegate,
        normalizedStart,
        normalizedEnd,
        partitionSeq,
        nativeDiscovered)
    } else {
      GlutenStreamKafkaSourceUtil.prepareOffsetLogHandoff(
        delegate,
        normalizedStart,
        normalizedEnd,
        partitionSeq,
        nativeDiscovered)
    }
    preparedBatch = Some(PreparedBatch(key, partitions))
    partitions
  }

  private def deserializeDelegateOffset(offset: Offset): Offset = {
    delegate.deserializeOffset(offset.json())
  }

  override def createReaderFactory(): PartitionReaderFactory = delegate.createReaderFactory()

  override def deserializeOffset(json: String): Offset = delegate.deserializeOffset(json)

  override def commit(end: Offset): Unit = delegate.commit(end)

  override def stop(): Unit = delegate.stop()

  override def prepareForTriggerAvailableNow(): Unit =
    triggerAvailableNowDelegate.prepareForTriggerAvailableNow()

  override def planInputPartitions(start: Offset): Array[InputPartition] =
    realTimeDelegate.planInputPartitions(start)

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset =
    realTimeDelegate.mergeOffsets(offsets)

  override def prepareForRealTimeMode(): Unit =
    realTimeDelegate.prepareForRealTimeMode()

  override def metrics(latestConsumedOffset: Optional[Offset]): java.util.Map[String, String] =
    metricsDelegate.metrics(latestConsumedOffset)

  override def toString: String = s"GlutenNativeKafkaMicroBatchStream($delegate)"

  private def admissionDelegate: SupportsAdmissionControl = delegate match {
    case supported: SupportsAdmissionControl => supported
    case other =>
      throw new GlutenNotSupportException(
        s"Native Kafka micro-batch stream wrapper requires SupportsAdmissionControl, got " +
          s"${other.getClass.getName}")
  }

  private def triggerAvailableNowDelegate: SupportsTriggerAvailableNow = delegate match {
    case supported: SupportsTriggerAvailableNow => supported
    case other =>
      throw new GlutenNotSupportException(
        s"Native Kafka micro-batch stream wrapper requires SupportsTriggerAvailableNow, got " +
          s"${other.getClass.getName}")
  }

  private def realTimeDelegate: SupportsRealTimeMode = delegate match {
    case supported: SupportsRealTimeMode => supported
    case other =>
      throw new GlutenNotSupportException(
        s"Native Kafka micro-batch stream wrapper requires SupportsRealTimeMode, got " +
          s"${other.getClass.getName}")
  }

  private def metricsDelegate: ReportsSourceMetrics = delegate match {
    case supported: ReportsSourceMetrics => supported
    case other =>
      throw new GlutenNotSupportException(
        s"Native Kafka micro-batch stream wrapper requires ReportsSourceMetrics, got " +
          s"${other.getClass.getName}")
  }
}

private case class PreparedBatchKey(startOffsetJson: String, endOffsetJson: String)

private case class PreparedBatch(key: PreparedBatchKey, partitions: Array[InputPartition])
