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
import org.apache.gluten.execution.MicroBatchScanExecTransformer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2ScanRelation
import org.apache.spark.sql.kafka010.GlutenStreamKafkaSourceUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap, Set => JSet}

case class GlutenKafkaOffsetLogHandoffRule() extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!GlutenConfig.get.enableNativeStreamingKafkaSourceOffsetLogHandoff) {
      return plan
    }

    plan.resolveOperatorsUp {
      case relation: StreamingRelationV2 =>
        GlutenKafkaOffsetLogHandoffRule.wrapKafkaTableIfNeeded(relation.table) match {
          case Some(wrapped) => relation.copy(table = wrapped)
          case None => relation
        }

      case relation: StreamingDataSourceV2ScanRelation
          if relation.startOffset.isDefined &&
            relation.endOffset.isDefined &&
            MicroBatchScanExecTransformer.supportsBatchScan(relation.scan) =>
        relation.stream match {
          case stream: MicroBatchStream =>
            GlutenKafkaOffsetLogHandoffRule.validateSubscriptionOptions(relation.relation.options)
            val wrapped = GlutenStreamKafkaSourceUtil.wrapKafkaMicroBatchStreamIfNeeded(
              stream,
              GlutenKafkaOffsetLogHandoffRule.subscribePattern(relation.relation.options))
            if (wrapped eq stream) {
              relation
            } else {
              relation.copy(stream = wrapped)
            }
          case _ => relation
        }
    }
  }
}

object GlutenKafkaOffsetLogHandoffRule {
  private val KafkaTableClassName = "org.apache.spark.sql.kafka010.KafkaSourceProvider$KafkaTable"
  private val SubscribePatternOption = "subscribePattern"

  private def wrapKafkaTableIfNeeded(table: Table): Option[Table] = table match {
    case _: GlutenKafkaOffsetLogHandoffTable => None
    case read: SupportsRead if table.getClass.getName == KafkaTableClassName =>
      Some(new GlutenKafkaOffsetLogHandoffTable(read))
    case _ => None
  }

  private[kafka] def subscribePattern(options: CaseInsensitiveStringMap): Option[String] =
    Option(options.get(SubscribePatternOption)).filter(_.nonEmpty)

  private[kafka] def validateSubscriptionOptions(options: CaseInsensitiveStringMap): Unit =
    subscribePattern(options).foreach {
      pattern =>
        GlutenStreamKafkaSourceUtil.validateNativeTopicPatternDiscoveryForHandoff(pattern, options)
    }
}

final private class GlutenKafkaOffsetLogHandoffTable(delegate: SupportsRead)
  extends Table
  with SupportsRead
  with SupportsWrite {

  override def name(): String = delegate.name()

  override def id(): String = delegate.id()

  override def schema(): StructType = delegate.schema()

  override def columns(): Array[Column] = delegate.columns()

  override def partitioning(): Array[Transform] = delegate.partitioning()

  override def properties(): JMap[String, String] = delegate.properties()

  override def capabilities(): JSet[TableCapability] = delegate.capabilities()

  override def constraints(): Array[Constraint] = delegate.constraints()

  override def version(): String = delegate.version()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    GlutenKafkaOffsetLogHandoffRule.validateSubscriptionOptions(options)
    new GlutenKafkaOffsetLogHandoffScanBuilder(
      delegate.newScanBuilder(options),
      GlutenKafkaOffsetLogHandoffRule.subscribePattern(options))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = delegate match {
    case writable: SupportsWrite => writable.newWriteBuilder(info)
    case other =>
      throw new UnsupportedOperationException(
        s"Kafka offset-log handoff table wrapper expected SupportsWrite, got " +
          s"${other.getClass.getName}")
  }

  override def toString: String = delegate.toString
}

final private class GlutenKafkaOffsetLogHandoffScanBuilder(
    delegate: ScanBuilder,
    subscribePattern: Option[String])
  extends ScanBuilder {
  override def build(): Scan =
    new GlutenKafkaOffsetLogHandoffScan(delegate.build(), subscribePattern)
}

final private class GlutenKafkaOffsetLogHandoffScan(
    delegate: Scan,
    subscribePattern: Option[String])
  extends Scan {
  override def readSchema(): StructType = delegate.readSchema()

  override def description(): String = delegate.description()

  override def toBatch(): Batch = delegate.toBatch()

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    GlutenStreamKafkaSourceUtil
      .wrapKafkaMicroBatchStreamIfNeeded(
        delegate.toMicroBatchStream(checkpointLocation),
        subscribePattern)
      .asInstanceOf[MicroBatchStream]
  }

  override def toContinuousStream(checkpointLocation: String): ContinuousStream =
    delegate.toContinuousStream(checkpointLocation)

  override def supportedCustomMetrics(): Array[CustomMetric] = delegate.supportedCustomMetrics()

  override def reportDriverMetrics(): Array[CustomTaskMetric] = delegate.reportDriverMetrics()

  override def columnarSupportMode(): Scan.ColumnarSupportMode = delegate.columnarSupportMode()
}
