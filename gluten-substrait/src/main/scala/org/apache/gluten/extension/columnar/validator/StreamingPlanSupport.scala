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
package org.apache.gluten.extension.columnar.validator

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.SparkOwnedMicroBatchScanExec
import org.apache.gluten.execution.streaming.NativeStreamingStatefulOperatorExec
import org.apache.gluten.extension.columnar.heuristic.RewrittenNodeWall

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.execution.datasources.v2.OverwriteByExpressionExec
import org.apache.spark.sql.execution.datasources.v2.OverwritePartitionsDynamicExec
import org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec

import java.util.Locale

object StreamingPlanSupport {
  val FullNativeStatefulOperatorRequirement: String =
    "Full native Spark Structured Streaming requires native stateful physical operators; " +
      "the native StateStore provider is provider-only and does not make Spark stateful " +
      "operators native."

  val FullNativeKafkaOffsetLogRequirement: String =
    "Full native Spark Structured Streaming requires native Kafka offset-log handoff; " +
      "Spark-planned finite Kafka splits do not satisfy native source ownership."

  val FullNativeKafkaNativeOffsetPlanningRequirement: String =
    "Full native Spark Structured Streaming requires native Kafka offset planning and " +
      "broker-discovery validation before Spark split planning; Spark/JVM-discovered Kafka " +
      "partitions do not satisfy native source ownership."

  def nativeStatefulOperatorGateRequirement(kind: String): String = {
    s"Native Spark Structured Streaming stateful operator '$kind' requires its operator-specific " +
      "execution gate before it can satisfy full-native state ownership."
  }

  val StatefulStreamingOperatorClassFragments: Seq[String] = Seq(
    "StateStore",
    "StreamingDeduplicate",
    "StreamingSymmetricHashJoin",
    "StreamingGlobalLimit",
    "FlatMapGroupsWithState",
    "FlatMapGroupsInPandasWithState",
    "MapGroupsWithState",
    "TransformWithState"
  )

  def isStatefulStreamingClassName(className: String): Boolean = {
    StatefulStreamingOperatorClassFragments.exists(className.contains)
  }

  def isNativeStreamingStatefulPlan(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[NativeStreamingStatefulOperatorExec]
  }

  def isSparkOwnedStatefulStreamingPlan(plan: SparkPlan): Boolean = {
    !isNativeStreamingStatefulPlan(plan) && {
      plan match {
        case aggregate: BaseAggregateExec if aggregate.isStreaming => true
        case _ =>
          val className = plan.getClass.getName
          isStatefulStreamingClassName(className)
      }
    }
  }

  def nativeStatefulOperatorGateFailure(
      plan: SparkPlan,
      glutenConf: GlutenConfig): Option[String] = {
    plan match {
      case native: NativeStreamingStatefulOperatorExec =>
        native.nativeStatefulOperatorKind match {
          case kind
              if NativeStreamingStatefulOperatorExec.DeduplicateOperatorKinds.contains(kind) &&
                glutenConf.enableNativeStreamingStatefulDeduplicate =>
            None
          case kind
              if NativeStreamingStatefulOperatorExec.AggregationOperatorKinds.contains(kind) &&
                glutenConf.enableNativeStreamingStatefulAggregation =>
            None
          case kind =>
            Some(nativeStatefulOperatorGateRequirement(kind))
        }
      case _ => None
    }
  }

  def isKafkaMicroBatchScan(plan: MicroBatchScanExec): Boolean = {
    val scanClass = plan.scan.getClass.getName
    val streamClass = plan.stream.getClass.getName
    val description = Option(plan.scan.description()).getOrElse("")

    Seq(scanClass, streamClass, description).exists(_.toLowerCase(Locale.ROOT).contains("kafka"))
  }

  def describeMicroBatchScan(plan: MicroBatchScanExec): String = {
    val description = Option(plan.scan.description()).getOrElse(plan.nodeName)
    s"$description (${plan.scan.getClass.getName})"
  }

  private def isDataSourceV2Write(plan: SparkPlan): Boolean = plan match {
    case _: AppendDataExec | _: ReplaceDataExec | _: OverwriteByExpressionExec |
        _: OverwritePartitionsDynamicExec | _: WriteToDataSourceV2Exec =>
      true
    case _ => false
  }

  def isStreamingSink(plan: SparkPlan): Boolean = {
    isDataSourceV2Write(plan) && plan.children.exists(isStreamingPlan)
  }

  def isStreamingPlan(plan: SparkPlan): Boolean = {
    plan.exists {
      case _: MicroBatchScanExec => true
      case _: SparkOwnedMicroBatchScanExec => true
      case wall: RewrittenNodeWall if isStreamingPlan(wall.originalChild) => true
      case sink if isStreamingSink(sink) => true
      case stateful if isStatefulStreamingPlan(stateful) => true
      case _ => false
    }
  }

  def containsStatefulStreamingPlan(plan: SparkPlan): Boolean = {
    plan.exists {
      case wall: RewrittenNodeWall if containsStatefulStreamingPlan(wall.originalChild) => true
      case stateful if isStatefulStreamingPlan(stateful) => true
      case _ => false
    }
  }

  def containsSparkOwnedStatefulStreamingPlan(plan: SparkPlan): Boolean = {
    plan.exists {
      case wall: RewrittenNodeWall if containsSparkOwnedStatefulStreamingPlan(wall.originalChild) =>
        true
      case stateful if isSparkOwnedStatefulStreamingPlan(stateful) => true
      case _ => false
    }
  }

  def isStatefulStreamingPlan(plan: SparkPlan): Boolean = {
    isNativeStreamingStatefulPlan(plan) || isSparkOwnedStatefulStreamingPlan(plan)
  }

  def firstUnsupportedNativeStreamingBoundary(
      plan: SparkPlan,
      glutenConf: GlutenConfig): Option[String] = {
    if (glutenConf.enableNativeStreamingFull && hasSparkOwnedStreamingBridge(glutenConf)) {
      return Some(
        "Full native Spark Structured Streaming forbids Spark-owned source, sink, and state " +
          s"bridges; disable ${GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key}, " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key}, " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key}, and " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}.")
    }

    plan.collectFirst {
      case native
          if isNativeStreamingStatefulPlan(native) &&
            nativeStatefulOperatorGateFailure(native, glutenConf).isDefined =>
        nativeStatefulOperatorGateFailure(native, glutenConf).get
      case stateful
          if isSparkOwnedStatefulStreamingPlan(stateful) && glutenConf.enableNativeStreamingFull =>
        FullNativeStatefulOperatorRequirement
      case scan: MicroBatchScanExec
          if glutenConf.enableNativeStreamingFull &&
            glutenConf.enableNativeStreamingKafkaSource &&
            isKafkaMicroBatchScan(scan) &&
            !glutenConf.enableNativeStreamingKafkaSourceOffsetLogHandoff =>
        FullNativeKafkaOffsetLogRequirement
      case scan: MicroBatchScanExec
          if glutenConf.enableNativeStreamingFull &&
            glutenConf.enableNativeStreamingKafkaSource &&
            isKafkaMicroBatchScan(scan) &&
            !glutenConf.enableNativeStreamingKafkaSourceNativeOffsetPlanning =>
        FullNativeKafkaNativeOffsetPlanningRequirement
      case scan: MicroBatchScanExec
          if glutenConf.enableNativeStreamingKafkaSource && isKafkaMicroBatchScan(scan) &&
            !glutenConf.enableNativeStreamingKafkaSourceExecution &&
            !(glutenConf.enableNativeStreamingSparkInput &&
              glutenConf.enableNativeStreamingSparkBridgeExecution) =>
        s"Native Kafka micro-batch source execution is disabled by " +
          s"${GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key}"
      case scan: MicroBatchScanExec
          if glutenConf.enableNativeStreamingKafkaSource && !isKafkaMicroBatchScan(scan) &&
            !glutenConf.enableNativeStreamingSparkInput =>
        s"Native Spark Structured Streaming source execution currently only supports Kafka " +
          s"micro-batch scans; found ${describeMicroBatchScan(scan)}"
      case scan: MicroBatchScanExec
          if glutenConf.enableNativeStreamingSparkInput &&
            !glutenConf.enableNativeStreamingSparkBridgeExecution &&
            !(glutenConf.enableNativeStreamingKafkaSource && isKafkaMicroBatchScan(scan)) =>
        s"Spark-owned streaming source input bridge execution is disabled by " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}"
      case scan: MicroBatchScanExec
          if !glutenConf.enableNativeStreamingKafkaSource &&
            !glutenConf.enableNativeStreamingSparkInput =>
        s"Native Spark Structured Streaming sources are disabled by " +
          s"${GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key}, and the Spark-owned " +
          s"source input bridge is disabled by " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key}"
      case sink
          if isStreamingSink(sink) && !glutenConf.enableNativeStreamingSink &&
            glutenConf.enableNativeStreamingSparkOutput &&
            !glutenConf.enableNativeStreamingSparkBridgeExecution =>
        s"Spark-owned streaming sink output bridge execution is disabled by " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}"
      case sink
          if isStreamingSink(sink) && !glutenConf.enableNativeStreamingSink &&
            !glutenConf.enableNativeStreamingSparkOutput =>
        s"Native Spark Structured Streaming sinks are disabled by " +
          s"${GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key}, and the Spark-owned sink " +
          s"output bridge is disabled by " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key}"
      case stateful
          if isSparkOwnedStatefulStreamingPlan(stateful) &&
            !glutenConf.enableNativeStreamingStateStore &&
            glutenConf.enableNativeStreamingSparkState &&
            !glutenConf.enableNativeStreamingSparkBridgeExecution =>
        s"Spark-owned streaming state bridge execution is disabled by " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}"
      case stateful
          if isSparkOwnedStatefulStreamingPlan(stateful) &&
            !glutenConf.enableNativeStreamingStateStore &&
            !glutenConf.enableNativeStreamingSparkState =>
        "Spark streaming state remains Spark-owned until native streaming state or the " +
          s"Spark-owned state bridge is explicitly enabled; guarded by " +
          s"${GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key} and " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key}"
    }
  }

  private def hasSparkOwnedStreamingBridge(glutenConf: GlutenConfig): Boolean = {
    glutenConf.enableNativeStreamingSparkInput ||
    glutenConf.enableNativeStreamingSparkOutput ||
    glutenConf.enableNativeStreamingSparkState ||
    glutenConf.enableNativeStreamingSparkBridgeExecution
  }
}
