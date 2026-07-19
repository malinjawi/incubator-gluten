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
package org.apache.gluten.extension.columnar.heuristic

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.columnar.{FallbackTag, FallbackTags}
import org.apache.gluten.extension.columnar.FallbackTags.add
import org.apache.gluten.extension.columnar.transition.BackendTransitions
import org.apache.gluten.extension.columnar.validator.StreamingPlanSupport

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.SparkPlan

case class StreamingQueryFallbackPolicy(
    isStreaming: Boolean,
    glutenConf: GlutenConfig,
    originalPlan: SparkPlan)
  extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    val originalIsStreaming = isStreaming || StreamingPlanSupport.isStreamingPlan(originalPlan)
    val candidateIsStreaming = StreamingPlanSupport.isStreamingPlan(plan)
    if (
      (!originalIsStreaming && !candidateIsStreaming) ||
      plan.isInstanceOf[FallbackNode]
    ) {
      return plan
    }

    val fallbackReason = if (!glutenConf.enableNativeStreaming) {
      Some(
        "Native Spark Structured Streaming is disabled. Set " +
          "spark.gluten.sql.streaming.native.enabled=true only for guarded native " +
          "streaming experiments.")
    } else if (
      glutenConf.enableNativeStreamingFull &&
      hasSparkOwnedStreamingBridge(glutenConf)
    ) {
      Some(
        "Full native Spark Structured Streaming forbids Spark-owned source, sink, and state " +
          s"bridges; disable ${GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key}, " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key}, " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key}, and " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}.")
    } else if (
      !glutenConf.enableNativeStreamingSparkBridgeExecution &&
      (glutenConf.enableNativeStreamingSparkInput ||
        glutenConf.enableNativeStreamingSparkOutput ||
        glutenConf.enableNativeStreamingSparkState)
    ) {
      Some(
        "Spark-owned native streaming source, sink, or state bridges are configured, but " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}=false.")
    } else {
      StreamingPlanSupport.firstUnsupportedNativeStreamingBoundary(originalPlan, glutenConf)
    }

    if (fallbackReason.isEmpty) {
      return plan
    }

    val fallbackPlan = BackendTransitions.insert(originalPlan, plan.supportsColumnar)
    StreamingQueryFallbackPolicy.addFallbackTagsRecursively(
      fallbackPlan,
      FallbackTag.Exclusive(fallbackReason.get)
    )
    FallbackNode(fallbackPlan)
  }

  private def hasSparkOwnedStreamingBridge(glutenConf: GlutenConfig): Boolean = {
    glutenConf.enableNativeStreamingSparkInput ||
    glutenConf.enableNativeStreamingSparkOutput ||
    glutenConf.enableNativeStreamingSparkState ||
    glutenConf.enableNativeStreamingSparkBridgeExecution
  }
}

object StreamingQueryFallbackPolicy {
  private def addFallbackTagsRecursively[T](plan: TreeNode[_], t: T)(implicit
      converter: FallbackTag.Converter[T]): Unit = {
    plan.foreach {
      case other: TreeNode[_] if FallbackTags.getOption(other).isEmpty => add(other, t)
      case _ =>
    }
  }
}
