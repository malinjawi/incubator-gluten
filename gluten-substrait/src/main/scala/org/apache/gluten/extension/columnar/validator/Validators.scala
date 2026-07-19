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

import org.apache.gluten.backendsapi.{BackendsApiManager, BackendSettingsApi}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.expression.ExpressionUtils
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, BatchScanExec, MicroBatchScanExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec, ReplaceDataExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

object Validators {
  implicit class ValidatorBuilderImplicits(builder: Validator.Builder) {
    private lazy val conf = GlutenConfig.get
    private lazy val settings = BackendsApiManager.getSettings

    // Get VeloxConfig if available
    private lazy val veloxConf: Option[Any] = {
      try {
        // scalastyle:off classforname
        val veloxConfigClass = Class.forName("org.apache.gluten.config.VeloxConfig")
        // scalastyle:on classforname
        val getMethod = veloxConfigClass.getMethod("get")
        Some(getMethod.invoke(null))
      } catch {
        case _: Exception => None
      }
    }

    /** Fails validation if a plan node was already tagged with TRANSFORM_UNSUPPORTED. */
    def fallbackByHint(): Validator.Builder = {
      builder.add(FallbackByHint)
    }

    /**
     * Fails validation if a plan node includes an expression that is considered too complex to
     * executed by native library. By default, we use a threshold option in config to make the
     * decision.
     */
    def fallbackComplexExpressions(): Validator.Builder = {
      builder.add(new FallbackComplexExpressions(conf.fallbackExpressionsThreshold))
    }

    /** Fails validation on non-scan plan nodes if Gluten is running as scan-only mode. */
    def fallbackIfScanOnly(): Validator.Builder = {
      builder.add(new FallbackIfScanOnly(conf.enableScanOnly))
    }

    /**
     * Fails validation if native-execution of a plan node is not supported by current backend
     * implementation by checking the active BackendSettings.
     */
    def fallbackByBackendSettings(): Validator.Builder = {
      builder.add(new FallbackByBackendSettings(settings))
    }

    /**
     * Fails validation if native-execution of a plan node is disabled by Gluten/Spark
     * configuration.
     */
    def fallbackByUserOptions(): Validator.Builder = {
      builder.add(new FallbackByUserOptions(conf))
    }

    /**
     * Fails validation for Spark Structured Streaming nodes unless they are covered by explicit
     * native streaming research gates.
     */
    def fallbackByStreamingOptions(
        isStreaming: Boolean,
        glutenConf: GlutenConfig): Validator.Builder = {
      builder.add(new FallbackByStreamingOptions(isStreaming, glutenConf))
    }

    def fallbackByTestInjects(): Validator.Builder = {
      builder.add(new FallbackByTestInjects())
    }

    /** Fails validation if a plan node's input or output schema contains TimestampNTZType. */
    def fallbackByTimestampNTZ(): Validator.Builder = {
      builder.add(new FallbackByTimestampNTZ(veloxConf))
    }

    /**
     * Fails validation on non-scan plan nodes if Gluten is running as scan-only mode. Also, passes
     * validation on filter for the exception that filter + scan is detected. Because filters can be
     * pushed into scan then the filter conditions will be processed only in scan.
     */
    def fallbackIfScanOnlyWithFilterPushed(scanOnly: Boolean): Validator.Builder = {
      builder.add(new FallbackIfScanOnlyWithFilterPushed(scanOnly))
    }

    /**
     * Attempts to offload the input query plan node and check native validation result. Fails when
     * native validation failed.
     */
    def fallbackByNativeValidation(rules: Seq[OffloadSingleNode]): Validator.Builder = {
      builder.add(new FallbackByNativeValidation(rules))
    }
  }

  private object FallbackByHint extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (FallbackTags.nonEmpty(plan)) {
        val tag = FallbackTags.get(plan)
        return fail(tag.reason())
      }
      pass()
    }
  }

  private class FallbackComplexExpressions(threshold: Int) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (ExpressionUtils.hasComplexExpressions(plan, threshold)) {
        return fail(
          s"Disabled because at least one present expression exceeded depth threshold: " +
            s"${plan.nodeName}")
      }
      pass()
    }
  }

  private class FallbackIfScanOnly(scanOnly: Boolean) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = plan match {
      case _: BatchScanExec => pass()
      case _: FileSourceScanExec => pass()
      case p if HiveTableScanExecTransformer.isHiveTableScan(p) => pass()
      case p if scanOnly => fail(p)
      case _ => pass()
    }
  }

  private class FallbackByBackendSettings(settings: BackendSettingsApi) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = plan match {
      case p: ShuffleExchangeExec if !settings.supportColumnarShuffleExec() => fail(p)
      case p: SortMergeJoinExec if !settings.supportSortMergeJoinExec() => fail(p)
      case p: WriteFilesExec if !settings.enableNativeWriteFiles() =>
        fail(p)
      case p: CartesianProductExec if !settings.supportCartesianProductExec() => fail(p)
      case p: TakeOrderedAndProjectExec if !settings.supportColumnarShuffleExec() => fail(p)
      case p: AppendDataExec if !settings.supportAppendDataExec() => fail(p)
      case p: ReplaceDataExec if !settings.supportReplaceDataExec() => fail(p)
      case p: OverwriteByExpressionExec if !settings.supportOverwriteByExpression() => fail(p)
      case p: OverwritePartitionsDynamicExec if !settings.supportOverwritePartitionsDynamic() =>
        fail(p)
      case p: WriteToDataSourceV2Exec if !settings.supportWriteToDataSourceV2() => fail(p)
      case _ => pass()
    }
  }

  private class FallbackByUserOptions(glutenConf: GlutenConfig) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = plan match {
      case p: SortExec if !glutenConf.enableColumnarSort => fail(p)
      case p: WindowExec if !glutenConf.enableColumnarWindow => fail(p)
      case p: SortMergeJoinExec if !glutenConf.enableColumnarSortMergeJoin => fail(p)
      case p: BatchScanExec if !glutenConf.enableColumnarBatchScan => fail(p)
      case p: FileSourceScanExec if !glutenConf.enableColumnarFileScan => fail(p)
      case p: ProjectExec if !glutenConf.enableColumnarProject => fail(p)
      case p: FilterExec if !glutenConf.enableColumnarFilter => fail(p)
      case p: UnionExec if !glutenConf.enableColumnarUnion => fail(p)
      case p: ExpandExec if !glutenConf.enableColumnarExpand => fail(p)
      case p: SortAggregateExec if !glutenConf.forceToUseHashAgg => fail(p)
      case p: ShuffledHashJoinExec if !glutenConf.enableColumnarShuffledHashJoin => fail(p)
      case p: ShuffleExchangeExec if !glutenConf.enableColumnarShuffle => fail(p)
      case p: BroadcastExchangeExec if !glutenConf.enableColumnarBroadcastExchange => fail(p)
      case p: AppendDataExec if !glutenConf.enableAppendData => fail(p)
      case p: ReplaceDataExec if !glutenConf.enableReplaceData => fail(p)
      case p: OverwriteByExpressionExec if !glutenConf.enableOverwriteByExpression => fail(p)
      case p: OverwritePartitionsDynamicExec if !glutenConf.enableOverwritePartitionsDynamic =>
        fail(p)
      case p: WriteToDataSourceV2Exec if !glutenConf.enableColumnarWriteToDataSourceV2 => fail(p)
      case p @ (_: LocalLimitExec | _: GlobalLimitExec) if !glutenConf.enableColumnarLimit =>
        fail(p)
      case p: GenerateExec if !glutenConf.enableColumnarGenerate => fail(p)
      case p: CoalesceExec if !glutenConf.enableColumnarCoalesce => fail(p)
      case p: CartesianProductExec if !glutenConf.cartesianProductTransformerEnabled => fail(p)
      case p: TakeOrderedAndProjectExec
          if !(glutenConf.enableTakeOrderedAndProject && glutenConf.enableColumnarSort &&
            glutenConf.enableColumnarShuffle && glutenConf.enableColumnarProject) =>
        fail(p)
      case p: BroadcastHashJoinExec if !glutenConf.enableColumnarBroadcastJoin =>
        fail(p)
      case p: BroadcastNestedLoopJoinExec
          if !(glutenConf.enableColumnarBroadcastJoin &&
            glutenConf.broadcastNestedLoopJoinTransformerTransformerEnabled) =>
        fail(p)
      case p @ (_: HashAggregateExec | _: SortAggregateExec | _: ObjectHashAggregateExec)
          if !glutenConf.enableColumnarHashAgg =>
        fail(p)
      case p
          if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(
            plan) && !glutenConf.enableColumnarWindowGroupLimit =>
        fail(p)
      case p
          if HiveTableScanExecTransformer.isHiveTableScan(
            p) && !glutenConf.enableColumnarHiveTableScan =>
        fail(p)
      case p: SampleExec
          if !(glutenConf.enableColumnarSample && BackendsApiManager.getSettings
            .supportSampleExec()) =>
        fail(p)
      case p: RangeExec if !glutenConf.enableColumnarRange => fail(p)
      case p: CollectLimitExec if !glutenConf.enableColumnarCollectLimit => fail(p)
      case p: CollectTailExec if !glutenConf.enableColumnarCollectTail => fail(p)
      case _ => pass()
    }
  }

  private class FallbackByStreamingOptions(isStreamingQuery: Boolean, glutenConf: GlutenConfig)
    extends Validator {
    private def hasExecutableStreamingSourceBoundary: Boolean = {
      (glutenConf.enableNativeStreamingKafkaSource &&
        glutenConf.enableNativeStreamingKafkaSourceExecution &&
        (!glutenConf.enableNativeStreamingFull ||
          (glutenConf.enableNativeStreamingKafkaSourceOffsetLogHandoff &&
            glutenConf.enableNativeStreamingKafkaSourceNativeOffsetPlanning))) ||
      (!glutenConf.enableNativeStreamingFull &&
        glutenConf.enableNativeStreamingSparkInput &&
        glutenConf.enableNativeStreamingSparkBridgeExecution) ||
      (!glutenConf.enableNativeStreamingFull &&
        glutenConf.enableNativeStreamingFileSource)
    }

    private def hasExecutableStreamingSinkBoundary: Boolean = {
      glutenConf.enableNativeStreamingSink ||
      (!glutenConf.enableNativeStreamingFull &&
        glutenConf.enableNativeStreamingSparkOutput &&
        glutenConf.enableNativeStreamingSparkBridgeExecution)
    }

    private def hasExecutableStreamingStateBoundary: Boolean = {
      !glutenConf.enableNativeStreamingFull &&
      (glutenConf.enableNativeStreamingStateStore ||
        (glutenConf.enableNativeStreamingSparkState &&
          glutenConf.enableNativeStreamingSparkBridgeExecution))
    }

    private def hasSparkOwnedStreamingBridge: Boolean = {
      glutenConf.enableNativeStreamingSparkInput ||
      glutenConf.enableNativeStreamingSparkOutput ||
      glutenConf.enableNativeStreamingSparkState ||
      glutenConf.enableNativeStreamingSparkBridgeExecution
    }

    private def isNativeStreamingStatelessOperator(plan: SparkPlan): Boolean = {
      plan.isInstanceOf[ProjectExec] || plan.isInstanceOf[FilterExec]
    }

    private def statelessSourceBoundaryFailureMessage: String = {
      if (glutenConf.enableNativeStreamingFull) {
        "Native stateless Spark Structured Streaming operators require a full-native " +
          "streaming source boundary. Enable native Kafka source execution, native Kafka " +
          s"offset planning, and Kafka offset-log handoff with " +
          s"${GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key}, " +
          s"${GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_NATIVE_OFFSET_PLANNING_ENABLED.key}, " +
          s"and ${GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_OFFSET_LOG_HANDOFF_ENABLED.key}."
      } else {
        "Native stateless Spark Structured Streaming operators require an executable " +
          "streaming source boundary. Enable native Kafka source execution with " +
          s"${GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key}, or " +
          "enable the Spark-owned source input bridge with " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key} and " +
          s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}."
      }
    }

    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (!isStreamingQuery && !StreamingPlanSupport.isStreamingPlan(plan)) {
        return pass()
      }

      if (!glutenConf.enableNativeStreaming) {
        return fail(
          s"Native Spark Structured Streaming is disabled by " +
            s"${GlutenConfig.NATIVE_STREAMING_ENABLED.key}")
      }

      if (glutenConf.enableNativeStreamingFull && hasSparkOwnedStreamingBridge) {
        return fail(
          "Full native Spark Structured Streaming forbids Spark-owned source, sink, and state " +
            s"bridges; disable ${GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key}, " +
            s"${GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key}, " +
            s"${GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key}, and " +
            s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}.")
      }

      plan match {
        case plan
            if isNativeStreamingStatelessOperator(plan) &&
              !glutenConf.enableNativeStreamingStateless =>
          fail(
            s"Native stateless Spark Structured Streaming is disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key}")
        case plan
            if isNativeStreamingStatelessOperator(plan) &&
              !hasExecutableStreamingSourceBoundary =>
          fail(statelessSourceBoundaryFailureMessage)
        case plan
            if isNativeStreamingStatelessOperator(plan) &&
              !hasExecutableStreamingSinkBoundary =>
          fail(
            "Native stateless Spark Structured Streaming operators require an executable " +
              "streaming sink boundary. Enable native streaming sink execution with " +
              s"${GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key}, or enable the Spark-owned " +
              "sink output bridge with " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key} and " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}.")
        case plan
            if isNativeStreamingStatelessOperator(plan) &&
              StreamingPlanSupport.containsSparkOwnedStatefulStreamingPlan(plan) &&
              glutenConf.enableNativeStreamingFull =>
          fail(StreamingPlanSupport.FullNativeStatefulOperatorRequirement)
        case plan
            if isNativeStreamingStatelessOperator(plan) &&
              StreamingPlanSupport.containsSparkOwnedStatefulStreamingPlan(plan) &&
              !hasExecutableStreamingStateBoundary =>
          fail(
            "Spark streaming state remains Spark-owned until native streaming state or the " +
              s"Spark-owned state bridge is explicitly enabled; guarded by " +
              s"${GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key} and " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key}")
        case plan if isNativeStreamingStatelessOperator(plan) =>
          pass()
        case scan: MicroBatchScanExec
            if glutenConf.enableNativeStreamingFull &&
              glutenConf.enableNativeStreamingKafkaSource &&
              StreamingPlanSupport.isKafkaMicroBatchScan(scan) &&
              !glutenConf.enableNativeStreamingKafkaSourceOffsetLogHandoff =>
          fail(StreamingPlanSupport.FullNativeKafkaOffsetLogRequirement)
        case scan: MicroBatchScanExec
            if glutenConf.enableNativeStreamingFull &&
              glutenConf.enableNativeStreamingKafkaSource &&
              StreamingPlanSupport.isKafkaMicroBatchScan(scan) &&
              !glutenConf.enableNativeStreamingKafkaSourceNativeOffsetPlanning =>
          fail(StreamingPlanSupport.FullNativeKafkaNativeOffsetPlanningRequirement)
        case scan: MicroBatchScanExec
            if glutenConf.enableNativeStreamingKafkaSource &&
              StreamingPlanSupport.isKafkaMicroBatchScan(scan) &&
              glutenConf.enableNativeStreamingKafkaSourceExecution =>
          pass()
        case scan: MicroBatchScanExec
            if glutenConf.enableNativeStreamingKafkaSource &&
              StreamingPlanSupport.isKafkaMicroBatchScan(scan) &&
              !glutenConf.enableNativeStreamingKafkaSourceExecution &&
              !(glutenConf.enableNativeStreamingSparkInput &&
                glutenConf.enableNativeStreamingSparkBridgeExecution) =>
          fail(
            s"Native Kafka micro-batch source execution is disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key}")
        case _: MicroBatchScanExec
            if glutenConf.enableNativeStreamingSparkInput &&
              glutenConf.enableNativeStreamingSparkBridgeExecution =>
          pass()
        case _: MicroBatchScanExec if glutenConf.enableNativeStreamingSparkInput =>
          fail(
            s"Spark-owned streaming source input bridge execution is disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}")
        case scan: MicroBatchScanExec if glutenConf.enableNativeStreamingKafkaSource =>
          fail(
            s"Native Spark Structured Streaming source execution currently only supports Kafka " +
              s"micro-batch scans; found ${StreamingPlanSupport.describeMicroBatchScan(scan)}")
        case _: MicroBatchScanExec =>
          fail(
            s"Native Spark Structured Streaming sources are disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key}, and the Spark-owned " +
              s"source input bridge is disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key}")
        case plan
            if StreamingPlanSupport.isStreamingSink(plan) &&
              (glutenConf.enableNativeStreamingSink ||
                (glutenConf.enableNativeStreamingSparkOutput &&
                  glutenConf.enableNativeStreamingSparkBridgeExecution)) =>
          pass()
        case plan
            if StreamingPlanSupport.isStreamingSink(plan) &&
              glutenConf.enableNativeStreamingSparkOutput =>
          fail(
            s"Spark-owned streaming sink output bridge execution is disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}")
        case plan if StreamingPlanSupport.isStreamingSink(plan) =>
          fail(
            s"Native Spark Structured Streaming sinks are disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key}, and the Spark-owned sink " +
              s"output bridge is disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key}")
        case _: AppendDataExec | _: ReplaceDataExec | _: OverwriteByExpressionExec |
            _: OverwritePartitionsDynamicExec | _: WriteToDataSourceV2Exec
            if glutenConf.enableNativeStreamingSink ||
              (glutenConf.enableNativeStreamingSparkOutput &&
                glutenConf.enableNativeStreamingSparkBridgeExecution) =>
          pass()
        case _: AppendDataExec | _: ReplaceDataExec | _: OverwriteByExpressionExec |
            _: OverwritePartitionsDynamicExec | _: WriteToDataSourceV2Exec
            if glutenConf.enableNativeStreamingSparkOutput =>
          fail(
            s"Spark-owned streaming sink output bridge execution is disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}")
        case _: AppendDataExec | _: ReplaceDataExec | _: OverwriteByExpressionExec |
            _: OverwritePartitionsDynamicExec | _: WriteToDataSourceV2Exec =>
          fail(
            s"Native Spark Structured Streaming sinks are disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key}, and the Spark-owned sink " +
              s"output bridge is disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key}")
        case plan
            if StreamingPlanSupport.isNativeStreamingStatefulPlan(plan) =>
          StreamingPlanSupport.nativeStatefulOperatorGateFailure(plan, glutenConf) match {
            case Some(reason) => fail(reason)
            case None => pass()
          }
        case plan
            if StreamingPlanSupport.isSparkOwnedStatefulStreamingPlan(plan) &&
              glutenConf.enableNativeStreamingFull =>
          fail(StreamingPlanSupport.FullNativeStatefulOperatorRequirement)
        case plan
            if StreamingPlanSupport.isSparkOwnedStatefulStreamingPlan(plan) &&
              hasExecutableStreamingStateBoundary =>
          pass()
        case plan
            if StreamingPlanSupport.isSparkOwnedStatefulStreamingPlan(plan) &&
              glutenConf.enableNativeStreamingSparkState =>
          fail(
            s"Spark-owned streaming state bridge execution is disabled by " +
              s"${GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key}")
        case plan if StreamingPlanSupport.isSparkOwnedStatefulStreamingPlan(plan) =>
          fail(
            "Spark streaming state remains Spark-owned until native streaming state or the " +
              "Spark-owned state bridge is explicitly enabled")
        case _: FileSourceScanExec if glutenConf.enableNativeStreamingFileSource =>
          pass()
        // Stateless stream-to-STATIC equi-join gate. A BroadcastHashJoinExec / ShuffledHashJoinExec
        // inside a streaming micro-batch joins the streaming side to a STATIC (batch) relation that
        // Spark has broadcast/shuffled; it carries no join state store. Stateful stream-stream joins
        // are a distinct operator (StreamingSymmetricHashJoin) and are NOT matched here, so they
        // remain Spark-owned. Spark still owns the source, sink, checkpoint logs, and offsets.
        case _: BroadcastHashJoinExec | _: ShuffledHashJoinExec
            if glutenConf.enableNativeStreamingStatelessJoin =>
          pass()
        // The broadcast of the static build side feeds the stateless join above. Passing it lets the
        // build side stay columnar so the native join can consume a native HashedRelation.
        case _: BroadcastExchangeExec if glutenConf.enableNativeStreamingStatelessJoin =>
          pass()
        case _ =>
          fail(
            s"${plan.nodeName} is not in the native Spark Structured Streaming stateless " +
              "operator allowlist")
      }
    }
  }

  private class FallbackByTestInjects() extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (FallbackInjects.shouldFallback(plan)) {
        return fail(plan)
      }
      pass()
    }
  }

  private class FallbackByTimestampNTZ(veloxConf: Option[Any]) extends Validator {
    // Check if TimestampNTZ validation is enabled via VeloxConfig
    // Default to true (enabled) if VeloxConfig is not available or method call fails
    private val enableValidation: Boolean = veloxConf
      .flatMap {
        config =>
          try {
            val enableMethod = config.getClass.getMethod("enableTimestampNtzValidation")
            Some(enableMethod.invoke(config).asInstanceOf[Boolean])
          } catch {
            case _: Exception => None
          }
      }
      .getOrElse(true)

    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (!enableValidation) {
        // Validation is disabled, allow TimestampNTZ
        return pass()
      }

      def containsNTZ(dataType: DataType): Boolean = dataType match {
        case dt if dt.catalogString == "timestamp_ntz" => true
        case st: StructType => st.exists(f => containsNTZ(f.dataType))
        case at: ArrayType => containsNTZ(at.elementType)
        case mt: MapType => containsNTZ(mt.keyType) || containsNTZ(mt.valueType)
        case _ => false
      }
      val hasNTZ = plan.output.exists(a => containsNTZ(a.dataType)) ||
        plan.children.exists(_.output.exists(a => containsNTZ(a.dataType)))
      if (hasNTZ) {
        fail(s"${plan.nodeName} has TimestampNTZType in input/output schema")
      } else {
        pass()
      }
    }
  }

  private class FallbackIfScanOnlyWithFilterPushed(scanOnly: Boolean) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (!scanOnly) {
        return pass()
      }
      // Scan-only mode
      plan match {
        case _: BatchScanExec => pass()
        case _: FileSourceScanExec => pass()
        case p if HiveTableScanExecTransformer.isHiveTableScan(p) => pass()
        case filter: FilterExec =>
          val childIsScan = filter.child.isInstanceOf[FileSourceScanExec] ||
            filter.child.isInstanceOf[BatchScanExec] || filter.child
              .isInstanceOf[BasicScanExecTransformer]
          if (childIsScan) {
            pass()
          } else {
            fail(filter)
          }
        case other => fail(other)
      }
    }
  }

  private class FallbackByNativeValidation(offloadRules: Seq[OffloadSingleNode])
    extends Validator
    with Logging {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      val offloadedNode = offloadAttempt.apply(plan)
      val out = offloadedNode match {
        case v: ValidatablePlan =>
          v.doValidate().toValidatorOutcome()
        case other =>
          // Currently we assume a plan to be offload-able by default.
          pass()
      }
      out
    }

    private val offloadAttempt: SparkPlan => SparkPlan = {
      node =>
        offloadRules.foldLeft(node) {
          case (node, rule) =>
            rule.offload(node)
        }
    }
  }

  /**
   * A standard validator for legacy planner that does native validation.
   *
   * The native validation is ordered in the latest validator, namely the one created by
   * #fallbackByNativeValidation. The validator accepts offload rules for doing offload attempts,
   * then call native validation code on the offloaded plan.
   *
   * Once the native validation fails, the validator then gives negative outcome.
   */
  def newValidator(conf: GlutenConfig, offloads: Seq[OffloadSingleNode]): Validator = {
    newValidator(conf, offloads, isStreaming = false)
  }

  def newValidator(
      conf: GlutenConfig,
      offloads: Seq[OffloadSingleNode],
      isStreaming: Boolean): Validator = {
    val nativeValidator = Validator.builder().fallbackByNativeValidation(offloads).build()
    newValidator(conf, isStreaming).andThen(nativeValidator)
  }

  /**
   * A validator that doesn't involve native validation.
   *
   * This could also be used in legacy planner for doing trivial offload without the help of rewrite
   * rules.
   */
  def newValidator(conf: GlutenConfig): Validator = {
    newValidator(conf, isStreaming = false)
  }

  def newValidator(conf: GlutenConfig, isStreaming: Boolean): Validator = {
    Validator
      .builder()
      .fallbackByHint()
      .fallbackIfScanOnlyWithFilterPushed(conf.enableScanOnly)
      .fallbackComplexExpressions()
      .fallbackByStreamingOptions(isStreaming, conf)
      .fallbackByBackendSettings()
      .fallbackByUserOptions()
      .fallbackByTimestampNTZ()
      .fallbackByTestInjects()
      .build()
  }
}
