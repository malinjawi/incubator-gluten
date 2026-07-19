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
import org.apache.gluten.extension.columnar.validator.Validator.{Failed, Passed}
import org.apache.gluten.extension.columnar.validator.Validators._

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream, Offset}
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.execution.{LeafExecNode, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, MicroBatchScanExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

class StreamingValidatorSuite extends SparkFunSuite {

  test("streaming validation is disabled by default") {
    assertFailed(
      validate(projectPlan(), isStreaming = true),
      GlutenConfig.NATIVE_STREAMING_ENABLED.key)
  }

  test("streaming stateless operators require stateless gate") {
    assertFailed(
      validate(
        projectPlan(),
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true"),
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key)
  }

  test("streaming stateless gate allows project with executable bridge boundaries") {
    val confs = Seq(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true"
    )

    assert(validate(projectPlan(), isStreaming = true, confs: _*) == Passed)
    assertFailed(
      validate(FakeUnsupportedExec(), isStreaming = true, confs: _*),
      "is not in the native Spark Structured Streaming stateless operator allowlist")
  }

  test("full native stateless Kafka islands require native source ownership gates") {
    val project = projectPlan(kafkaMicroBatchScan())
    val fullNativeKafkaHandoffOnly = Seq(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_OFFSET_LOG_HANDOFF_ENABLED.key -> "true"
    )

    assertFailed(
      validate(project, isStreaming = true, fullNativeKafkaHandoffOnly: _*),
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_NATIVE_OFFSET_PLANNING_ENABLED.key)

    assert(
      validate(
        project,
        isStreaming = true,
        (fullNativeKafkaHandoffOnly :+
          (GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_NATIVE_OFFSET_PLANNING_ENABLED.key ->
            "true")): _*) == Passed)
  }

  test("batch validation ignores native streaming gates") {
    assert(validate(projectPlan(), isStreaming = false) == Passed)
  }

  test("batch DSv2 writes are not classified as streaming sinks") {
    val batchSink = AppendDataExec(
      FakeLeafExec(Seq(AttributeReference("value", IntegerType, nullable = true)())),
      () => (),
      FakeWrite)

    assert(!StreamingPlanSupport.isStreamingPlan(batchSink))
    assert(validate(batchSink, isStreaming = false) == Passed)
  }

  test("micro-batch subtrees use streaming validation even when caller is not streaming") {
    val project = projectPlan(kafkaMicroBatchScan())

    assertFailed(
      validate(
        project,
        isStreaming = false,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true"
      ),
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key
    )

    assert(
      validate(
        project,
        isStreaming = false,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true"
      ) == Passed)
  }

  test("streaming validation looks through rewritten node walls") {
    val project = projectPlan(RewrittenNodeWall(kafkaMicroBatchScan()))

    assertFailed(
      validate(
        project,
        isStreaming = false,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true"
      ),
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key
    )
  }

  test("streaming plan support recognizes Spark-owned source bridge") {
    assert(
      StreamingPlanSupport.isStreamingPlan(
        SparkOwnedMicroBatchScanExec(kafkaMicroBatchScan())))
  }

  test("Kafka micro-batch scan requires native Kafka execution gate") {
    val scan = kafkaMicroBatchScan()

    assertFailed(
      validate(
        scan,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true"),
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key
    )

    assert(
      validate(
        scan,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true"
      ) == Passed)
  }

  test("Spark-owned source bridge can cover Kafka when bridge execution is explicitly enabled") {
    val scan = kafkaMicroBatchScan()

    assert(
      validate(
        scan,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true"
      ) == Passed)
  }

  test("stateful streaming nodes require native state or Spark state bridge execution") {
    val statefulPlan = FakeStateStoreExec()

    assertFailed(
      validate(
        statefulPlan,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true"),
      "Spark streaming state remains Spark-owned")

    assertFailed(
      validate(
        statefulPlan,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "true"),
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key
    )

    assert(
      validate(
        statefulPlan,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true"
      ) == Passed)

    assert(
      validate(
        statefulPlan,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true") == Passed)
  }

  test("native stateless streaming islands require explicit state boundary over stateful child") {
    val projectOverState = projectPlan(FakeStateStoreExec())
    val statelessBridgeConfs = Seq(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true"
    )

    assertFailed(
      validate(projectOverState, isStreaming = true, statelessBridgeConfs: _*),
      "Spark streaming state remains Spark-owned")

    assert(
      validate(
        projectOverState,
        isStreaming = true,
        (statelessBridgeConfs :+
          (GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "true")): _*) == Passed)
  }

  test("stateful streaming fence covers Spark 4.1 state operator families") {
    Seq(
      FakeStateStoreExec(),
      FakeStreamingDeduplicateExec(),
      FakeStreamingSymmetricHashJoinExec(),
      FakeStreamingGlobalLimitExec(),
      FakeFlatMapGroupsWithStateExec(),
      FakeFlatMapGroupsInPandasWithStateExec(),
      FakeMapGroupsWithStateExec(),
      FakeTransformWithStateExec()
    ).foreach {
      statefulPlan =>
        assertFailed(
          validate(
            statefulPlan,
            isStreaming = true,
            GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true"),
          "Spark streaming state remains Spark-owned")
    }
  }

  test("stateful streaming taxonomy is shared across validator and plan visibility tests") {
    StreamingPlanSupport.StatefulStreamingOperatorClassFragments.foreach {
      fragment =>
        assert(
          StreamingPlanSupport.isStatefulStreamingClassName(s"org.apache.spark.$fragment" + "Exec"),
          s"Expected $fragment to be recognized as a stateful streaming operator family"
        )
    }
  }

  test("full native streaming rejects Spark-owned bridges") {
    val project = projectPlan(kafkaMicroBatchScan())
    val fullNativeWithBridgeConfs = Seq(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_STATE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true"
    )

    assertFailed(
      validate(project, isStreaming = true, fullNativeWithBridgeConfs: _*),
      "Full native Spark Structured Streaming forbids Spark-owned")
  }

  test("full native streaming requires native-owned source and sink boundaries") {
    val fullNativeKafkaWithoutHandoff = new SQLConf()
    fullNativeKafkaWithoutHandoff.setConfString(
      GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key,
      "true")
    fullNativeKafkaWithoutHandoff.setConfString(
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key,
      "true")
    assert(
      StreamingPlanSupport.firstUnsupportedNativeStreamingBoundary(
        kafkaMicroBatchScan(),
        new GlutenConfig(fullNativeKafkaWithoutHandoff)
      ).contains(StreamingPlanSupport.FullNativeKafkaOffsetLogRequirement))

    assertFailed(
      validate(
        kafkaMicroBatchScan(),
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true"
      ),
      StreamingPlanSupport.FullNativeKafkaOffsetLogRequirement
    )

    assertFailed(
      validate(
        kafkaMicroBatchScan(),
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_OFFSET_LOG_HANDOFF_ENABLED.key -> "true"
      ),
      StreamingPlanSupport.FullNativeKafkaNativeOffsetPlanningRequirement
    )

    assert(
      validate(
        kafkaMicroBatchScan(),
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_OFFSET_LOG_HANDOFF_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_NATIVE_OFFSET_PLANNING_ENABLED.key -> "true"
      ) == Passed)

    assert(
      validate(
        streamingSinkPlan(),
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "true"
      ) == Passed)

    assertFailed(
      validate(
        FakeStateStoreExec(),
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true"
      ),
      StreamingPlanSupport.FullNativeStatefulOperatorRequirement
    )
  }

  test("full native streaming does not treat native StateStore provider as native stateful exec") {
    val projectOverState = projectPlan(FakeStateStoreExec())

    assertFailed(
      validate(
        projectOverState,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_SINK_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true"
      ),
      StreamingPlanSupport.FullNativeStatefulOperatorRequirement
    )
  }

  test("native stateful deduplicate gate does not treat Spark dedupe as full native") {
    val sqlConf = new SQLConf()
    sqlConf.setConfString(GlutenConfig.NATIVE_STREAMING_STATEFUL_DEDUPLICATE_ENABLED.key, "true")
    assert(new GlutenConfig(sqlConf).enableNativeStreamingStatefulDeduplicate)

    assertFailed(
      validate(
        FakeStreamingDeduplicateExec(),
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATEFUL_DEDUPLICATE_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true"
      ),
      StreamingPlanSupport.FullNativeStatefulOperatorRequirement
    )
  }

  test("native stateful marker separates native candidates from Spark-owned stateful operators") {
    val nativeDedupe = FakeNativeStreamingDeduplicateExec()
    val sparkDedupe = FakeStreamingDeduplicateExec()

    assert(StreamingPlanSupport.isStatefulStreamingPlan(nativeDedupe))
    assert(StreamingPlanSupport.isNativeStreamingStatefulPlan(nativeDedupe))
    assert(!StreamingPlanSupport.isSparkOwnedStatefulStreamingPlan(nativeDedupe))
    assert(StreamingPlanSupport.containsStatefulStreamingPlan(projectPlan(nativeDedupe)))
    assert(!StreamingPlanSupport.containsSparkOwnedStatefulStreamingPlan(projectPlan(nativeDedupe)))

    assert(StreamingPlanSupport.isStatefulStreamingPlan(sparkDedupe))
    assert(!StreamingPlanSupport.isNativeStreamingStatefulPlan(sparkDedupe))
    assert(StreamingPlanSupport.isSparkOwnedStatefulStreamingPlan(sparkDedupe))
  }

  test("native stateful deduplicate candidate requires dedicated operator gate") {
    val nativeDedupe = FakeNativeStreamingDeduplicateExec()
    val nativeDedupeWithinWatermark = FakeNativeStreamingDeduplicateExec(
      NativeStreamingStatefulOperatorExec.DeduplicateWithinWatermark)

    Seq(nativeDedupe, nativeDedupeWithinWatermark).foreach {
      nativePlan =>
        assertFailed(
          validate(
            nativePlan,
            isStreaming = true,
            GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true"
          ),
          StreamingPlanSupport.nativeStatefulOperatorGateRequirement(
            nativePlan.nativeStatefulOperatorKind)
        )
    }

    Seq(nativeDedupe, nativeDedupeWithinWatermark).foreach {
      nativePlan =>
        assert(
          validate(
            nativePlan,
            isStreaming = true,
            GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
            GlutenConfig.NATIVE_STREAMING_STATEFUL_DEDUPLICATE_ENABLED.key -> "true"
          ) == Passed)
    }
  }

  test("native stateful count aggregation candidate requires dedicated operator gate") {
    val nativeCountAggregation = FakeNativeStreamingCountAggregationExec()

    assertFailed(
      validate(
        nativeCountAggregation,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true"
      ),
      StreamingPlanSupport.nativeStatefulOperatorGateRequirement(
        NativeStreamingStatefulOperatorExec.CountAggregation)
    )

    assert(
      validate(
        nativeCountAggregation,
        isStreaming = true,
        GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
        GlutenConfig.NATIVE_STREAMING_STATEFUL_AGGREGATION_ENABLED.key -> "true"
      ) == Passed)
  }

  test("native stateless island over native stateful child does not require Spark state bridge") {
    val projectOverNativeDedupe = projectPlan(FakeNativeStreamingDeduplicateExec())
    val projectOverSparkDedupe = projectPlan(FakeStreamingDeduplicateExec())
    val statelessBridgeConfs = Seq(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATELESS_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_OUTPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATEFUL_DEDUPLICATE_ENABLED.key -> "true"
    )

    assert(
      validate(projectOverNativeDedupe, isStreaming = true, statelessBridgeConfs: _*) == Passed)
    assertFailed(
      validate(projectOverSparkDedupe, isStreaming = true, statelessBridgeConfs: _*),
      "Spark streaming state remains Spark-owned")
  }

  private def validate(
      plan: SparkPlan,
      isStreaming: Boolean,
      confs: (String, String)*): Validator.OutCome = {
    val sqlConf = new SQLConf()
    confs.foreach { case (key, value) => sqlConf.setConfString(key, value) }

    Validator
      .builder()
      .fallbackByStreamingOptions(isStreaming, new GlutenConfig(sqlConf))
      .build()
      .validate(plan)
  }

  private def projectPlan(): ProjectExec = {
    val attr = AttributeReference("value", IntegerType, nullable = true)()
    ProjectExec(Seq(attr), FakeLeafExec(Seq(attr)))
  }

  private def projectPlan(child: SparkPlan): ProjectExec = {
    ProjectExec(child.output, child)
  }

  private def assertFailed(outcome: Validator.OutCome, reasonContains: String): Unit = {
    outcome match {
      case Failed(reason) => assert(reason.contains(reasonContains), reason)
      case Passed => fail(s"Expected validation failure containing: $reasonContains")
    }
  }

  private def kafkaMicroBatchScan(): MicroBatchScanExec = {
    val output = Seq(AttributeReference("value", IntegerType, nullable = true)())
    MicroBatchScanExec(output, FakeKafkaScan, FakeKafkaStream, FakeOffset("0"), FakeOffset("1"))
  }

  private def streamingSinkPlan(): AppendDataExec = {
    AppendDataExec(kafkaMicroBatchScan(), () => (), FakeWrite)
  }

  private case class FakeOffset(jsonValue: String) extends Offset {
    override def json(): String = jsonValue
  }

  private object FakeKafkaScan extends Scan {
    override def readSchema(): StructType = new StructType().add("value", IntegerType)

    override def description(): String = "KafkaScan"

    override def toBatch(): Batch =
      throw new UnsupportedOperationException("Fake streaming scan does not support batch reads")

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = FakeKafkaStream

    override def toContinuousStream(checkpointLocation: String): ContinuousStream =
      throw new UnsupportedOperationException(
        "Fake streaming scan does not support continuous reads")

    override def supportedCustomMetrics(): Array[CustomMetric] = Array.empty

    override def reportDriverMetrics(): Array[CustomTaskMetric] = Array.empty

    override def columnarSupportMode(): Scan.ColumnarSupportMode =
      Scan.ColumnarSupportMode.UNSUPPORTED
  }

  private object FakeKafkaStream extends MicroBatchStream {
    override def latestOffset(): Offset = FakeOffset("latest")

    override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] =
      Array.empty

    override def createReaderFactory(): PartitionReaderFactory = FakeReaderFactory

    override def initialOffset(): Offset = FakeOffset("initial")

    override def deserializeOffset(json: String): Offset = FakeOffset(json)

    override def commit(end: Offset): Unit = {}

    override def stop(): Unit = {}
  }

  private object FakeReaderFactory extends PartitionReaderFactory {
    override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
      throw new UnsupportedOperationException("Fake scan has no partitions")
  }

  private object FakeWrite extends Write {
    override def description(): String = "FakeWrite"

    override def toBatch(): BatchWrite =
      throw new UnsupportedOperationException("Fake write is never executed")

    override def toStreaming(): StreamingWrite =
      throw new UnsupportedOperationException("Fake write is never executed")

    override def supportedCustomMetrics(): Array[CustomMetric] = Array.empty

    override def reportDriverMetrics(): Array[CustomTaskMetric] = Array.empty
  }

  private case class FakeStateStoreExec() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake state plan is never executed")
  }

  private case class FakeStreamingDeduplicateExec() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake dedupe plan is never executed")
  }

  private case class FakeNativeStreamingDeduplicateExec(
      override val nativeStatefulOperatorKind: String =
        NativeStreamingStatefulOperatorExec.Deduplicate)
    extends LeafExecNode
    with NativeStreamingStatefulOperatorExec {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake native dedupe plan is never executed")
  }

  private case class FakeNativeStreamingCountAggregationExec()
    extends LeafExecNode
    with NativeStreamingStatefulOperatorExec {
    override val nativeStatefulOperatorKind: String =
      NativeStreamingStatefulOperatorExec.CountAggregation

    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake native count plan is never executed")
  }

  private case class FakeStreamingSymmetricHashJoinExec() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake join plan is never executed")
  }

  private case class FakeStreamingGlobalLimitExec() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake limit plan is never executed")
  }

  private case class FakeFlatMapGroupsWithStateExec() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake flatMapGroupsWithState plan is never executed")
  }

  private case class FakeFlatMapGroupsInPandasWithStateExec() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake pandas state plan is never executed")
  }

  private case class FakeMapGroupsWithStateExec() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake mapGroupsWithState plan is never executed")
  }

  private case class FakeTransformWithStateExec() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake transformWithState plan is never executed")
  }

  private case class FakeUnsupportedExec() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake unsupported plan is never executed")
  }

  private case class FakeLeafExec(override val output: Seq[Attribute]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake leaf plan is never executed")
  }
}
