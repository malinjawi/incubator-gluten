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
import org.apache.gluten.extension.columnar.validator.StreamingPlanSupport

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream, Offset}
import org.apache.spark.sql.execution.{LeafExecNode, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

class StreamingQueryFallbackPolicySuite extends SparkFunSuite {

  test("non-streaming plans are not wrapped by streaming fallback policy") {
    val plan = projectPlan()
    val result = applyPolicy(plan, isStreaming = false)

    assert(result eq plan)
  }

  test("micro-batch plans report boundaries when caller metadata is not streaming") {
    val plan = kafkaMicroBatchScan()
    val boundary = unsupportedBoundary(
      plan,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true")

    assert(
      boundary.exists(
        _.contains(GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key)))
  }

  test("micro-batch plans are not wrapped when bridge execution is enabled") {
    val plan = kafkaMicroBatchScan()
    val result = applyPolicy(
      plan,
      isStreaming = false,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true"
    )

    assert(result eq plan)
  }

  test("streaming plans without unsupported native streaming boundaries are not wrapped") {
    val plan = projectPlan()
    val result = applyPolicy(
      plan,
      isStreaming = true,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true")

    assert(result eq plan)
  }

  test("Kafka prototype source reports native Kafka execution fence boundary") {
    val scan = kafkaMicroBatchScan()
    val boundary = unsupportedBoundary(
      scan,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true")

    assert(
      boundary.exists(
        _.contains(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key)))
  }

  test("Kafka prototype source has no boundary when the execution fence is enabled") {
    val scan = kafkaMicroBatchScan()
    val boundary = unsupportedBoundary(
      scan,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true"
    )

    assert(boundary.isEmpty)
  }

  test("Kafka prototype source is not wrapped when the execution fence is enabled") {
    val scan = kafkaMicroBatchScan()
    val result = applyPolicy(
      scan,
      isStreaming = true,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true"
    )

    assert(result eq scan)
  }

  test("Spark-owned source bridge removes Kafka boundary when bridge execution is enabled") {
    val scan = kafkaMicroBatchScan()
    val boundary = unsupportedBoundary(
      scan,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true"
    )

    assert(boundary.isEmpty)
  }

  test("Spark-owned source bridge reports execution fence when bridge is only planned") {
    val scan = kafkaMicroBatchScan()
    val boundary = unsupportedBoundary(
      scan,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "false",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true"
    )

    assert(
      boundary.exists(
        _.contains(GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key)))
  }

  test("full native streaming reports Spark-owned bridge boundary") {
    val scan = kafkaMicroBatchScan()
    val boundary = unsupportedBoundary(
      scan,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_INPUT_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_SPARK_BRIDGE_EXECUTION_ENABLED.key -> "true"
    )

    assert(boundary.exists(_.contains("Full native Spark Structured Streaming forbids")))

  }

  test("full native streaming allows native-owned Kafka source boundary") {
    val scan = kafkaMicroBatchScan()
    val handoffOnlyBoundary = unsupportedBoundary(
      scan,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_OFFSET_LOG_HANDOFF_ENABLED.key -> "true"
    )
    assert(
      handoffOnlyBoundary.contains(
        StreamingPlanSupport.FullNativeKafkaNativeOffsetPlanningRequirement))

    val boundary = unsupportedBoundary(
      scan,
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_OFFSET_LOG_HANDOFF_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_NATIVE_OFFSET_PLANNING_ENABLED.key -> "true"
    )

    assert(boundary.isEmpty)
  }

  test("full native streaming reports provider-only native state boundary") {
    val boundary = unsupportedBoundary(
      FakeStateStoreExec(),
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_FULL_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true"
    )

    assert(boundary.contains(StreamingPlanSupport.FullNativeStatefulOperatorRequirement))
  }

  private def applyPolicy(
      plan: SparkPlan,
      isStreaming: Boolean,
      confs: (String, String)*): SparkPlan = {
    StreamingQueryFallbackPolicy(isStreaming, glutenConf(confs: _*), plan).apply(plan)
  }

  private def unsupportedBoundary(
      plan: SparkPlan,
      confs: (String, String)*): Option[String] = {
    StreamingPlanSupport.firstUnsupportedNativeStreamingBoundary(plan, glutenConf(confs: _*))
  }

  private def glutenConf(confs: (String, String)*): GlutenConfig = {
    val sqlConf = new SQLConf()
    confs.foreach { case (key, value) => sqlConf.setConfString(key, value) }
    new GlutenConfig(sqlConf)
  }

  private def projectPlan(): ProjectExec = {
    val attr = AttributeReference("value", IntegerType, nullable = true)()
    ProjectExec(Seq(attr), FakeLeafExec(Seq(attr)))
  }

  private def kafkaMicroBatchScan(): MicroBatchScanExec = {
    val output = Seq(AttributeReference("value", IntegerType, nullable = true)())
    MicroBatchScanExec(output, FakeKafkaScan, FakeKafkaStream, FakeOffset("0"), FakeOffset("1"))
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

  private case class FakeLeafExec(override val output: Seq[Attribute]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake leaf plan is never executed")
  }

  private case class FakeStateStoreExec() extends LeafExecNode {
    override def output: Seq[Attribute] =
      Seq(AttributeReference("value", IntegerType, nullable = true)())

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Fake stateful plan is never executed")
  }
}
