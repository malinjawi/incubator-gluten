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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarMicroBatchWriterFactory, ColumnarStreamingDataWriterFactory, ColumnarStreamingSinkCommitCoordinator}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.extension.columnar.transition.Convention.RowType

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, Write, WriterCommitMessage}
import org.apache.spark.sql.datasources.v2.{DataWritingColumnarBatchSparkTask, DataWritingColumnarBatchSparkTaskResult, StreamWriterCommitProgressUtil, WritingColumnarBatchSparkTask}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.LongAccumulator

trait ColumnarV2TableWriteExec extends V2TableWriteExec with ValidatablePlan {

  def refreshCache: () => Unit

  def write: Write

  def batchWrite: BatchWrite = write.toBatch

  def withNewQuery(newQuery: SparkPlan): SparkPlan = withNewChildInternal(newQuery)

  protected def createBatchWriterFactory(schema: StructType): ColumnarBatchDataWriterFactory

  protected def createStreamingWriterFactory(schema: StructType): ColumnarStreamingDataWriterFactory

  override protected def run(): Seq[InternalRow] = {
    writeColumnarBatchWithV2(batchWrite)
    refreshCache()
    Nil
  }

  override def batchType(): Convention.BatchType = Convention.BatchType.None

  override def rowType(): Convention.RowType = RowType.VanillaRowType

  override def requiredChildConvention(): Seq[ConventionReq] = Seq(
    ConventionReq.ofBatch(
      ConventionReq.BatchType.Is(BackendsApiManager.getSettings.primaryBatchType)))

  private def writingTaskBatch: WritingColumnarBatchSparkTask[_] = DataWritingColumnarBatchSparkTask

  protected def shouldSkipNativeStreamingEpoch(batchWrite: BatchWrite): Boolean = false

  private def writeColumnarBatchWithV2(batchWrite: BatchWrite): Unit = {
    if (shouldSkipNativeStreamingEpoch(batchWrite)) {
      logInfo(s"Skipping data source write support $batchWrite for an already committed epoch.")
      commitProgress = Some(StreamWriterCommitProgressUtil.getStreamWriterCommitProgress(0L))
      return
    }

    val rdd: RDD[ColumnarBatch] = {
      val tempRdd = query.executeColumnar()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else {
        tempRdd
      }
    }
    // introduce a local var to avoid serializing the whole class
    val task = writingTaskBatch
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(
      s"Start processing data source write support: $batchWrite. " +
        s"The input RDD has ${rdd.partitions.length} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = customMetrics
    val (factory, streamingCommitCoordinator) = batchWrite match {
      case m: MicroBatchWrite =>
        val epochIdField = m.getClass.getDeclaredField("epochId")
        epochIdField.setAccessible(true)
        val epochId = epochIdField.getLong(m)
        val glutenConfig = new GlutenConfig(query.conf)
        (
          new ColumnarMicroBatchWriterFactory(
            epochId,
            createStreamingWriterFactory(query.schema),
            glutenConfig.enableNativeStreamingSinkTestFailTaskAfterWrite,
            glutenConfig.nativeStreamingSinkTestFailTaskAfterWriteAction,
            glutenConfig.nativeStreamingSinkTestFailTaskAfterWritePartitionId,
            glutenConfig.enableNativeStreamingSinkTestFailTaskAfterCommit,
            glutenConfig.nativeStreamingSinkTestFailTaskAfterCommitAction,
            glutenConfig.nativeStreamingSinkTestFailTaskAfterCommitPartitionId
          ),
          Some(new ColumnarStreamingSinkCommitCoordinator(epochId, rdd.partitions.length)))
      case _ =>
        (createBatchWriterFactory(query.schema), None)
    }
    val messages = streamingCommitCoordinator
      .map(_.snapshotMessages)
      .getOrElse(new Array[WriterCommitMessage](rdd.partitions.length))
    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[ColumnarBatch]) =>
          task.run(factory, context, iter, useCommitCoordinator, writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingColumnarBatchSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          val recorded = streamingCommitCoordinator
            .map(_.recordWriterCommit(index, commitMessage))
            .getOrElse(true)
          if (recorded) {
            messages(index) = commitMessage
            totalNumRowsAccumulator.add(result.numRows)
            batchWrite.onDataWriterCommit(commitMessage)
          } else {
            logWarning(
              s"Ignored idempotent duplicate writer commit for streaming sink partition $index")
          }
        }
      )

      logInfo(s"Data source write support $batchWrite is committing.")
      streamingCommitCoordinator match {
        case Some(coordinator) =>
          injectPreStreamingSinkCommitFailureIfEnabled()
          coordinator.commit(batchWrite)
          injectPostStreamingSinkCommitFailureIfEnabled()
        case None => batchWrite.commit(messages)
      }
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(
        StreamWriterCommitProgressUtil.getStreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          streamingCommitCoordinator match {
            case Some(coordinator) => coordinator.abort(batchWrite)
            case None => batchWrite.abort(messages)
          }
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("_LEGACY_ERROR_TEMP_2070", cause = cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        throw cause
    }
  }

  private def injectPreStreamingSinkCommitFailureIfEnabled(): Unit = {
    val glutenConfig = new GlutenConfig(query.conf)
    if (glutenConfig.enableNativeStreamingSinkTestFailBeforeCommit) {
      failNativeStreamingSinkForRestartTest(
        "Injected native streaming sink failure before BatchWrite.commit for restart testing",
        glutenConfig.nativeStreamingSinkTestFailBeforeCommitAction)
    }
  }

  private def injectPostStreamingSinkCommitFailureIfEnabled(): Unit = {
    val glutenConfig = new GlutenConfig(query.conf)
    if (glutenConfig.enableNativeStreamingSinkTestFailAfterCommit) {
      failNativeStreamingSinkForRestartTest(
        "Injected native streaming sink failure after BatchWrite.commit for restart testing",
        glutenConfig.nativeStreamingSinkTestFailAfterCommitAction)
    }
  }

  private def failNativeStreamingSinkForRestartTest(message: String, action: String): Unit = {
    action match {
      case "halt" =>
        logError(message)
        Runtime.getRuntime.halt(86)
      case _ =>
        throw new SparkException(message)
    }
  }

  override val customMetrics: Map[String, SQLMetric] = {
    write
      .supportedCustomMetrics()
      .map {
        customMetric =>
          customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
      }
      .toMap ++ BackendsApiManager.getMetricsApiInstance.genBatchWriteMetrics(sparkContext)
  }
}
