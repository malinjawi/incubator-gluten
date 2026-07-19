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
package org.apache.spark.sql.datasources.v2

import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarMicroBatchWriterFactory}

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.v2.StreamWriterCommitProgress
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

case class DataWritingColumnarBatchSparkTaskResult(
    numRows: Long,
    writerCommitMessage: WriterCommitMessage)

trait WritingColumnarBatchSparkTask[W <: DataWriter[ColumnarBatch]]
  extends Logging
  with Serializable {

  protected def write(writer: W, row: ColumnarBatch): Unit

  def run(
      factory: ColumnarBatchDataWriterFactory,
      context: TaskContext,
      iter: Iterator[ColumnarBatch],
      useCommitCoordinator: Boolean,
      customMetrics: Map[String, SQLMetric]): DataWritingColumnarBatchSparkTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = factory.createWriter(partId, taskId).asInstanceOf[W]

    var count = 0L
    var committed = false
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
        val batch = iter.next()
        // Count is here.
        count += batch.numRows().toLong
        write(dataWriter, batch)
        injectNativeStreamingSinkTaskFailureAfterWriteIfEnabled(factory, context)
      }

      CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(
            s"Commit authorized for partition $partId (task $taskId, attempt $attemptId, " +
              s"stage $stageId.$stageAttempt)")
          val commitMessage = dataWriter.commit()
          committed = true
          commitMessage
        } else {
          val commitDeniedException = QueryExecutionErrors.commitDeniedError(
            partId,
            taskId,
            attemptId,
            stageId,
            stageAttempt)
          logInfo(commitDeniedException.getMessage)
          throw commitDeniedException
        }
      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        val commitMessage = dataWriter.commit()
        committed = true
        commitMessage
      }
      injectNativeStreamingSinkTaskFailureAfterCommitIfEnabled(factory, context)
      // Native write's metrics should be updated again after commit.
      CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)

      logInfo(
        s"Committed partition $partId (task $taskId, attempt $attemptId, " +
          s"stage $stageId.$stageAttempt)")

      DataWritingColumnarBatchSparkTaskResult(count, msg)

    })(
      catchBlock = {
        if (!committed) {
          // If there is an error before commit has completed, abort this writer.
          logError(
            s"Aborting commit for partition $partId (task $taskId, attempt $attemptId, " +
              s"stage $stageId.$stageAttempt)")
          dataWriter.abort()
          logError(
            s"Aborted commit for partition $partId (task $taskId, attempt $attemptId, " +
              s"stage $stageId.$stageAttempt)")
        } else {
          logError(
            s"Skipping abort for already committed partition $partId (task $taskId, " +
              s"attempt $attemptId, stage $stageId.$stageAttempt)")
        }
      },
      finallyBlock = {
        dataWriter.close()
      }
    )
  }

  private def injectNativeStreamingSinkTaskFailureAfterWriteIfEnabled(
      factory: ColumnarBatchDataWriterFactory,
      context: TaskContext): Unit = {
    factory match {
      case microBatchFactory: ColumnarMicroBatchWriterFactory
          if microBatchFactory.failTaskAfterWriteEnabled() &&
            context.partitionId() == microBatchFactory.failTaskAfterWritePartitionId() &&
            context.attemptNumber() == 0 =>
        val message =
          s"Injected native streaming sink task failure after DataWriter.write for " +
            s"partition ${context.partitionId()} (task ${context.taskAttemptId()}, " +
            s"attempt ${context.attemptNumber()}, stage ${context.stageId()}." +
            s"${context.stageAttemptNumber()})"
        microBatchFactory.failTaskAfterWriteAction() match {
          case "halt" =>
            logError(message)
            Runtime.getRuntime.halt(86)
          case _ =>
            throw new SparkException(message)
        }
      case _ =>
    }
  }

  private def injectNativeStreamingSinkTaskFailureAfterCommitIfEnabled(
      factory: ColumnarBatchDataWriterFactory,
      context: TaskContext): Unit = {
    factory match {
      case microBatchFactory: ColumnarMicroBatchWriterFactory
          if microBatchFactory.failTaskAfterCommitEnabled() &&
            context.partitionId() == microBatchFactory.failTaskAfterCommitPartitionId() &&
            context.attemptNumber() == 0 =>
        val message =
          s"Injected native streaming sink task failure after DataWriter.commit for " +
            s"partition ${context.partitionId()} (task ${context.taskAttemptId()}, " +
            s"attempt ${context.attemptNumber()}, stage ${context.stageId()}." +
            s"${context.stageAttemptNumber()})"
        microBatchFactory.failTaskAfterCommitAction() match {
          case "halt" =>
            logError(message)
            Runtime.getRuntime.halt(87)
          case _ =>
            throw new SparkException(message)
        }
      case _ =>
    }
  }
}

object DataWritingColumnarBatchSparkTask
  extends WritingColumnarBatchSparkTask[DataWriter[ColumnarBatch]] {

  override protected def write(writer: DataWriter[ColumnarBatch], batch: ColumnarBatch): Unit = {
    writer.write(batch)
  }
}

object StreamWriterCommitProgressUtil {
  def getStreamWriterCommitProgress(numOutputRows: Long): StreamWriterCommitProgress = {
    StreamWriterCommitProgress(numOutputRows)
  }
}
