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

import org.apache.gluten.connector.write.{ColumnarMicroBatchWriterFactory, ColumnarStreamingDataWriterFactory, IcebergColumnarBatchDataWriter}
import org.apache.gluten.execution.IcebergWriteJniWrapper
import org.apache.gluten.metrics.BatchWriteMetrics

import org.apache.spark.{SparkConf, SparkEnv, SparkException, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.iceberg.{PartitionSpec, SortOrder}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties

class IcebergColumnarDataWritingSparkTaskSuite extends AnyFunSuite {

  test("native Iceberg micro-batch task aborts and closes denied speculative attempt") {
    val stageId = 201
    val stageAttempt = 0
    val partitionId = 3
    val taskAttemptId = 900L
    val epochId = 42L
    val writerHandle = 10L
    val coordinator = new RecordingOutputCommitCoordinator
    val jniWrapper = new RecordingIcebergWriteJniWrapper(failAbort = true)
    val streamingFactory = new RecordingIcebergStreamingWriterFactory(
      jniWrapper,
      writerHandle = writerHandle)
    val microBatchFactory = new ColumnarMicroBatchWriterFactory(epochId, streamingFactory)

    withSparkEnv(coordinator) {
      val deniedError = intercept[SparkException] {
        DataWritingColumnarBatchSparkTask.run(
          microBatchFactory,
          taskContext(
            stageId,
            stageAttempt,
            partitionId,
            taskAttemptId = taskAttemptId,
            attemptNumber = 1),
          Iterator.empty,
          useCommitCoordinator = true,
          Map.empty
        )
      }

      assert(deniedError.getMessage.contains("Commit denied"))
      assert(streamingFactory.createdWriters == Seq((partitionId, taskAttemptId, epochId)))
      assert(coordinator.requests == Seq((stageId, stageAttempt, partitionId, 1)))
      assert(jniWrapper.commits.isEmpty)
      assert(jniWrapper.aborts == Seq(writerHandle))
      assert(jniWrapper.closes == Seq(writerHandle))
    }
  }

  test("native Iceberg micro-batch task commits first attempt and aborts duplicate attempt") {
    val stageId = 202
    val stageAttempt = 0
    val partitionId = 4
    val epochId = 43L
    val firstWriterHandle = 11L
    val duplicateWriterHandle = 12L
    val coordinator = new RecordingOutputCommitCoordinator
    val jniWrapper = new RecordingIcebergWriteJniWrapper()
    val streamingFactory = new RecordingIcebergStreamingWriterFactory(
      jniWrapper,
      writerHandles = Seq(firstWriterHandle, duplicateWriterHandle))
    val microBatchFactory = new ColumnarMicroBatchWriterFactory(epochId, streamingFactory)

    withSparkEnv(coordinator) {
      DataWritingColumnarBatchSparkTask.run(
        microBatchFactory,
        taskContext(
          stageId,
          stageAttempt,
          partitionId,
          taskAttemptId = 901L,
          attemptNumber = 0),
        Iterator.empty,
        useCommitCoordinator = true,
        Map.empty
      )

      assert(jniWrapper.commits == Seq(firstWriterHandle))
      assert(jniWrapper.aborts.isEmpty)
      assert(jniWrapper.closes == Seq(firstWriterHandle))
      assert(coordinator.requests == Seq((stageId, stageAttempt, partitionId, 0)))

      val deniedError = intercept[SparkException] {
        DataWritingColumnarBatchSparkTask.run(
          microBatchFactory,
          taskContext(
            stageId,
            stageAttempt,
            partitionId,
            taskAttemptId = 902L,
            attemptNumber = 1),
          Iterator.empty,
          useCommitCoordinator = true,
          Map.empty
        )
      }

      assert(deniedError.getMessage.contains("Commit denied"))
      assert(streamingFactory.createdWriters == Seq(
        (partitionId, 901L, epochId),
        (partitionId, 902L, epochId)))
      assert(coordinator.requests == Seq(
        (stageId, stageAttempt, partitionId, 0),
        (stageId, stageAttempt, partitionId, 1)))
      assert(jniWrapper.commits == Seq(firstWriterHandle))
      assert(jniWrapper.aborts == Seq(duplicateWriterHandle))
      assert(jniWrapper.closes == Seq(firstWriterHandle, duplicateWriterHandle))
    }
  }

  test("native Iceberg micro-batch task cleans native files when commit message conversion fails") {
    val stageId = 203
    val stageAttempt = 0
    val partitionId = 5
    val taskAttemptId = 903L
    val epochId = 44L
    val writerHandle = 13L
    val coordinator = new RecordingOutputCommitCoordinator
    val jniWrapper = new RecordingIcebergWriteJniWrapper(
      commitMessages = Array("not-json"))
    val streamingFactory = new RecordingIcebergStreamingWriterFactory(
      jniWrapper,
      writerHandle = writerHandle)
    val microBatchFactory = new ColumnarMicroBatchWriterFactory(epochId, streamingFactory)

    withSparkEnv(coordinator) {
      val commitError = intercept[Exception] {
        DataWritingColumnarBatchSparkTask.run(
          microBatchFactory,
          taskContext(
            stageId,
            stageAttempt,
            partitionId,
            taskAttemptId = taskAttemptId,
            attemptNumber = 0),
          Iterator.empty,
          useCommitCoordinator = true,
          Map.empty
        )
      }

      assert(commitError.getMessage.contains("Unrecognized token") || commitError
        .getMessage
        .contains("not-json"))
      assert(streamingFactory.createdWriters == Seq((partitionId, taskAttemptId, epochId)))
      assert(coordinator.requests == Seq((stageId, stageAttempt, partitionId, 0)))
      assert(jniWrapper.commits == Seq(writerHandle))
      assert(jniWrapper.cleanups == Seq(writerHandle))
      assert(jniWrapper.aborts.isEmpty)
      assert(jniWrapper.closes == Seq(writerHandle))
    }
  }

  test("native Iceberg micro-batch task skips abort after injected post-commit failure") {
    val stageId = 204
    val stageAttempt = 0
    val partitionId = 6
    val taskAttemptId = 904L
    val epochId = 45L
    val writerHandle = 14L
    val coordinator = new RecordingOutputCommitCoordinator
    val jniWrapper = new RecordingIcebergWriteJniWrapper()
    val streamingFactory = new RecordingIcebergStreamingWriterFactory(
      jniWrapper,
      writerHandle = writerHandle)
    val microBatchFactory = new ColumnarMicroBatchWriterFactory(
      epochId,
      streamingFactory,
      false,
      "throw",
      0,
      true,
      "throw",
      partitionId)

    withSparkEnv(coordinator) {
      val injectedError = intercept[SparkException] {
        DataWritingColumnarBatchSparkTask.run(
          microBatchFactory,
          taskContext(
            stageId,
            stageAttempt,
            partitionId,
            taskAttemptId = taskAttemptId,
            attemptNumber = 0),
          Iterator.empty,
          useCommitCoordinator = true,
          Map.empty
        )
      }

      assert(injectedError.getMessage.contains("after DataWriter.commit"))
      assert(streamingFactory.createdWriters == Seq((partitionId, taskAttemptId, epochId)))
      assert(coordinator.requests == Seq((stageId, stageAttempt, partitionId, 0)))
      assert(jniWrapper.commits == Seq(writerHandle))
      assert(jniWrapper.aborts.isEmpty)
      assert(jniWrapper.closes == Seq(writerHandle))
    }
  }

  private def taskContext(
      stageId: Int,
      stageAttempt: Int,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int): TaskContextImpl = {
    new TaskContextImpl(
      stageId,
      stageAttempt,
      partitionId,
      taskAttemptId,
      attemptNumber,
      numPartitions = 1,
      taskMemoryManager = null,
      localProperties = new Properties(),
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpus = 1,
      resources = Map.empty
    )
  }

  private def withSparkEnv[T](coordinator: OutputCommitCoordinator)(body: => T): T = {
    val previous = SparkEnv.get
    val env = new SparkEnv(
      "driver",
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      coordinator,
      new SparkConf())
    SparkEnv.set(env)
    try {
      body
    } finally {
      SparkEnv.set(previous)
    }
  }
}

private class RecordingIcebergStreamingWriterFactory(
    jniWrapper: RecordingIcebergWriteJniWrapper,
    writerHandles: Seq[Long])
  extends ColumnarStreamingDataWriterFactory {
  def this(jniWrapper: RecordingIcebergWriteJniWrapper, writerHandle: Long) = {
    this(jniWrapper, Seq(writerHandle))
  }

  require(writerHandles.nonEmpty, "Recording Iceberg writer factory requires at least one handle")

  var createdWriters: Seq[(Int, Long, Long)] = Seq.empty

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] = {
    val writerHandle = writerHandles.lift(createdWriters.size).getOrElse(writerHandles.last)
    createdWriters = createdWriters :+ ((partitionId, taskId, epochId))
    IcebergColumnarBatchDataWriter(
      writerHandle,
      jniWrapper,
      format = 1,
      PartitionSpec.unpartitioned(),
      SortOrder.unsorted())
  }
}

private class RecordingIcebergWriteJniWrapper(
    failAbort: Boolean = false,
    commitMessages: Array[String] = Array.empty)
  extends IcebergWriteJniWrapper(null) {
  var commits: Seq[Long] = Seq.empty
  var cleanups: Seq[Long] = Seq.empty
  var aborts: Seq[Long] = Seq.empty
  var closes: Seq[Long] = Seq.empty
  var metricsReads: Seq[Long] = Seq.empty

  override def write(writerHandle: Long, batch: Long): Unit = {}

  override def commit(writerHandle: Long): Array[String] = {
    commits = commits :+ writerHandle
    commitMessages
  }

  override def cleanupCommittedFiles(writerHandle: Long): Unit = {
    cleanups = cleanups :+ writerHandle
  }

  override def abort(writerHandle: Long): Unit = {
    aborts = aborts :+ writerHandle
    if (failAbort) {
      throw new RuntimeException(s"abort failed for $writerHandle")
    }
  }

  override def close(writerHandle: Long): Unit = {
    closes = closes :+ writerHandle
  }

  override def metrics(writerHandle: Long): BatchWriteMetrics = {
    metricsReads = metricsReads :+ writerHandle
    new BatchWriteMetrics(0L, 0, 0L, 0L)
  }
}

private class RecordingOutputCommitCoordinator
  extends OutputCommitCoordinator(new SparkConf(), true) {
  val requests = scala.collection.mutable.ArrayBuffer.empty[(Int, Int, Int, Int)]

  override def canCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = {
    requests += ((stage, stageAttempt, partition, attemptNumber))
    attemptNumber == 0
  }
}
