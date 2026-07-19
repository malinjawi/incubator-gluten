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
package org.apache.gluten.connector.write

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.scalatest.funsuite.AnyFunSuite

class ColumnarMicroBatchWriterFactorySuite extends AnyFunSuite {

  test("native streaming sink writer factory requires valid epoch contract") {
    val negativeEpochError = intercept[IllegalArgumentException] {
      new ColumnarMicroBatchWriterFactory(-1L, new RecordingStreamingWriterFactory)
    }
    assert(negativeEpochError.getMessage.contains("non-negative epoch id"))

    val nullFactoryError = intercept[NullPointerException] {
      new ColumnarMicroBatchWriterFactory(0L, null)
    }
    assert(nullFactoryError.getMessage.contains("streamingWriterFactory"))

    val negativePostCommitPartitionError = intercept[IllegalArgumentException] {
      new ColumnarMicroBatchWriterFactory(
        0L,
        new RecordingStreamingWriterFactory,
        false,
        "throw",
        0,
        false,
        "throw",
        -1)
    }
    assert(negativePostCommitPartitionError.getMessage.contains("post-commit"))

    val invalidPostWriteActionError = intercept[IllegalArgumentException] {
      new ColumnarMicroBatchWriterFactory(
        0L,
        new RecordingStreamingWriterFactory,
        true,
        "exit",
        0)
    }
    assert(invalidPostWriteActionError.getMessage.contains("failTaskAfterWriteAction"))

    val invalidPostCommitActionError = intercept[IllegalArgumentException] {
      new ColumnarMicroBatchWriterFactory(
        0L,
        new RecordingStreamingWriterFactory,
        false,
        "throw",
        0,
        true,
        "sleep",
        0)
    }
    assert(invalidPostCommitActionError.getMessage.contains("failTaskAfterCommitAction"))

    val normalizedFactory = new ColumnarMicroBatchWriterFactory(
      0L,
      new RecordingStreamingWriterFactory,
      true,
      "HALT",
      0,
      true,
      "THROW",
      0)
    assert(normalizedFactory.failTaskAfterWriteAction() == "halt")
    assert(normalizedFactory.failTaskAfterCommitAction() == "throw")
  }

  test("native streaming sink writer factory forwards Spark epoch to executor writer") {
    val streamingFactory = new RecordingStreamingWriterFactory
    val microBatchFactory = new ColumnarMicroBatchWriterFactory(42L, streamingFactory)

    microBatchFactory.createWriter(7, 11L)

    assert(streamingFactory.observed == Some((7, 11L, 42L)))
  }

  test("native streaming sink commit coordinator owns epoch commit transitions") {
    val batchWrite = new RecordingBatchWrite
    val coordinator = new ColumnarStreamingSinkCommitCoordinator(epochId = 9L, numPartitions = 2)

    val missingCommitError = intercept[IllegalStateException] {
      coordinator.commit(batchWrite)
    }
    assert(missingCommitError.getMessage.contains("missing writer commits"))

    assert(coordinator.recordWriterCommit(0, RecordingWriterCommitMessage("p0")))
    val duplicateCommitError = intercept[IllegalStateException] {
      coordinator.recordWriterCommit(0, RecordingWriterCommitMessage("p0-retry"))
    }
    assert(duplicateCommitError.getMessage.contains("duplicate commit"))
    assert(duplicateCommitError.getMessage.contains("orphanCandidatePartitions=[0]"))

    assert(coordinator.recordWriterCommit(1, RecordingWriterCommitMessage("p1")))
    coordinator.commit(batchWrite)

    assert(batchWrite.commits == Seq(Seq(
      RecordingWriterCommitMessage("p0"),
      RecordingWriterCommitMessage("p1"))))

    val postCommitError = intercept[IllegalStateException] {
      coordinator.recordWriterCommit(1, RecordingWriterCommitMessage("late"))
    }
    assert(postCommitError.getMessage.contains("after completion"))
  }

  test("native streaming sink commit coordinator aborts partial epochs once") {
    val batchWrite = new RecordingBatchWrite
    val coordinator = new ColumnarStreamingSinkCommitCoordinator(epochId = 10L, numPartitions = 2)

    assert(coordinator.recordWriterCommit(0, RecordingWriterCommitMessage("p0")))
    coordinator.abort(batchWrite)
    coordinator.abort(batchWrite)

    assert(batchWrite.aborts == Seq(Seq(RecordingWriterCommitMessage("p0"), null)))
  }

  test("native streaming sink commit coordinator rejects invalid writer commits") {
    val coordinator = new ColumnarStreamingSinkCommitCoordinator(epochId = 11L, numPartitions = 1)

    val negativePartitionError = intercept[IllegalArgumentException] {
      coordinator.recordWriterCommit(-1, RecordingWriterCommitMessage("bad"))
    }
    assert(negativePartitionError.getMessage.contains("invalid partition"))

    val overflowPartitionError = intercept[IllegalArgumentException] {
      coordinator.recordWriterCommit(1, RecordingWriterCommitMessage("bad"))
    }
    assert(overflowPartitionError.getMessage.contains("invalid partition"))

    val nullCommitError = intercept[NullPointerException] {
      coordinator.recordWriterCommit(0, null)
    }
    assert(nullCommitError.getMessage.contains("writerCommitMessage"))
  }

  test("native streaming sink commit coordinator does not abort completed epochs") {
    val batchWrite = new RecordingBatchWrite
    val coordinator = new ColumnarStreamingSinkCommitCoordinator(epochId = 12L, numPartitions = 1)

    assert(coordinator.recordWriterCommit(0, RecordingWriterCommitMessage("p0")))
    coordinator.commit(batchWrite)
    coordinator.abort(batchWrite)

    assert(batchWrite.commits == Seq(Seq(RecordingWriterCommitMessage("p0"))))
    assert(batchWrite.aborts.isEmpty)

    val secondCommitError = intercept[IllegalStateException] {
      coordinator.commit(batchWrite)
    }
    assert(secondCommitError.getMessage.contains("after completion"))
  }

  test("native streaming sink commit coordinator aborts full epoch after batch commit failure") {
    val batchWrite = new RecordingBatchWrite(failCommit = true)
    val coordinator = new ColumnarStreamingSinkCommitCoordinator(epochId = 13L, numPartitions = 2)

    assert(coordinator.recordWriterCommit(0, RecordingWriterCommitMessage("p0")))
    assert(coordinator.recordWriterCommit(1, RecordingWriterCommitMessage("p1")))

    val commitError = intercept[RuntimeException] {
      coordinator.commit(batchWrite)
    }
    assert(commitError.getMessage.contains("commit failed"))

    val secondCommitError = intercept[IllegalStateException] {
      coordinator.commit(batchWrite)
    }
    assert(secondCommitError.getMessage.contains("after commit failure"))

    val postFailureRecordError = intercept[IllegalStateException] {
      coordinator.recordWriterCommit(1, RecordingWriterCommitMessage("late"))
    }
    assert(postFailureRecordError.getMessage.contains("after commit failure"))

    coordinator.abort(batchWrite)
    coordinator.abort(batchWrite)

    assert(batchWrite.commits == Seq(Seq(
      RecordingWriterCommitMessage("p0"),
      RecordingWriterCommitMessage("p1"))))
    assert(batchWrite.aborts == Seq(Seq(
      RecordingWriterCommitMessage("p0"),
      RecordingWriterCommitMessage("p1"))))

    val postAbortError = intercept[IllegalStateException] {
      coordinator.recordWriterCommit(1, RecordingWriterCommitMessage("late"))
    }
    assert(postAbortError.getMessage.contains("after completion"))
  }

  test("native streaming sink commit coordinator keeps abort retryable after abort failure") {
    val batchWrite = new RecordingBatchWrite(failAbort = true)
    val coordinator = new ColumnarStreamingSinkCommitCoordinator(epochId = 14L, numPartitions = 2)

    assert(coordinator.recordWriterCommit(0, RecordingWriterCommitMessage("p0")))

    val abortError = intercept[RuntimeException] {
      coordinator.abort(batchWrite)
    }
    assert(abortError.getMessage.contains("abort failed"))
    assert(coordinator.auditSummary.contains("state=abortFailed"))
    assert(coordinator.auditSummary.contains("abortAttempts=1"))
    assert(coordinator.auditSummary.contains("abortFailures=1"))
    assert(coordinator.auditSummary.contains("orphanCandidatePartitions=[0]"))

    val postAbortFailureCommitError = intercept[IllegalStateException] {
      coordinator.commit(batchWrite)
    }
    assert(postAbortFailureCommitError.getMessage.contains("after abort failure"))
    assert(postAbortFailureCommitError.getMessage.contains("abort retry required"))

    val postAbortFailureRecordError = intercept[IllegalStateException] {
      coordinator.recordWriterCommit(1, RecordingWriterCommitMessage("late"))
    }
    assert(postAbortFailureRecordError.getMessage.contains("after abort failure"))
    assert(postAbortFailureRecordError.getMessage.contains("abort retry required"))

    batchWrite.failAbort = false
    coordinator.abort(batchWrite)
    coordinator.abort(batchWrite)

    assert(batchWrite.aborts == Seq(
      Seq(RecordingWriterCommitMessage("p0"), null),
      Seq(RecordingWriterCommitMessage("p0"), null)))

    val postAbortError = intercept[IllegalStateException] {
      coordinator.recordWriterCommit(1, RecordingWriterCommitMessage("late"))
    }
    assert(postAbortError.getMessage.contains("after completion"))
    assert(postAbortError.getMessage.contains("state=aborted"))
    assert(coordinator.auditSummary.contains("state=aborted"))
    assert(coordinator.auditSummary.contains("abortAttempts=2"))
    assert(coordinator.auditSummary.contains("abortFailures=1"))
    assert(coordinator.auditSummary.contains("orphanCandidatePartitions=[]"))
  }

  test("native streaming sink commit coordinator ignores idempotent duplicate writer commits") {
    val batchWrite = new RecordingBatchWrite
    val coordinator = new ColumnarStreamingSinkCommitCoordinator(epochId = 15L, numPartitions = 1)
    val commitMessage = RecordingWriterCommitMessage("p0")
    val retryCommitMessage = RecordingWriterCommitMessage("p0")

    assert(coordinator.recordWriterCommit(0, commitMessage))
    assert(!coordinator.recordWriterCommit(0, commitMessage))
    assert(!coordinator.recordWriterCommit(0, retryCommitMessage))
    assert(coordinator.snapshotMessages.toSeq == Seq(commitMessage))
    assert(coordinator.auditSummary.contains("idempotentDuplicateCommits=2"))
    assert(coordinator.auditSummary.contains("orphanCandidatePartitions=[0]"))

    coordinator.commit(batchWrite)

    assert(batchWrite.commits == Seq(Seq(commitMessage)))
    assert(coordinator.auditSummary.contains("state=committed"))
    assert(coordinator.auditSummary.contains("orphanCandidatePartitions=[]"))
  }

}

private class RecordingStreamingWriterFactory extends ColumnarStreamingDataWriterFactory {
  var observed: Option[(Int, Long, Long)] = None

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] = {
    observed = Some((partitionId, taskId, epochId))
    NoopColumnarBatchWriter
  }
}

private object NoopColumnarBatchWriter extends DataWriter[ColumnarBatch] {
  override def write(record: ColumnarBatch): Unit = {}

  override def commit(): WriterCommitMessage = NoopWriterCommitMessage

  override def abort(): Unit = {}

  override def close(): Unit = {}
}

private object NoopWriterCommitMessage extends WriterCommitMessage

private case class RecordingWriterCommitMessage(id: String) extends WriterCommitMessage

private class RecordingBatchWrite(
    failCommit: Boolean = false,
    var failAbort: Boolean = false)
  extends BatchWrite {
  var commits: Seq[Seq[WriterCommitMessage]] = Seq.empty
  var aborts: Seq[Seq[WriterCommitMessage]] = Seq.empty

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = null

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    commits = commits :+ messages.toSeq
    if (failCommit) {
      throw new RuntimeException("commit failed")
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    aborts = aborts :+ messages.toSeq
    if (failAbort) {
      throw new RuntimeException("abort failed")
    }
  }
}
