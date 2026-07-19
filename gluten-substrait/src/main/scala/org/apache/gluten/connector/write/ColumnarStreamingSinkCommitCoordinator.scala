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

import org.apache.spark.sql.connector.write.{BatchWrite, WriterCommitMessage}

import java.util.Objects

class ColumnarStreamingSinkCommitCoordinator(
    epochId: Long,
    numPartitions: Int)
  extends Serializable {
  require(epochId >= 0, s"Native streaming sink requires a non-negative epoch id: $epochId")
  require(
    numPartitions >= 0,
    s"Native streaming sink requires a non-negative partition count: $numPartitions")

  private val messages = new Array[WriterCommitMessage](numPartitions)
  private var completed = false
  private var commitFailed = false
  private var abortFailed = false
  private var completionState = "open"
  private var idempotentDuplicateCommits = 0
  private var abortAttempts = 0
  private var abortFailures = 0

  def recordWriterCommit(
      partitionIndex: Int,
      message: WriterCommitMessage): Boolean = synchronized {
    ensureOpen("record writer commit")
    require(
      partitionIndex >= 0 && partitionIndex < numPartitions,
      s"Native streaming sink epoch $epochId received commit for invalid partition " +
        s"$partitionIndex; " +
        s"partition count is $numPartitions"
    )
    Objects.requireNonNull(message, "writerCommitMessage")
    val existing = messages(partitionIndex)
    if (existing != null) {
      if (existing == message) {
        idempotentDuplicateCommits += 1
        false
      } else {
        throw new IllegalStateException(
          s"Native streaming sink epoch $epochId received duplicate commit for partition " +
            s"$partitionIndex; $auditSummaryLocked")
      }
    } else {
      messages(partitionIndex) = message
      true
    }
  }

  def commit(batchWrite: BatchWrite): Unit = synchronized {
    ensureOpen("commit")
    val missing = messages.indices.filter(messages(_) == null)
    if (missing.nonEmpty) {
      throw new IllegalStateException(
        s"Native streaming sink epoch $epochId cannot commit; missing writer commits for " +
          s"partitions ${missing.mkString(",")}; $auditSummaryLocked")
    }
    try {
      batchWrite.commit(messages.clone())
      completed = true
      completionState = "committed"
    } catch {
      case t: Throwable =>
        commitFailed = true
        completionState = "commitFailed"
        throw t
    }
  }

  def abort(batchWrite: BatchWrite): Unit = synchronized {
    if (!completed) {
      abortAttempts += 1
      try {
        batchWrite.abort(messages.clone())
        completed = true
        abortFailed = false
        completionState = if (commitFailed) "abortedAfterCommitFailure" else "aborted"
      } catch {
        case t: Throwable =>
          abortFailed = true
          abortFailures += 1
          completionState =
            if (commitFailed) "abortAfterCommitFailureFailed" else "abortFailed"
          throw t
      }
    }
  }

  def snapshotMessages: Array[WriterCommitMessage] = synchronized {
    messages.clone()
  }

  def auditSummary: String = synchronized {
    auditSummaryLocked
  }

  private def ensureOpen(action: String): Unit = {
    if (completed) {
      throw new IllegalStateException(
        s"Native streaming sink epoch $epochId cannot $action after completion; " +
          auditSummaryLocked)
    }
    if (abortFailed) {
      throw new IllegalStateException(
        s"Native streaming sink epoch $epochId cannot $action after abort failure; " +
          s"abort retry required; $auditSummaryLocked")
    }
    if (commitFailed) {
      throw new IllegalStateException(
        s"Native streaming sink epoch $epochId cannot $action after commit failure; " +
          s"abort required; $auditSummaryLocked")
    }
  }

  private def auditSummaryLocked: String = {
    val committedPartitions = messages.indices.filter(messages(_) != null)
    val missingPartitions = messages.indices.filter(messages(_) == null)
    val orphanCandidatePartitions =
      if (completed) {
        Seq.empty[Int]
      } else {
        committedPartitions
      }
    s"audit(epoch=$epochId, state=$completionState, partitions=$numPartitions, " +
      s"committedPartitions=${formatPartitionIndexes(committedPartitions)}, " +
      s"missingPartitions=${formatPartitionIndexes(missingPartitions)}, " +
      s"idempotentDuplicateCommits=$idempotentDuplicateCommits, " +
      s"abortAttempts=$abortAttempts, abortFailures=$abortFailures, " +
      s"orphanCandidatePartitions=${formatPartitionIndexes(orphanCandidatePartitions)})"
  }

  private def formatPartitionIndexes(indexes: Seq[Int]): String = {
    if (indexes.isEmpty) {
      "[]"
    } else if (indexes.size <= 16) {
      indexes.mkString("[", ",", "]")
    } else {
      indexes.take(16).mkString("[", ",", s",... +${indexes.size - 16}]")
    }
  }
}
