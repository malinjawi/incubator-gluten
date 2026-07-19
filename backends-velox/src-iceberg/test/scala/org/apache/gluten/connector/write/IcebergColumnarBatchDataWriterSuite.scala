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

import org.apache.gluten.execution.IcebergWriteJniWrapper
import org.apache.gluten.metrics.BatchWriteMetrics

import org.apache.iceberg.{PartitionSpec, SortOrder}
import org.scalatest.funsuite.AnyFunSuite

class IcebergColumnarBatchDataWriterSuite extends AnyFunSuite {

  test("native Iceberg writer closes native handle after abort failure") {
    val jniWrapper = new RecordingIcebergWriteJniWrapper(failAbort = true)
    val writer = writerWith(jniWrapper, writerHandle = 7L)

    val abortError = intercept[RuntimeException] {
      writer.abort()
    }

    assert(abortError.getMessage.contains("abort failed"))
    assert(jniWrapper.aborts == Seq(7L))
    assert(jniWrapper.closes.isEmpty)

    writer.close()
    writer.close()

    assert(jniWrapper.aborts == Seq(7L))
    assert(jniWrapper.closes == Seq(7L))
  }

  test("native Iceberg writer abort and close are idempotent after failed task attempt") {
    val jniWrapper = new RecordingIcebergWriteJniWrapper()
    val writer = writerWith(jniWrapper, writerHandle = 8L)

    writer.abort()
    writer.abort()
    writer.close()
    writer.close()

    assert(jniWrapper.aborts == Seq(8L))
    assert(jniWrapper.closes == Seq(8L))
  }

  test("native Iceberg writer rejects metrics after close without touching native metrics") {
    val jniWrapper = new RecordingIcebergWriteJniWrapper()
    val writer = writerWith(jniWrapper, writerHandle = 9L)

    writer.close()

    val closedError = intercept[IllegalStateException] {
      writer.currentMetricsValues()
    }

    assert(closedError.getMessage.contains("after native Iceberg writer is closed"))
    assert(jniWrapper.metricsReads == Seq.empty)
    assert(jniWrapper.closes == Seq(9L))
  }

  test("native Iceberg writer cleans native files when commit message parsing fails") {
    val jniWrapper = new RecordingIcebergWriteJniWrapper(
      commitMessages = Array("not-json"))
    val writer = writerWith(jniWrapper, writerHandle = 10L)

    val commitError = intercept[Exception] {
      writer.commit()
    }

    assert(commitError.getMessage.contains("Unrecognized token") || commitError
      .getMessage
      .contains("not-json"))
    assert(jniWrapper.commits == Seq(10L))
    assert(jniWrapper.cleanups == Seq(10L))
    assert(jniWrapper.aborts.isEmpty)

    writer.abort()
    writer.close()

    assert(jniWrapper.aborts.isEmpty)
    assert(jniWrapper.closes == Seq(10L))
  }

  test("native Iceberg writer preserves commit failure when native cleanup also fails") {
    val jniWrapper = new RecordingIcebergWriteJniWrapper(
      commitMessages = Array("not-json"),
      failCleanup = true)
    val writer = writerWith(jniWrapper, writerHandle = 11L)

    val commitError = intercept[Exception] {
      writer.commit()
    }

    assert(jniWrapper.cleanups == Seq(11L))
    assert(commitError.getSuppressed.exists(_.getMessage.contains("cleanup failed")))
  }

  test("native Iceberg writer rejects terminal-state commit retries before JNI") {
    val jniWrapper = new RecordingIcebergWriteJniWrapper()
    val writer = writerWith(jniWrapper, writerHandle = 12L)

    writer.commit()
    writer.currentMetricsValues()

    val secondCommitError = intercept[IllegalStateException] {
      writer.commit()
    }
    val lateWriteError = intercept[IllegalStateException] {
      writer.write(null)
    }

    assert(secondCommitError.getMessage.contains("writer is completed"))
    assert(lateWriteError.getMessage.contains("writer is completed"))
    assert(jniWrapper.commits == Seq(12L))
    assert(jniWrapper.metricsReads == Seq(12L))
    assert(jniWrapper.aborts.isEmpty)
  }

  test("native Iceberg writer rejects commit after abort before JNI") {
    val jniWrapper = new RecordingIcebergWriteJniWrapper()
    val writer = writerWith(jniWrapper, writerHandle = 13L)

    writer.abort()

    val commitAfterAbortError = intercept[IllegalStateException] {
      writer.commit()
    }

    assert(commitAfterAbortError.getMessage.contains("writer is completed"))
    assert(jniWrapper.aborts == Seq(13L))
    assert(jniWrapper.commits.isEmpty)
  }

  private def writerWith(
      jniWrapper: RecordingIcebergWriteJniWrapper,
      writerHandle: Long): IcebergColumnarBatchDataWriter = {
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
    failCleanup: Boolean = false,
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
    if (failCleanup) {
      throw new RuntimeException(s"cleanup failed for $writerHandle")
    }
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
