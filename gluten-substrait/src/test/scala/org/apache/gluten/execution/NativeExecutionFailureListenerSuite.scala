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

import org.apache.spark.{ExceptionFailure, TaskEndReason}
import org.apache.spark.scheduler.{AccumulableInfo, SparkListenerTaskEnd}
import org.apache.spark.util.AccumulatorV2

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.immutable.Seq

class NativeExecutionFailureListenerSuite extends AnyFunSuite {

  test("listener cancels stage for serialized query-failing native task failures") {
    val nativeFailure =
      NativeExecutionFailure("kafka", "data-loss", retryable = false, queryFailure = true)
    val cancellations = scala.collection.mutable.ArrayBuffer.empty[(Int, String)]
    val listener =
      new NativeExecutionFailureListener((stageId, reason) => cancellations += stageId -> reason)

    listener.onTaskEnd(taskEnd(7, serializedFailure(nativeFailure)))

    assert(cancellations.size == 1)
    assert(cancellations.head._1 == 7)
    assert(cancellations.head._2.contains("data-loss"))
  }

  test("direct throwable classifier detects query-failing native task failures") {
    val nativeFailure =
      NativeExecutionFailure("kafka", "data-loss", retryable = false, queryFailure = true)
    val cause = new NativeExecutionFailureException(nativeFailure, new RuntimeException("boom"))
    val outer = new RuntimeException("outer", cause)

    assert(NativeExecutionFailureListener.queryFailureFromThrowable(outer) == Some(nativeFailure))
  }

  test("direct throwable classifier ignores retryable native task failures") {
    val nativeFailure =
      NativeExecutionFailure("kafka", "timeout", retryable = true, queryFailure = false)
    val cause = new NativeExecutionFailureException(nativeFailure, new RuntimeException("boom"))

    assert(NativeExecutionFailureListener.queryFailureFromThrowable(cause).isEmpty)
  }

  test("listener recovers query-failing native task failures from serialized descriptions") {
    val nativeFailure =
      NativeExecutionFailure("kafka", "fatal", retryable = false, queryFailure = true)
    val cancellations = scala.collection.mutable.ArrayBuffer.empty[(Int, String)]
    val listener =
      new NativeExecutionFailureListener((stageId, reason) => cancellations += stageId -> reason)

    listener.onTaskEnd(taskEnd(9, serializedFailure(nativeFailure)))

    assert(cancellations.size == 1)
    assert(cancellations.head._1 == 9)
    assert(cancellations.head._2.contains("fatal"))
  }

  test("listener recovers query-failing native task failures from prefixed descriptions") {
    val nativeFailure =
      NativeExecutionFailure("kafka", "data-loss", retryable = false, queryFailure = true)
    val cancellations = scala.collection.mutable.ArrayBuffer.empty[(Int, String)]
    val listener =
      new NativeExecutionFailureListener((stageId, reason) => cancellations += stageId -> reason)

    listener.onTaskEnd(taskEnd(11, prefixedSerializedFailure(nativeFailure)))

    assert(cancellations.size == 1)
    assert(cancellations.head._1 == 11)
    assert(cancellations.head._2.contains("data-loss"))
  }

  test("listener ignores unclassified task failures") {
    val cancellations = scala.collection.mutable.ArrayBuffer.empty[(Int, String)]
    val listener =
      new NativeExecutionFailureListener((stageId, reason) => cancellations += stageId -> reason)

    listener.onTaskEnd(taskEnd(10, plainFailure("plain executor failure")))

    assert(cancellations.isEmpty)
  }

  private def taskEnd(stageId: Int, reason: TaskEndReason): SparkListenerTaskEnd = {
    SparkListenerTaskEnd(stageId, 0, "ResultTask", reason, null, null, null)
  }

  private def serializedFailure(nativeFailure: NativeExecutionFailure): ExceptionFailure = {
    ExceptionFailure(
      classOf[NativeExecutionFailureException].getName,
      NativeExecutionFailurePolicies.describe(nativeFailure),
      Array.empty[StackTraceElement],
      "",
      None,
      noAccumUpdates,
      noAccums,
      noMetricPeaks
    )
  }

  private def prefixedSerializedFailure(nativeFailure: NativeExecutionFailure): ExceptionFailure = {
    ExceptionFailure(
      classOf[NativeExecutionFailureException].getName,
      classOf[NativeExecutionFailureException].getName + ": " +
        NativeExecutionFailurePolicies.describe(nativeFailure) + "\n\tat native.Kafka.poll",
      Array.empty[StackTraceElement],
      "",
      None,
      noAccumUpdates,
      noAccums,
      noMetricPeaks
    )
  }

  private def plainFailure(description: String): ExceptionFailure = {
    ExceptionFailure(
      classOf[RuntimeException].getName,
      description,
      Array.empty[StackTraceElement],
      "",
      None,
      noAccumUpdates,
      noAccums,
      noMetricPeaks
    )
  }

  private def noAccumUpdates: Seq[AccumulableInfo] = Seq.empty

  private def noAccums: Seq[AccumulatorV2[_, _]] = Seq.empty

  private def noMetricPeaks: Seq[Long] = Seq.empty
}
