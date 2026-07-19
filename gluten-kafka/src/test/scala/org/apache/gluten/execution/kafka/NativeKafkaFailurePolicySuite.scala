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
package org.apache.gluten.execution.kafka

import org.apache.gluten.execution.{NativeExecutionFailureException, NativeExecutionFailurePolicies}

import org.scalatest.funsuite.AnyFunSuite

class NativeKafkaFailurePolicySuite extends AnyFunSuite {

  test("classifies native Kafka seek poll and watermark labels") {
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka consumer timeout seek failure for topic=t, partition=0, offset=1: " +
          "timed out") ==
        Some(NativeKafkaFailureKind.Timeout))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka consumer retriable poll failure while reading a Spark-planned finite " +
          "range: " +
          "transport error") == Some(NativeKafkaFailureKind.Retriable))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka consumer data-loss watermark lookup failure for topic=t, partition=0: " +
          "offset out of range") == Some(NativeKafkaFailureKind.DataLoss))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka consumer fatal poll failure while reading a Spark-planned finite range: " +
          "broker protocol error") == Some(NativeKafkaFailureKind.Fatal))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka consumer fatal config failure: requires 'bootstrap.servers' or " +
          "'metadata.broker.list' in Kafka params for topic=t, partition=0") ==
        Some(NativeKafkaFailureKind.Fatal))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka consumer fatal create failure: failed to create Kafka consumer: " +
          "invalid configuration") == Some(NativeKafkaFailureKind.Fatal))
  }

  test("classifies native Kafka finite reader labels") {
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka reader data-loss finite-range failure: planned startOffset 1 is before " +
          "earliest available offset 3 for topic=t, partition=0") ==
        Some(NativeKafkaFailureKind.DataLoss))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka reader timeout poll failure before Spark planned end offset: expected " +
          "offset 3 before endOffset 4 for topic=t, partition=0") ==
        Some(NativeKafkaFailureKind.Timeout))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka reader fatal finite-range failure: received record for topic=t2, " +
          "partition=0 while reading topic=t, partition=0") ==
        Some(NativeKafkaFailureKind.Fatal))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka reader fatal finite-range failure: received negative offset -1 for " +
          "topic=t, partition=0 while reading Spark-planned offsets=[5, 6)") ==
        Some(NativeKafkaFailureKind.Fatal))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka reader fatal watermark lookup failure: requires non-negative monotonic " +
          "watermark offsets for topic=t, partition=0, watermarks=[6, 5)") ==
        Some(NativeKafkaFailureKind.Fatal))
  }

  test("classifies native Kafka connector and data source labels") {
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka connector fatal consumer-factory failure: reached topic=t, " +
          "partition=0, offsets=[1, 2), but no Kafka consumer factory is registered") ==
        Some(NativeKafkaFailureKind.Fatal))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka data source fatal split failure: requires KafkaConnectorSplit") ==
        Some(NativeKafkaFailureKind.Fatal))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka data source fatal projection failure: output column 'headers' requests " +
          "Spark Kafka headers") == Some(NativeKafkaFailureKind.Fatal))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka data source fatal batch-size failure: requires a positive requested " +
          "batch size") == Some(NativeKafkaFailureKind.Fatal))
    assert(
      NativeKafkaFailurePolicy.classify(
        "Native Kafka connector fatal table-handle failure: requires KafkaTableHandle") ==
        Some(NativeKafkaFailureKind.Fatal))
  }

  test("exposes retry and fail-query policy for native Kafka labels") {
    assert(NativeKafkaFailurePolicy.isRetryable(NativeKafkaFailureKind.Timeout))
    assert(NativeKafkaFailurePolicy.isRetryable(NativeKafkaFailureKind.Retriable))
    assert(!NativeKafkaFailurePolicy.isRetryable(NativeKafkaFailureKind.DataLoss))
    assert(!NativeKafkaFailurePolicy.isRetryable(NativeKafkaFailureKind.Fatal))
    assert(!NativeKafkaFailurePolicy.isRetryable(NativeKafkaFailureKind.EndOfPartition))

    assert(!NativeKafkaFailurePolicy.shouldFailQuery(NativeKafkaFailureKind.Timeout))
    assert(!NativeKafkaFailurePolicy.shouldFailQuery(NativeKafkaFailureKind.Retriable))
    assert(NativeKafkaFailurePolicy.shouldFailQuery(NativeKafkaFailureKind.DataLoss))
    assert(NativeKafkaFailurePolicy.shouldFailQuery(NativeKafkaFailureKind.Fatal))
    assert(!NativeKafkaFailurePolicy.shouldFailQuery(NativeKafkaFailureKind.EndOfPartition))
  }

  test("classifies nested causes and suppressed native Kafka failures") {
    val nested = new RuntimeException(
      "Spark wrapper",
      new IllegalStateException(
        "Native Kafka consumer retriable seek failure for topic=t, partition=1, offset=7: " +
          "all brokers down"))
    assert(NativeKafkaFailurePolicy.classify(nested) == Some(NativeKafkaFailureKind.Retriable))
    assert(NativeKafkaFailurePolicy.isRetryable(nested))
    assert(!NativeKafkaFailurePolicy.shouldFailQuery(nested))

    val suppressed = new RuntimeException("Spark wrapper")
    suppressed.addSuppressed(
      new RuntimeException(
        "Native Kafka consumer fatal watermark lookup failure for topic=t, partition=1: " +
          "unknown broker error"))
    assert(NativeKafkaFailurePolicy.classify(suppressed) == Some(NativeKafkaFailureKind.Fatal))
    assert(!NativeKafkaFailurePolicy.isRetryable(suppressed))
    assert(NativeKafkaFailurePolicy.shouldFailQuery(suppressed))
  }

  test("ignores unrelated messages and label-like substrings") {
    assert(
      NativeKafkaFailurePolicy
        .classify("Native Kafka consumer retryable-ish poll failure")
        .isEmpty)
    assert(
      NativeKafkaFailurePolicy
        .classify("Native Kafka consumer fatalistic poll failure")
        .isEmpty)
    assert(NativeKafkaFailurePolicy.classify("Kafka retriable error without native label").isEmpty)
    assert(
      NativeKafkaFailurePolicy
        .classify(new RuntimeException("plain Spark task failure"))
        .isEmpty)
  }

  test("registers Kafka policy with generic native execution failure wrapper") {
    val cause = new RuntimeException(
      "Native Kafka consumer data-loss poll failure while reading a Spark-planned finite range: " +
        "offset out of range")

    val classified = NativeExecutionFailurePolicies.classify(cause)
    assert(classified.exists(_.source == "kafka"))
    assert(classified.exists(_.label == "data-loss"))
    assert(classified.exists(_.queryFailure))

    val thrown = intercept[NativeExecutionFailureException] {
      NativeExecutionFailurePolicies.wrap(new Iterator[Int] {
        override def hasNext: Boolean = true
        override def next(): Int = throw cause
      }).next()
    }
    assert(thrown.failure.source == "kafka")
    assert(thrown.failure.label == "data-loss")
    assert(thrown.failure.queryFailure)
    assert(thrown.getCause eq cause)
  }

  test("registers native Kafka reader data-loss with generic failure wrapper") {
    val cause = new RuntimeException(
      "Native Kafka reader data-loss finite-range failure: planned endOffset 9 is after " +
        "latest available offset 7 for topic=t, partition=0")

    val classified = NativeExecutionFailurePolicies.classify(cause)
    assert(classified.exists(_.source == "kafka"))
    assert(classified.exists(_.label == "data-loss"))
    assert(classified.exists(_.queryFailure))

    val thrown = intercept[NativeExecutionFailureException] {
      NativeExecutionFailurePolicies.wrap(new Iterator[Int] {
        override def hasNext: Boolean = true
        override def next(): Int = throw cause
      }).next()
    }
    assert(thrown.failure.source == "kafka")
    assert(thrown.failure.label == "data-loss")
    assert(thrown.failure.queryFailure)
    assert(thrown.getCause eq cause)
  }
}
