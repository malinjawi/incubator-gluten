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

import org.scalatest.funsuite.AnyFunSuite

class NativeExecutionFailurePoliciesSuite extends AnyFunSuite {

  test("protect annotates classified native construction failures") {
    val cause = new RuntimeException("native construction failed")
    val failure = NativeExecutionFailure("test", "fatal", retryable = false, queryFailure = true)

    val thrown = intercept[NativeExecutionFailureException] {
      NativeExecutionFailurePolicies.withPoliciesForTest(
        Seq(MessagePolicy("construction", failure))) {
        NativeExecutionFailurePolicies.protect {
          throw cause
        }
      }
    }

    assert(thrown.failure == failure)
    assert(thrown.getCause eq cause)
    assert(thrown.getMessage.contains("policy=fail query"))
  }

  test("wrap annotates classified native hasNext and next failures") {
    val retryable =
      NativeExecutionFailure("test", "retriable", retryable = true, queryFailure = false)
    val queryFailure =
      NativeExecutionFailure("test", "data-loss", retryable = false, queryFailure = true)
    val hasNextCause = new RuntimeException("native hasNext failed")
    val nextCause = new RuntimeException("native next failed")

    val hasNextThrown = intercept[NativeExecutionFailureException] {
      NativeExecutionFailurePolicies.withPoliciesForTest(Seq(MessagePolicy("hasNext", retryable))) {
        NativeExecutionFailurePolicies.wrap(new Iterator[Int] {
          override def hasNext: Boolean = throw hasNextCause
          override def next(): Int = 1
        }).hasNext
      }
    }
    assert(hasNextThrown.failure == retryable)
    assert(hasNextThrown.getCause eq hasNextCause)
    assert(hasNextThrown.getMessage.contains("policy=allow task retry"))

    val nextThrown = intercept[NativeExecutionFailureException] {
      NativeExecutionFailurePolicies.withPoliciesForTest(Seq(MessagePolicy("next", queryFailure))) {
        NativeExecutionFailurePolicies.wrap(new Iterator[Int] {
          override def hasNext: Boolean = true
          override def next(): Int = throw nextCause
        }).next()
      }
    }
    assert(nextThrown.failure == queryFailure)
    assert(nextThrown.getCause eq nextCause)
    assert(nextThrown.getMessage.contains("policy=fail query"))
  }

  test("unclassified native failures pass through unchanged") {
    val cause = new IllegalArgumentException("plain native failure")

    val thrown = intercept[IllegalArgumentException] {
      NativeExecutionFailurePolicies.withPoliciesForTest(Seq.empty) {
        NativeExecutionFailurePolicies.wrap(new Iterator[Int] {
          override def hasNext: Boolean = true
          override def next(): Int = throw cause
        }).next()
      }
    }

    assert(thrown eq cause)
  }

  test("description parser round-trips policy action") {
    val queryFailure =
      NativeExecutionFailure("kafka", "data-loss", retryable = false, queryFailure = true)
    val retryable =
      NativeExecutionFailure("kafka", "timeout", retryable = true, queryFailure = false)
    val noRetry =
      NativeExecutionFailure("kafka", "end-of-partition", retryable = false, queryFailure = false)

    assert(
      NativeExecutionFailurePolicies.classifyDescription(
        NativeExecutionFailurePolicies.describe(queryFailure)) == Some(queryFailure))
    assert(
      NativeExecutionFailurePolicies.classifyDescription(
        NativeExecutionFailurePolicies.describe(retryable)) == Some(retryable))
    assert(
      NativeExecutionFailurePolicies.classifyDescription(
        NativeExecutionFailurePolicies.describe(noRetry)) == Some(noRetry))
    assert(NativeExecutionFailurePolicies.classifyDescription("plain executor failure").isEmpty)
  }

  test("description parser recovers native failure from Spark exception descriptions") {
    val queryFailure =
      NativeExecutionFailure("kafka", "data-loss", retryable = false, queryFailure = true)
    val retryable =
      NativeExecutionFailure("kafka", "timeout", retryable = true, queryFailure = false)

    assert(
      NativeExecutionFailurePolicies.classifyDescription(
        classOf[NativeExecutionFailureException].getName + ": " +
          NativeExecutionFailurePolicies.describe(queryFailure)) == Some(queryFailure))
    assert(
      NativeExecutionFailurePolicies.classifyDescription(
        "executor task failed\nCaused by: " +
          classOf[NativeExecutionFailureException].getName + ": " +
          NativeExecutionFailurePolicies.describe(retryable) + "\n\tat native.Kafka.poll") ==
        Some(retryable))
    assert(
      NativeExecutionFailurePolicies
        .classifyDescription(
          "native kafka data-loss without Gluten native policy marker")
        .isEmpty)
    assert(
      NativeExecutionFailurePolicies
        .classifyDescription(
          NativeExecutionFailurePolicies.describe(queryFailure) + "-not-the-action")
        .isEmpty)
  }

  private case class MessagePolicy(fragment: String, failure: NativeExecutionFailure)
    extends NativeExecutionFailurePolicy {
    override def classify(error: Throwable): Option[NativeExecutionFailure] = {
      Option(error.getMessage).filter(_.contains(fragment)).map(_ => failure)
    }
  }
}
