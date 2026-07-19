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

import org.apache.gluten.execution.{NativeExecutionFailure, NativeExecutionFailurePolicy}

import java.util.IdentityHashMap

sealed abstract class NativeKafkaFailureKind(
    val label: String,
    val retryable: Boolean,
    val queryFailure: Boolean)

object NativeKafkaFailureKind {
  case object EndOfPartition
    extends NativeKafkaFailureKind("end-of-partition", retryable = false, queryFailure = false)
  case object Timeout
    extends NativeKafkaFailureKind("timeout", retryable = true, queryFailure = false)
  case object Retriable
    extends NativeKafkaFailureKind("retriable", retryable = true, queryFailure = false)
  case object DataLoss
    extends NativeKafkaFailureKind("data-loss", retryable = false, queryFailure = true)
  case object Fatal
    extends NativeKafkaFailureKind("fatal", retryable = false, queryFailure = true)

  private val ByLabel: Map[String, NativeKafkaFailureKind] =
    Seq(EndOfPartition, Timeout, Retriable, DataLoss, Fatal).map(kind => kind.label -> kind).toMap

  def fromLabel(label: String): Option[NativeKafkaFailureKind] = ByLabel.get(label)
}

object NativeKafkaFailurePolicy {
  private val NativeFailurePattern =
    ("""Native Kafka (?:consumer|reader|connector|data source) """ +
      """(end-of-partition|timeout|retriable|data-loss|fatal) """ +
      """[a-z-]+(?: [a-z-]+)* failure\b""").r

  def classify(message: String): Option[NativeKafkaFailureKind] = {
    Option(message).flatMap {
      nonNullMessage =>
        NativeFailurePattern
          .findFirstMatchIn(nonNullMessage)
          .flatMap(m => NativeKafkaFailureKind.fromLabel(m.group(1)))
    }
  }

  def classify(error: Throwable): Option[NativeKafkaFailureKind] = {
    messages(error).flatMap(classify).take(1).toSeq.headOption
  }

  def isRetryable(kind: NativeKafkaFailureKind): Boolean = kind.retryable

  def isRetryable(error: Throwable): Boolean = classify(error).exists(isRetryable)

  def shouldFailQuery(kind: NativeKafkaFailureKind): Boolean = kind.queryFailure

  def shouldFailQuery(error: Throwable): Boolean = classify(error).exists(shouldFailQuery)

  private def messages(error: Throwable): Iterator[String] = {
    val seen = new IdentityHashMap[Throwable, java.lang.Boolean]()

    def loop(current: Throwable): Iterator[Throwable] = {
      if (current == null || seen.containsKey(current)) {
        Iterator.empty
      } else {
        seen.put(current, java.lang.Boolean.TRUE)
        Iterator(current) ++ loop(current.getCause) ++ current.getSuppressed.iterator.flatMap(loop)
      }
    }

    loop(error).flatMap(error => Option(error.getMessage))
  }
}

class NativeKafkaExecutionFailurePolicy extends NativeExecutionFailurePolicy {
  override def classify(error: Throwable): Option[NativeExecutionFailure] = {
    NativeKafkaFailurePolicy.classify(error).map {
      kind =>
        NativeExecutionFailure(
          source = "kafka",
          label = kind.label,
          retryable = kind.retryable,
          queryFailure = kind.queryFailure)
    }
  }
}
