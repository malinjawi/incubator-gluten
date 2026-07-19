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

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

case class NativeExecutionFailure(
    source: String,
    label: String,
    retryable: Boolean,
    queryFailure: Boolean)

trait NativeExecutionFailurePolicy {
  def classify(error: Throwable): Option[NativeExecutionFailure]
}

class NativeExecutionFailureException(val failure: NativeExecutionFailure, cause: Throwable)
  extends RuntimeException(NativeExecutionFailurePolicies.describe(failure), cause)

object NativeExecutionFailurePolicies {
  private val QueryFailureAction = "fail query"
  private val RetryAction = "allow task retry"
  private val NoRetryAction = "do not retry"
  private val DescriptionPattern = (
    "(?s)^.*?Gluten native ([A-Za-z0-9_.-]+) failure classified as " +
      "([A-Za-z0-9_.-]+); policy=(" +
      QueryFailureAction + "|" + RetryAction + "|" + NoRetryAction + ")(?:\\s|$).*$").r

  @volatile private var loadedPolicies: Seq[NativeExecutionFailurePolicy] = _

  def classify(error: Throwable): Option[NativeExecutionFailure] = {
    policies.iterator.flatMap(policy => policy.classify(error)).take(1).toSeq.headOption
  }

  def protect[T](body: => T): T = {
    try {
      body
    } catch {
      case NonFatal(error) => throw annotate(error)
    }
  }

  def wrap[T](underlying: Iterator[T]): Iterator[T] = {
    new Iterator[T] {
      override def hasNext: Boolean = {
        try {
          underlying.hasNext
        } catch {
          case NonFatal(error) => throw annotate(error)
        }
      }

      override def next(): T = {
        try {
          underlying.next()
        } catch {
          case NonFatal(error) => throw annotate(error)
        }
      }
    }
  }

  private[execution] def describe(failure: NativeExecutionFailure): String = {
    val action =
      if (failure.queryFailure) {
        QueryFailureAction
      } else if (failure.retryable) {
        RetryAction
      } else {
        NoRetryAction
      }
    s"Gluten native ${failure.source} failure classified as ${failure.label}; policy=$action"
  }

  private[execution] def classifyDescription(
      description: String): Option[NativeExecutionFailure] = {
    Option(description).flatMap {
      case DescriptionPattern(source, label, action) =>
        Some(
          NativeExecutionFailure(
            source,
            label,
            retryable = action == RetryAction,
            queryFailure = action == QueryFailureAction))
      case _ => None
    }
  }

  private[execution] def withPoliciesForTest[T](
      testPolicies: Seq[NativeExecutionFailurePolicy])(body: => T): T = synchronized {
    val previous = loadedPolicies
    loadedPolicies = testPolicies
    try {
      body
    } finally {
      loadedPolicies = previous
    }
  }

  private def annotate(error: Throwable): Throwable = {
    error match {
      case alreadyClassified: NativeExecutionFailureException => alreadyClassified
      case other =>
        classify(other).map(new NativeExecutionFailureException(_, other)).getOrElse(other)
    }
  }

  private def policies: Seq[NativeExecutionFailurePolicy] = {
    var localPolicies = loadedPolicies
    if (localPolicies == null) {
      synchronized {
        localPolicies = loadedPolicies
        if (localPolicies == null) {
          localPolicies = loadPolicies()
          loadedPolicies = localPolicies
        }
      }
    }
    localPolicies
  }

  private def loadPolicies(): Seq[NativeExecutionFailurePolicy] = {
    val loader = Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getClass.getClassLoader)
    ServiceLoader.load(classOf[NativeExecutionFailurePolicy], loader).iterator().asScala.toSeq
  }
}
