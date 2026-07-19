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

import org.apache.spark.{ExceptionFailure, SparkContext, TaskEndReason}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

import java.util.{Collections, IdentityHashMap, WeakHashMap}

private[execution] class NativeExecutionFailureListener(cancelStage: (Int, String) => Unit)
  extends SparkListener
  with Logging {

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    NativeExecutionFailureListener.queryFailure(event.reason).foreach {
      failure =>
        val reason = NativeExecutionFailureListener.stageCancelReason(failure)
        logWarning(reason)
        cancelStage(event.stageId, reason)
    }
  }
}

object NativeExecutionFailureListener extends Logging {
  private val RegisteredContexts =
    Collections.newSetFromMap(new WeakHashMap[SparkContext, java.lang.Boolean]())

  def register(sc: SparkContext): Unit = {
    RegisteredContexts.synchronized {
      if (RegisteredContexts.add(sc)) {
        val cancelStage = (stageId: Int, reason: String) => sc.cancelStage(stageId, reason)
        sc.addSparkListener(new NativeExecutionFailureListener(cancelStage))
        logInfo("Registered Gluten native execution failure listener.")
      }
    }
  }

  private[execution] def queryFailure(reason: TaskEndReason): Option[NativeExecutionFailure] = {
    reason match {
      case failure: ExceptionFailure =>
        failure
          .exception
          .flatMap(queryFailureFromThrowable(_))
          .orElse(queryFailureFromDescription(failure.description))
      case _ => None
    }
  }

  private[execution] def stageCancelReason(failure: NativeExecutionFailure): String = {
    s"Gluten native ${failure.source} failure classified as ${failure.label}; " +
      "canceling stage to fail query"
  }

  private[execution] def queryFailureFromThrowable(
      error: Throwable): Option[NativeExecutionFailure] = {
    val visited = Collections.newSetFromMap(new IdentityHashMap[Throwable, java.lang.Boolean]())

    def loop(candidate: Throwable): Option[NativeExecutionFailure] = {
      if (candidate == null || !visited.add(candidate)) {
        None
      } else {
        candidate match {
          case native: NativeExecutionFailureException if native.failure.queryFailure =>
            Some(native.failure)
          case _ =>
            loop(candidate.getCause)
              .orElse(candidate.getSuppressed.iterator.flatMap(loop).toSeq.headOption)
        }
      }
    }

    loop(error)
  }

  private def queryFailureFromDescription(description: String): Option[NativeExecutionFailure] = {
    NativeExecutionFailurePolicies.classifyDescription(description).filter(_.queryFailure)
  }
}
