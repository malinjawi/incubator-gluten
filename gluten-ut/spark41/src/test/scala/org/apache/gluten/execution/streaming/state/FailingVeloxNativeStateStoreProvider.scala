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
package org.apache.gluten.execution.streaming.state

import java.util.concurrent.atomic.AtomicBoolean

class FailingVeloxNativeStateStoreProvider extends VeloxNativeStateStoreProvider {
  override private[state] def commit(version: Long, snapshot: Array[Byte]): Long = {
    val failpoints = FailingVeloxNativeStateStoreProvider
    if (failpoints.failNextCommit.compareAndSet(true, false)) {
      throw new RuntimeException("injected Velox native StateStore commit failure")
    }
    val committedVersion = super.commit(version, snapshot)
    if (failpoints.failAfterNativeCommit.compareAndSet(true, false)) {
      throw new RuntimeException("injected Velox native StateStore post-commit failure")
    }
    committedVersion
  }
}

object FailingVeloxNativeStateStoreProvider {
  private[state] val failNextCommit = new AtomicBoolean(false)
  private[state] val failAfterNativeCommit = new AtomicBoolean(false)

  def failOnce(): Unit = {
    failNextCommit.set(true)
  }

  def failAfterNativeCommitOnce(): Unit = {
    failAfterNativeCommit.set(true)
  }

  def reset(): Unit = {
    failNextCommit.set(false)
    failAfterNativeCommit.set(false)
  }
}
