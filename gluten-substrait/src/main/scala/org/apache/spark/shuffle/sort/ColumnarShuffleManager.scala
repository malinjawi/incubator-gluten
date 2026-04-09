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
package org.apache.spark.shuffle.sort

import org.apache.gluten.shuffle.SupportsColumnarShuffle

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.storage.BlockId
import org.apache.spark.util.collection.OpenHashSet

import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

class ColumnarShuffleManager(conf: SparkConf)
  extends ShuffleManager
  with SupportsColumnarShuffle
  with Logging {

  import ColumnarShuffleManager._

  private lazy val vanillaShuffleManager = new SortShuffleManager(conf)
  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)
  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /** A mapping from shuffle ids to the number of mappers producing output for those shuffles. */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  /** Obtains a [[ShuffleHandle]] to pass to tasks. */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (dependency.isInstanceOf[ColumnarShuffleDependency[_, _, _]]) {
      logInfo(s"Registering ColumnarShuffle shuffleId: $shuffleId")
      new ColumnarShuffleHandle[K, V](
        shuffleId,
        dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]])
    } else {
      vanillaShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case columnarShuffleHandle: ColumnarShuffleHandle[K @unchecked, V @unchecked] =>
        val mapTaskIds =
          taskIdMapsForShuffle.computeIfAbsent(handle.shuffleId, _ => new OpenHashSet[Long](16))
        mapTaskIds.synchronized {
          mapTaskIds.add(mapId)
        }
        GlutenShuffleUtils.genColumnarShuffleWriter(
          shuffleBlockResolver,
          columnarShuffleHandle,
          mapId,
          metrics)
      case _ =>
        vanillaShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case _: ColumnarShuffleHandle[_, _] =>
        GlutenShuffleUtils.genColumnarShuffleReader(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics)
      case _ =>
        vanillaShuffleManager.getReader(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(taskIdMapsForShuffle.remove(shuffleId)) match {
      case Some(mapTaskIds) =>
        mapTaskIds.iterator.foreach {
          mapId => shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
        }
        true
      case None =>
        vanillaShuffleManager.unregisterShuffle(shuffleId)
    }
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
    vanillaShuffleManager.stop()
  }
}

object ColumnarShuffleManager extends Logging {
  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }

  def bypassDecompressionSerializerManger: SerializerManager =
    new SerializerManager(
      SparkEnv.get.serializer,
      SparkEnv.get.conf,
      SparkEnv.get.securityManager.getIOEncryptionKey()) {
      // Bypass the shuffle read decompression, decryption is not supported
      override def wrapStream(blockId: BlockId, s: InputStream): InputStream = {
        s
      }
    }
}

private[spark] class ColumnarShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {}
