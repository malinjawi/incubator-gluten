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
package org.apache.spark.sql.delta.perf

import org.apache.gluten.vectorized.{ColumnarBatchSerializerInstance, NativePartitioning}

import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.shuffle.ColumnarShuffleDependency
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManagerId}

import scala.collection.mutable.ArrayBuffer

/**
 * White-box regression test for the Celeborn-to-local shuffle fallback ClassCastException in the
 * Delta optimized writer's own shuffle reader.
 *
 * [[GlutenOptimizedWriterShuffleReader]] used to match on the dependency type and hard-cast the
 * serializer of a [[ColumnarShuffleDependency]] to [[ColumnarBatchSerializerInstance]]. When
 * CelebornShuffleManager falls back to the local ColumnarShuffleManager, the dependency still
 * carries the Celeborn serializer bound at plan build time, which is a plain `SerializerInstance`,
 * so the cast failed. This mirrors the fix for the general [[ColumnarShuffleReader]]
 * (apache/gluten#12721).
 *
 * The cast happens before any block is fetched, so an empty block iterator is enough to exercise
 * the dispatch decision without native libraries, a Celeborn cluster or shuffle data.
 */
class GlutenOptimizedWriterShuffleReaderSuite extends SharedSparkSession {

  private def readWith(serializer: Serializer): Seq[Product2[Int, ColumnarBatch]] = {
    val dep = new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      spark.sparkContext.emptyRDD[(Int, ColumnarBatch)],
      new HashPartitioner(1),
      serializer = serializer,
      nativePartitioning = new NativePartitioning("hash", 1),
      metrics = Map.empty[String, SQLMetric]
    )
    val reader = new GlutenOptimizedWriterShuffleReader(
      dep,
      TaskContext.empty(),
      Iterator.empty: Iterator[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])],
      new TempShuffleReadMetrics())
    reader.read().toSeq
  }

  test(
    "read() tolerates a non-ColumnarBatchSerializerInstance on a ColumnarShuffleDependency " +
      "(Celeborn -> local fallback) instead of throwing ClassCastException") {
    // A plain row-based serializer stands in for the Celeborn serializer left on the
    // dependency after CelebornShuffleManager falls back to the local ColumnarShuffleManager.
    val nonColumnarSerializer = new JavaSerializer(spark.sparkContext.getConf)
    assert(!nonColumnarSerializer.newInstance().isInstanceOf[ColumnarBatchSerializerInstance])

    // Must not throw ClassCastException; empty input yields no rows.
    val rows = readWith(nonColumnarSerializer)
    assert(rows.isEmpty)
  }
}
