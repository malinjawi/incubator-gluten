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
package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.util.PathWithFileSystem

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.util.UUID

import scala.util.Random

/**
 * Deletion vector helper used by the Velox Delta path.
 *
 * The original JNI implementation in this branch is incomplete and diverged from the current Velox
 * Delta APIs. For the Spark-side DML path we can rely on Delta's own Roaring bitmap and
 * DeletionVectorStore utilities and keep the native Velox path focused on scanning files with DVs.
 *
 * This helper intentionally writes on-disk DVs only. The native Velox inline-DV read path in this
 * branch is not protocol-compatible yet, while the on-disk path is already validated.
 */
object GlutenDeletionVectorJni {

  private def createDVStore(hadoopConf: Configuration): DeletionVectorStore = {
    DeletionVectorStore.createInstance(hadoopConf)
  }

  private def createBitmap(rowIndices: Iterable[Long]): RoaringBitmapArray = {
    val bitmap = new RoaringBitmapArray()
    rowIndices.foreach(bitmap.add)
    bitmap
  }

  private def serializeBitmap(bitmap: RoaringBitmapArray): Array[Byte] = {
    bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
  }

  private def writeBitmap(
      bitmap: RoaringBitmapArray,
      tablePath: String,
      hadoopConf: Configuration,
      randomPrefix: String = f"${Random.nextInt(256)}%02x"): DeletionVectorDescriptor = {
    if (bitmap.isEmpty) {
      DeletionVectorDescriptor.EMPTY
    } else {
      val dvStore = createDVStore(hadoopConf)
      val table = new Path(tablePath)
      val tableWithFS = PathWithFileSystem.withConf(table, hadoopConf)
      val fileId = UUID.randomUUID()
      val writerPath = dvStore.generateFileNameInTable(tableWithFS, fileId, randomPrefix)
      val writer = dvStore.createWriter(writerPath)
      try {
        val serializedBitmap = serializeBitmap(bitmap)
        val range = writer.write(serializedBitmap)
        DeletionVectorDescriptor.onDiskWithRelativePath(
          id = fileId,
          randomPrefix = randomPrefix,
          sizeInBytes = range.length,
          cardinality = bitmap.cardinality,
          offset = Some(range.offset),
          maxRowIndex = None)
      } finally {
        writer.close()
      }
    }
  }

  private def loadBitmap(
      dv: DeletionVectorDescriptor,
      tablePath: String,
      hadoopConf: Configuration): RoaringBitmapArray = {
    val dvStore = createDVStore(hadoopConf)
    StoredBitmap.create(dv, new Path(tablePath)).load(dvStore)
  }

  def createDeletionVector(
      rowIndices: Iterable[Long],
      tablePath: String,
      hadoopConf: Configuration): DeletionVectorDescriptor = {
    writeBitmap(createBitmap(rowIndices), tablePath, hadoopConf)
  }

  def createDeletionVector(
      rowIndices: Array[Long],
      tablePath: String,
      hadoopConf: Configuration): DeletionVectorDescriptor = {
    writeBitmap(createBitmap(rowIndices.toIndexedSeq), tablePath, hadoopConf)
  }

  def mergeDeletionVectors(
      existing: DeletionVectorDescriptor,
      newIndices: Iterable[Long],
      tablePath: String,
      hadoopConf: Configuration): DeletionVectorDescriptor = {
    val mergedBitmap = loadBitmap(existing, tablePath, hadoopConf)
    newIndices.foreach(mergedBitmap.add)
    writeBitmap(mergedBitmap, tablePath, hadoopConf)
  }

  def mergeDeletionVectors(
      existing: DeletionVectorDescriptor,
      newIndices: Array[Long],
      tablePath: String,
      hadoopConf: Configuration): DeletionVectorDescriptor = {
    val mergedBitmap = loadBitmap(existing, tablePath, hadoopConf)
    newIndices.foreach(mergedBitmap.add)
    writeBitmap(mergedBitmap, tablePath, hadoopConf)
  }

  def loadDeletionVector(
      dv: DeletionVectorDescriptor,
      tablePath: String,
      hadoopConf: Configuration): Array[Byte] = {
    serializeBitmap(loadBitmap(dv, tablePath, hadoopConf))
  }

  /**
   * Compatibility entry point for the experimental ClickHouse-side wrapper in this workspace. The
   * caller passes `<tablePath>/_delta_log`; this method writes the DV under `<tablePath>`.
   */
  def nativeWriteDVFile(
      deltaLogPath: String,
      rowIndices: Array[Long],
      randomPrefix: String): String = {
    val tablePath = Option(new Path(deltaLogPath).getParent)
      .map(_.toString)
      .getOrElse(deltaLogPath)
    val descriptor = writeBitmap(
      createBitmap(rowIndices.toSeq),
      tablePath,
      new Configuration(),
      Option(randomPrefix).getOrElse(""))
    val absolutePath = descriptor.absolutePath(new Path(tablePath)).toString
    s"""{"filePath":"${escapeJson(absolutePath)}","offset":${descriptor.offset.getOrElse(0)},"sizeInBytes":${descriptor.sizeInBytes},"cardinality":${descriptor.cardinality},"uniqueId":"${escapeJson(descriptor.pathOrInlineDv)}"}"""
  }

  private def escapeJson(value: String): String = {
    value
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
  }

  class DeletionVectorBuilder private[delta] () {
    private val bitmap = new RoaringBitmapArray()

    def addRow(rowIndex: Long): DeletionVectorBuilder = {
      bitmap.add(rowIndex)
      this
    }

    def addRange(startRow: Long, endRow: Long): DeletionVectorBuilder = {
      var current = startRow
      while (current <= endRow) {
        bitmap.add(current)
        current += 1
      }
      this
    }

    def merge(otherDV: Array[Byte]): DeletionVectorBuilder = {
      bitmap.merge(RoaringBitmapArray.readFrom(otherDV))
      this
    }

    def cardinality: Long = bitmap.cardinality

    def build(inline: Boolean = false): Array[Byte] = serializeBitmap(bitmap)

    def close(): Unit = {}
  }

  def newBuilder(): DeletionVectorBuilder = new DeletionVectorBuilder()
}
