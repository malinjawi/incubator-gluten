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
package org.apache.gluten.connector.write

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.execution.IcebergWriteJniWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.iceberg._
import org.apache.iceberg.spark.source.IcebergWriteUtil

import scala.util.control.NonFatal

case class IcebergColumnarBatchDataWriter(
    writer: Long,
    jniWrapper: IcebergWriteJniWrapper,
    format: Int,
    partitionSpec: PartitionSpec,
    sortOrder: SortOrder)
  extends DataWriter[ColumnarBatch]
  with Logging {

  private val mapper = {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  }
  private var nativeCompleted = false
  private var closed = false

  override def write(batch: ColumnarBatch): Unit = {
    requireActive("write")
    // Pass the original batch to native code
    // The native code will use the schema (writeSchema) we provided during initialization
    // to determine which columns to write, effectively filtering out metadata columns
    // like __row_operation, _file, _pos that Spark 4.0 adds
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    jniWrapper.write(writer, batchHandle)
  }

  override def commit: WriterCommitMessage = {
    requireActive("commit")
    val nativeCommitMessages = jniWrapper.commit(writer)
    try {
      val dataFiles = nativeCommitMessages.map(d => parseDataFile(d, partitionSpec, sortOrder))
      val commitMessage = IcebergWriteUtil.commitDataFiles(dataFiles)
      nativeCompleted = true
      commitMessage
    } catch {
      case NonFatal(e) =>
        cleanupCommittedFiles(e)
        nativeCompleted = true
        throw e
    }
  }

  override def abort(): Unit = {
    if (!closed && !nativeCompleted) {
      logInfo("Abort the ColumnarBatchDataWriter")
      jniWrapper.abort(writer)
      nativeCompleted = true
    }
  }

  override def close(): Unit = {
    if (!closed) {
      logDebug("Close the ColumnarBatchDataWriter")
      jniWrapper.close(writer)
      closed = true
    }
  }

  private def cleanupCommittedFiles(commitFailure: Throwable): Unit = {
    try {
      logWarning("Cleaning up native Iceberg files after commit message construction failed")
      jniWrapper.cleanupCommittedFiles(writer)
    } catch {
      case NonFatal(cleanupFailure) =>
        commitFailure.addSuppressed(cleanupFailure)
    }
  }

  private def parseDataFile(json: String, spec: PartitionSpec, sortOrder: SortOrder): DataFile = {
    val dataFile = mapper.readValue(json, classOf[DataFileJson])
    val builder = DataFiles
      .builder(spec)
      .withPath(dataFile.path)
      .withFormat(getFileFormat)
      .withFileSizeInBytes(dataFile.fileSizeInBytes)
      .withPartition(PartitionDataJson.fromJson(dataFile.partitionDataJson, partitionSpec))
      .withMetrics(dataFile.metrics.metrics())
      .withSplitOffsets(dataFile.splitOffsets)
      .withSortOrder(sortOrder)
    builder.build()
  }

  private def getFileFormat: FileFormat = {
    format match {
      case 0 => FileFormat.ORC
      case 1 => FileFormat.PARQUET
      case _ => throw new UnsupportedOperationException()
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    requireOpen("read metrics")
    jniWrapper.metrics(writer).toCustomTaskMetrics
  }

  private def requireOpen(action: String): Unit = {
    if (closed) {
      throw new IllegalStateException(s"Cannot $action after native Iceberg writer is closed")
    }
  }

  private def requireActive(action: String): Unit = {
    requireOpen(action)
    if (nativeCompleted) {
      throw new IllegalStateException(s"Cannot $action after native Iceberg writer is completed")
    }
  }
}
