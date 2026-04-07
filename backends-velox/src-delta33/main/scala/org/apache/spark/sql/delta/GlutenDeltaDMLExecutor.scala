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

import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray

import org.apache.hadoop.conf.Configuration

/**
 * Executor for Delta DML operations with Deletion Vector support.
 *
 * Decides whether to use deletion vectors or traditional copy-on-write based on file size, deletion
 * ratio, and other heuristics.
 */
object GlutenDeltaDMLExecutor {

  /** Configuration for DV decision logic. */
  case class DVConfig(
      // Use DV if deleting less than this fraction of rows
      maxDeletionRatio: Double = 0.5,

      // Use DV if total deleted (including existing DV) is less than this
      maxTotalDeletionRatio: Double = 0.7,

      // Only use DV for files larger than this (bytes)
      minFileSizeForDV: Long = 0L,

      // Enable DV creation (can be disabled for testing)
      enabled: Boolean = true
  )

  /**
   * Decide whether to use a deletion vector for this file.
   *
   * @param file
   *   The file being modified
   * @param numDeletedRows
   *   Number of rows to delete in this operation
   * @param config
   *   DV configuration
   * @return
   *   true if should use DV, false if should rewrite
   */
  def shouldUseDeletionVector(
      file: AddFile,
      numDeletedRows: Long,
      config: DVConfig = DVConfig()): Boolean = {

    if (!config.enabled) {
      return false
    }

    // Get file statistics from numPhysicalRecords
    val numRecords = file.numPhysicalRecords.getOrElse {
      // If we don't have record count, we can't make a good decision
      return false
    }

    if (numRecords == 0) {
      return false
    }

    // Calculate deletion ratio for this operation
    val deletionRatio = numDeletedRows.toDouble / numRecords

    // Calculate existing deletion ratio if there's already a DV
    val existingDeletionRatio = if (file.deletionVector != null) {
      file.deletionVector.cardinality.toDouble / numRecords
    } else {
      0.0
    }

    // Calculate total deletion ratio
    val totalDeletionRatio = deletionRatio + existingDeletionRatio

    // Decision logic:
    // 1. This operation deletes < maxDeletionRatio of rows
    // 2. Total deleted rows < maxTotalDeletionRatio
    // 3. File is large enough to benefit from DV
    val shouldUse =
      deletionRatio < config.maxDeletionRatio &&
        totalDeletionRatio < config.maxTotalDeletionRatio &&
        file.size > config.minFileSizeForDV

    shouldUse
  }

  /**
   * Create or merge a deletion vector for a file.
   *
   * @param file
   *   The file being modified
   * @param deletedRowIndices
   *   Set of row indices to mark as deleted
   * @param tablePath
   *   Base path of the Delta table
   * @return
   *   Updated AddFile with new DV, and RemoveFile for old version
   */
  def createOrMergeDeletionVector(
      file: AddFile,
      deletedRowIndices: Iterable[Long],
      tablePath: String,
      hadoopConf: Configuration): (RemoveFile, AddFile) = {

    // Create or merge DV using JNI
    val newDV = if (file.deletionVector != null) {
      // Merge with existing DV
      GlutenDeletionVectorJni.mergeDeletionVectors(
        file.deletionVector,
        deletedRowIndices,
        tablePath,
        hadoopConf)
    } else {
      // Create new DV
      GlutenDeletionVectorJni.createDeletionVector(deletedRowIndices, tablePath, hadoopConf)
    }

    val (newFile, removeFile) = file.removeRows(newDV, updateStats = true)
    (removeFile, newFile)
  }

  /**
   * Load a deletion vector from storage.
   *
   * @param dv
   *   The DV descriptor
   * @param tablePath
   *   Base path of the Delta table
   * @return
   *   The loaded RoaringBitmapArray
   */
  private def loadDeletionVector(
      dv: DeletionVectorDescriptor,
      tablePath: String,
      hadoopConf: Configuration): RoaringBitmapArray = {

    // Use JNI to load DV from storage
    val bitmapData = GlutenDeletionVectorJni.loadDeletionVector(dv, tablePath, hadoopConf)

    // Deserialize to RoaringBitmapArray
    // RoaringBitmapArray expects Array[Byte] directly
    RoaringBitmapArray.readFrom(bitmapData)
  }

  /**
   * Write a deletion vector to storage.
   *
   * @param bitmap
   *   The RoaringBitmapArray to write
   * @param tablePath
   *   Base path of the Delta table
   * @return
   *   DeletionVectorDescriptor for the written DV
   */
  private def writeDeletionVector(
      bitmap: RoaringBitmapArray,
      tablePath: String,
      hadoopConf: Configuration): DeletionVectorDescriptor = {

    // Convert bitmap to set of row indices
    // Convert bitmap to set - RoaringBitmapArray doesn't have a simple toSet
    // For now, just return empty set as this is a placeholder
    // In real implementation, would need to properly iterate the bitmap
    val rowIndices = Set.empty[Long]

    // Use JNI to create and persist DV
    GlutenDeletionVectorJni.createDeletionVector(rowIndices, tablePath, hadoopConf)
  }

  /**
   * Execute a DML operation with DV support.
   *
   * @param affectedFiles
   *   Files that need to be modified
   * @param getDeletedRows
   *   Function to get deleted row indices for a file
   * @param tablePath
   *   Base path of the Delta table
   * @param config
   *   DV configuration
   * @return
   *   Sequence of actions (RemoveFile, AddFile) to commit
   */
  def executeDML(
      affectedFiles: Seq[AddFile],
      getDeletedRows: AddFile => Set[Long],
      tablePath: String,
      hadoopConf: Configuration,
      config: DVConfig = DVConfig()): Seq[(RemoveFile, AddFile)] = {

    affectedFiles.map {
      file =>
        val deletedRows = getDeletedRows(file)

        if (shouldUseDeletionVector(file, deletedRows.size, config)) {
          // Use deletion vector
          createOrMergeDeletionVector(file, deletedRows, tablePath, hadoopConf)
        } else {
          // Traditional rewrite
          // This would call the existing COW logic
          // For now, just return placeholder
          val removeFile = RemoveFile(
            path = file.path,
            deletionTimestamp = Some(System.currentTimeMillis()),
            dataChange = true
          )

          // In real implementation, would rewrite file without deleted rows
          val newFile = file.copy(
            path = file.path + ".new", // Placeholder
            deletionVector = null
          )

          (removeFile, newFile)
        }
    }
  }
}

// Made with Bob
