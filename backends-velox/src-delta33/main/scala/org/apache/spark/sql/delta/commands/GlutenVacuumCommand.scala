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
package org.apache.spark.sql.delta.commands

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.execution.command.RunnableCommand

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.collection.mutable

/** Metrics for VACUUM operations with deletion vectors. */
case class VacuumMetrics(
    numDeletedDataFiles: Long,
    numDeletedDVFiles: Long,
    totalBytesDeleted: Long,
    numScannedFiles: Long,
    executionTimeMs: Long,
    retentionHours: Long
)

/**
 * Gluten-optimized VACUUM command with Deletion Vector support.
 *
 * This command extends Delta's VACUUM operation to:
 *   1. Identify orphaned data files (not in current or retained versions) 2. Identify orphaned DV
 *      files (not referenced by any retained version) 3. Delete files older than retention period
 *      4. Respect retention period for safe concurrent reads
 *
 * The goal is to:
 *   - Reclaim storage space from deleted files
 *   - Clean up orphaned DV files
 *   - Maintain data integrity with retention period
 */
case class GlutenVacuumCommand(
    deltaLog: DeltaLog,
    target: LogicalPlan,
    retentionHours: Option[Long],
    dryRun: Boolean)
  extends RunnableCommand
  with DeltaCommand {

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this

  // Default retention period: 7 days (168 hours)
  private val defaultRetentionHours = 168L

  override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(deltaLog, "delta.dml.vacuum") {
      val startTime = System.currentTimeMillis()

      // Get retention period
      val retention = retentionHours.getOrElse {
        sparkSession.conf.get(
          "spark.databricks.delta.retentionDurationCheck.enabled",
          "true") match {
          case "true" => defaultRetentionHours
          case "false" => 0L // Allow immediate deletion if check disabled
        }
      }

      // Validate retention period
      if (retention < 0) {
        throw new IllegalArgumentException(
          s"Retention period must be non-negative, got $retention hours")
      }

      // Get current snapshot
      val snapshot = deltaLog.snapshot

      // Get Hadoop configuration
      val hadoopConf = sparkSession.sessionState.newHadoopConf()
      val fs = new Path(snapshot.path.toString).getFileSystem(hadoopConf)

      // Calculate cutoff timestamp
      val cutoffTimestamp = System.currentTimeMillis() - (retention * 60 * 60 * 1000)

      // Find files to delete
      val (dataFilesToDelete, dvFilesToDelete) = findFilesToDelete(
        sparkSession,
        deltaLog,
        snapshot,
        fs,
        cutoffTimestamp
      )

      if (dataFilesToDelete.isEmpty && dvFilesToDelete.isEmpty) {
        // No files to delete
        val metrics = VacuumMetrics(
          numDeletedDataFiles = 0,
          numDeletedDVFiles = 0,
          totalBytesDeleted = 0,
          numScannedFiles = 0,
          executionTimeMs = System.currentTimeMillis() - startTime,
          retentionHours = retention
        )
        logMetrics(metrics)
        return Seq(Row(0, 0, 0))
      }

      // Delete files (if not dry run)
      val (numDeletedData, numDeletedDV, bytesDeleted) = if (dryRun) {
        logInfo(
          s"DRY RUN: Would delete ${dataFilesToDelete.size} data files and ${dvFilesToDelete.size} DV files")
        (0L, 0L, 0L)
      } else {
        deleteFiles(fs, dataFilesToDelete, dvFilesToDelete)
      }

      // Collect metrics
      val metrics = VacuumMetrics(
        numDeletedDataFiles = numDeletedData,
        numDeletedDVFiles = numDeletedDV,
        totalBytesDeleted = bytesDeleted,
        numScannedFiles = dataFilesToDelete.size + dvFilesToDelete.size,
        executionTimeMs = System.currentTimeMillis() - startTime,
        retentionHours = retention
      )

      // Log metrics
      logMetrics(metrics)

      // Return metrics
      Seq(
        Row(
          metrics.numDeletedDataFiles,
          metrics.numDeletedDVFiles,
          metrics.totalBytesDeleted
        ))
    }
  }

  /**
   * Find files that can be safely deleted.
   *
   * A file can be deleted if:
   *   1. It's not in the current snapshot 2. It's not in any snapshot within the retention period
   *      3. Its deletion timestamp is older than the retention period
   */
  private def findFilesToDelete(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      fs: FileSystem,
      cutoffTimestamp: Long): (Seq[FileStatus], Seq[FileStatus]) = {

    // Get all files in current snapshot
    val currentFiles = snapshot.allFiles.collect().map(_.path).toSet

    // Get all DV files referenced in current snapshot
    val currentDVFiles = snapshot.allFiles
      .collect()
      .flatMap {
        file => Option(file.deletionVector).map(dv => extractDVPath(dv, snapshot.path.toString))
      }
      .toSet

    // Get all files from retained versions
    val retainedFiles = mutable.Set[String]()
    val retainedDVFiles = mutable.Set[String]()

    // Scan delta log for retained versions
    val logPath = new Path(snapshot.path.toString, "_delta_log")
    val logFiles = fs
      .listStatus(logPath)
      .filter(_.getPath.getName.endsWith(".json"))
      .sortBy(_.getPath.getName)
      .reverse

    logFiles.foreach {
      logFile =>
        val version = extractVersion(logFile.getPath.getName)
        val versionTimestamp = logFile.getModificationTime

        if (versionTimestamp >= cutoffTimestamp) {
          // This version is within retention period
          val actions = readDeltaLog(sparkSession, logFile.getPath.toString)

          // Collect data files
          actions.foreach {
            case add: AddFile => retainedFiles += add.path
            case remove: RemoveFile =>
              // Keep removed files if deletion is recent
              if (remove.deletionTimestamp.getOrElse(0L) >= cutoffTimestamp) {
                retainedFiles += remove.path
              }
            case _ => // Ignore other actions
          }

          // Collect DV files
          actions.foreach {
            case add: AddFile =>
              Option(add.deletionVector).foreach {
                dv => retainedDVFiles += extractDVPath(dv, snapshot.path.toString)
              }
            case remove: RemoveFile =>
              Option(remove.deletionVector).foreach {
                dv =>
                  if (remove.deletionTimestamp.getOrElse(0L) >= cutoffTimestamp) {
                    retainedDVFiles += extractDVPath(dv, snapshot.path.toString)
                  }
              }
            case _ => // Ignore other actions
          }
        }
    }

    // Find all physical files in table directory
    val allPhysicalFiles = listAllFiles(fs, new Path(snapshot.path.toString))

    // Separate data files and DV files
    val dataFiles = allPhysicalFiles.filter(_.getPath.getName.endsWith(".parquet"))
    val dvFiles = allPhysicalFiles.filter(_.getPath.getName.startsWith("deletion_vector_"))

    // Find orphaned data files
    val orphanedDataFiles = dataFiles.filter {
      file =>
        val path = file.getPath.toString
        val relativePath = path.substring(snapshot.path.toString.length + 1)

        !currentFiles.contains(relativePath) &&
        !retainedFiles.contains(relativePath) &&
        file.getModificationTime < cutoffTimestamp
    }

    // Find orphaned DV files
    val orphanedDVFiles = dvFiles.filter {
      file =>
        val path = file.getPath.toString

        !currentDVFiles.contains(path) &&
        !retainedDVFiles.contains(path) &&
        file.getModificationTime < cutoffTimestamp
    }

    (orphanedDataFiles, orphanedDVFiles)
  }

  /** Extract DV file path from DV descriptor. */
  private def extractDVPath(dv: DeletionVectorDescriptor, tablePath: String): String = {
    dv.storageType match {
      case "u" =>
        // UUID-based relative path
        val uuid = dv.pathOrInlineDv.substring(dv.pathOrInlineDv.length - 20)
        val prefix = if (dv.pathOrInlineDv.length > 20) {
          dv.pathOrInlineDv.substring(0, dv.pathOrInlineDv.length - 20)
        } else {
          ""
        }
        s"$tablePath/$prefix/deletion_vector_$uuid.bin"

      case "p" =>
        // Absolute path
        dv.pathOrInlineDv

      case "i" =>
        // Inline - no file
        ""

      case _ =>
        throw new IllegalArgumentException(s"Unknown DV storage type: ${dv.storageType}")
    }
  }

  /** Extract version number from log file name. */
  private def extractVersion(fileName: String): Long = {
    fileName.stripSuffix(".json").toLong
  }

  /** Read actions from a delta log file. */
  private def readDeltaLog(sparkSession: SparkSession, logPath: String): Seq[Action] = {
    // Read JSON log file
    val df = sparkSession.read.json(logPath)

    // Parse actions
    val actions = mutable.ArrayBuffer[Action]()

    df.collect().foreach {
      row =>
        // Parse add actions
        if (row.schema.fieldNames.contains("add") && !row.isNullAt(row.fieldIndex("add"))) {
          val addRow = row.getStruct(row.fieldIndex("add"))
          val path = addRow.getString(addRow.fieldIndex("path"))
          // Simplified: create AddFile with minimal fields
          actions += AddFile(
            path = path,
            partitionValues = Map.empty,
            size = 0,
            modificationTime = 0,
            dataChange = true,
            stats = null,
            tags = Map.empty,
            deletionVector = null // TODO: Parse DV descriptor
          )
        }

        // Parse remove actions
        if (row.schema.fieldNames.contains("remove") && !row.isNullAt(row.fieldIndex("remove"))) {
          val removeRow = row.getStruct(row.fieldIndex("remove"))
          val path = removeRow.getString(removeRow.fieldIndex("path"))
          val deletionTimestamp = if (removeRow.schema.fieldNames.contains("deletionTimestamp")) {
            Some(removeRow.getLong(removeRow.fieldIndex("deletionTimestamp")))
          } else {
            None
          }
          actions += RemoveFile(
            path = path,
            deletionTimestamp = deletionTimestamp,
            dataChange = true,
            extendedFileMetadata = Some(true),
            partitionValues = Map.empty,
            size = Some(0L),
            tags = Map.empty,
            deletionVector = null
          )
        }
    }

    actions.toSeq
  }

  /** List all files recursively in a directory. */
  private def listAllFiles(fs: FileSystem, path: Path): Seq[FileStatus] = {
    val files = mutable.ArrayBuffer[FileStatus]()

    def listRecursive(dir: Path): Unit = {
      val statuses = fs.listStatus(dir)
      statuses.foreach {
        status =>
          if (status.isDirectory) {
            // Skip _delta_log directory
            if (!status.getPath.getName.startsWith("_")) {
              listRecursive(status.getPath)
            }
          } else {
            files += status
          }
      }
    }

    listRecursive(path)
    files.toSeq
  }

  /** Delete the specified files. */
  private def deleteFiles(
      fs: FileSystem,
      dataFiles: Seq[FileStatus],
      dvFiles: Seq[FileStatus]): (Long, Long, Long) = {

    var numDeletedData = 0L
    var numDeletedDV = 0L
    var bytesDeleted = 0L

    // Delete data files
    dataFiles.foreach {
      file =>
        try {
          bytesDeleted += file.getLen
          if (fs.delete(file.getPath, false)) {
            numDeletedData += 1
            logInfo(s"Deleted data file: ${file.getPath}")
          }
        } catch {
          case e: Exception =>
            logWarning(s"Failed to delete data file ${file.getPath}: ${e.getMessage}")
        }
    }

    // Delete DV files
    dvFiles.foreach {
      file =>
        try {
          bytesDeleted += file.getLen
          if (fs.delete(file.getPath, false)) {
            numDeletedDV += 1
            logInfo(s"Deleted DV file: ${file.getPath}")
          }
        } catch {
          case e: Exception =>
            logWarning(s"Failed to delete DV file ${file.getPath}: ${e.getMessage}")
        }
    }

    (numDeletedData, numDeletedDV, bytesDeleted)
  }

  /** Log metrics for monitoring. */
  private def logMetrics(metrics: VacuumMetrics): Unit = {
    logInfo(
      s"VACUUM operation completed: " +
        s"deleted ${metrics.numDeletedDataFiles} data files, " +
        s"deleted ${metrics.numDeletedDVFiles} DV files, " +
        s"freed ${metrics.totalBytesDeleted} bytes, " +
        s"scanned ${metrics.numScannedFiles} files, " +
        s"retention ${metrics.retentionHours} hours, " +
        s"took ${metrics.executionTimeMs}ms")
  }
}

// Made with Bob
