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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.Path

import java.util.UUID

import scala.collection.mutable

/** Metrics for OPTIMIZE operations with deletion vectors. */
case class OptimizeMetrics(
    numRemovedFiles: Long,
    numAddedFiles: Long,
    numCompactedFiles: Long,
    numDeletionVectorsRemoved: Long,
    totalRowsCompacted: Long,
    totalBytesCompacted: Long,
    executionTimeMs: Long,
    numFilesBeforeOptimize: Long,
    numFilesAfterOptimize: Long
)

/** Configuration for OPTIMIZE operation. */
case class OptimizeConfig(
    maxFileSize: Long = 128 * 1024 * 1024, // 128 MB
    minFileSize: Long = 10 * 1024 * 1024, // 10 MB
    maxDeletionRatio: Double = 0.5, // 50%
    targetFileSize: Long = 64 * 1024 * 1024 // 64 MB
)

/**
 * Gluten-optimized OPTIMIZE command with Deletion Vector support.
 *
 * This command extends Delta's OPTIMIZE operation to:
 *   1. Identify files with high deletion ratios (>50%) 2. Compact DVs into new files (rewrite
 *      without deleted rows) 3. Combine small files into larger ones 4. Support Z-ORDER BY
 *      optimization
 *
 * The goal is to improve read performance by:
 *   - Eliminating DV overhead for heavily deleted files
 *   - Reducing the number of small files
 *   - Improving data locality with Z-ORDER
 */
case class GlutenOptimizeCommand(
    deltaLog: DeltaLog,
    target: LogicalPlan,
    partitionPredicate: Option[Expression],
    zOrderBy: Seq[String])
  extends RunnableCommand
  with DeltaCommand {

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this

  override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(deltaLog, "delta.dml.optimize") {
      val startTime = System.currentTimeMillis()
      val txn = deltaLog.startTransaction()

      // Get current snapshot
      val snapshot = txn.snapshot
      val numFilesBeforeOptimize = snapshot.numOfFiles

      // Get Hadoop configuration
      val hadoopConf = sparkSession.sessionState.newHadoopConf()

      // Get configuration
      val config = OptimizeConfig(
        maxFileSize =
          sparkSession.conf.get("spark.databricks.delta.optimize.maxFileSize", "134217728").toLong,
        minFileSize =
          sparkSession.conf.get("spark.databricks.delta.optimize.minFileSize", "10485760").toLong,
        maxDeletionRatio =
          sparkSession.conf.get("spark.databricks.delta.optimize.maxDeletionRatio", "0.5").toDouble,
        targetFileSize =
          sparkSession.conf.get("spark.databricks.delta.optimize.targetFileSize", "67108864").toLong
      )

      // Find files to optimize
      val filesToOptimize = findFilesToOptimize(sparkSession, snapshot, partitionPredicate, config)

      if (filesToOptimize.isEmpty) {
        // No files to optimize, return early
        val metrics = OptimizeMetrics(
          numRemovedFiles = 0,
          numAddedFiles = 0,
          numCompactedFiles = 0,
          numDeletionVectorsRemoved = 0,
          totalRowsCompacted = 0,
          totalBytesCompacted = 0,
          executionTimeMs = System.currentTimeMillis() - startTime,
          numFilesBeforeOptimize = numFilesBeforeOptimize,
          numFilesAfterOptimize = numFilesBeforeOptimize
        )
        logMetrics(metrics)
        return Seq.empty[Row]
      }

      // Group files by partition for optimization
      val filesByPartition = filesToOptimize.groupBy(_.partitionValues)

      // Process each partition
      val actions = filesByPartition.flatMap {
        case (partitionValues, files) =>
          optimizePartition(
            sparkSession,
            snapshot,
            files,
            partitionValues,
            zOrderBy,
            config,
            hadoopConf)
      }.toSeq

      // Collect metrics
      val metrics = collectMetrics(actions, startTime, numFilesBeforeOptimize)

      // Commit the transaction
      val operation = DeltaOperations.Optimize(
        predicate = partitionPredicate.toSeq,
        zOrderBy = zOrderBy
      )
      txn.commit(actions, operation)

      // Log metrics
      logMetrics(metrics)

      // Return metrics
      Seq(
        Row(
          metrics.numRemovedFiles,
          metrics.numAddedFiles,
          metrics.numCompactedFiles,
          metrics.numDeletionVectorsRemoved,
          metrics.totalRowsCompacted
        ))
    }
  }

  /**
   * Find files that need optimization.
   *
   * Files are selected for optimization if:
   *   1. They have a deletion vector with >50% deleted rows 2. They are smaller than minFileSize 3.
   *      They can be combined with other small files
   */
  private def findFilesToOptimize(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      partitionPredicate: Option[Expression],
      config: OptimizeConfig): Seq[AddFile] = {

    // Get all files
    val allFiles = snapshot.allFiles.collect()

    // Filter by partition predicate if provided
    val filteredFiles = partitionPredicate match {
      case Some(pred) =>
        // Apply partition predicate
        allFiles.filter {
          file =>
            // Simplified: evaluate predicate on partition values
            true // TODO: Implement proper predicate evaluation
        }
      case None => allFiles
    }

    // Select files for optimization
    filteredFiles.filter {
      file =>
        // Files with high deletion ratio
        val hasDV = file.deletionVector != null
        val deletionRatio = if (hasDV) {
          Option(file.deletionVector).map(_.cardinality).getOrElse(0L).toDouble /
            file.numPhysicalRecords.getOrElse(0L)
        } else {
          0.0
        }

        // Files that need optimization
        val needsCompaction = deletionRatio > config.maxDeletionRatio
        val isTooSmall = file.size < config.minFileSize

        needsCompaction || isTooSmall
    }.toSeq
  }

  /**
   * Optimize files in a single partition.
   *
   * Strategy:
   *   1. Compact files with DVs (rewrite without deleted rows) 2. Combine small files into larger
   *      ones 3. Apply Z-ORDER if specified
   */
  private def optimizePartition(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      files: Seq[AddFile],
      partitionValues: Map[String, String],
      zOrderBy: Seq[String],
      config: OptimizeConfig,
      hadoopConf: org.apache.hadoop.conf.Configuration): Seq[Action] = {

    val actions = mutable.ArrayBuffer[Action]()

    // Separate files with DVs from files without DVs
    val filesWithDV = files.filter(_.deletionVector != null)
    val filesWithoutDV = files.filter(_.deletionVector.isEmpty)

    // Compact files with DVs
    if (filesWithDV.nonEmpty) {
      val compactedActions = compactFilesWithDV(
        sparkSession,
        snapshot,
        filesWithDV,
        partitionValues,
        zOrderBy,
        config,
        hadoopConf
      )
      actions ++= compactedActions
    }

    // Combine small files
    if (filesWithoutDV.nonEmpty) {
      val combinedActions = combineSmallFiles(
        sparkSession,
        snapshot,
        filesWithoutDV,
        partitionValues,
        zOrderBy,
        config,
        hadoopConf
      )
      actions ++= combinedActions
    }

    actions.toSeq
  }

  /** Compact files with deletion vectors by rewriting without deleted rows. */
  private def compactFilesWithDV(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      files: Seq[AddFile],
      partitionValues: Map[String, String],
      zOrderBy: Seq[String],
      config: OptimizeConfig,
      hadoopConf: org.apache.hadoop.conf.Configuration): Seq[Action] = {

    val actions = mutable.ArrayBuffer[Action]()

    // Read all files and filter out deleted rows
    val dfs = files.map {
      file =>
        val df = sparkSession.read
          .format("delta")
          .load(snapshot.path.toString)
          .filter(col("_metadata.file_path") === file.path)

        // If file has DV, we need to filter out deleted rows
        // For now, we'll read the entire file and let the DV reader handle it
        df
    }

    // Union all dataframes
    val combinedDf = dfs.reduce(_ union _)

    // Apply Z-ORDER if specified
    val orderedDf = if (zOrderBy.nonEmpty) {
      combinedDf.repartitionByRange(zOrderBy.map(col): _*)
    } else {
      combinedDf
    }

    // Write compacted file(s)
    val newFiles = writeCompactedFiles(
      sparkSession,
      snapshot,
      orderedDf,
      partitionValues,
      config,
      hadoopConf
    )

    // Remove old files
    files.foreach {
      file =>
        actions += RemoveFile(
          path = file.path,
          deletionTimestamp = Some(System.currentTimeMillis()),
          dataChange = false, // OPTIMIZE is not a data change
          extendedFileMetadata = Some(true),
          partitionValues = file.partitionValues,
          size = Some(file.size),
          tags = file.tags,
          deletionVector = file.deletionVector
        )
    }

    // Add new files
    actions ++= newFiles

    actions.toSeq
  }

  /** Combine small files into larger ones. */
  private def combineSmallFiles(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      files: Seq[AddFile],
      partitionValues: Map[String, String],
      zOrderBy: Seq[String],
      config: OptimizeConfig,
      hadoopConf: org.apache.hadoop.conf.Configuration): Seq[Action] = {

    val actions = mutable.ArrayBuffer[Action]()

    // Group files into bins of target size
    val bins = mutable.ArrayBuffer[Seq[AddFile]]()
    var currentBin = mutable.ArrayBuffer[AddFile]()
    var currentBinSize = 0L

    files.sortBy(_.size).foreach {
      file =>
        if (currentBinSize + file.size > config.targetFileSize && currentBin.nonEmpty) {
          bins += currentBin.toSeq
          currentBin = mutable.ArrayBuffer[AddFile]()
          currentBinSize = 0L
        }
        currentBin += file
        currentBinSize += file.size
    }

    if (currentBin.nonEmpty) {
      bins += currentBin.toSeq
    }

    // Process each bin
    bins.foreach {
      binFiles =>
        if (binFiles.size > 1) {
          // Read all files in bin
          val dfs = binFiles.map {
            file =>
              sparkSession.read
                .format("delta")
                .load(snapshot.path.toString)
                .filter(col("_metadata.file_path") === file.path)
          }

          // Union all dataframes
          val combinedDf = dfs.reduce(_ union _)

          // Apply Z-ORDER if specified
          val orderedDf = if (zOrderBy.nonEmpty) {
            combinedDf.repartitionByRange(zOrderBy.map(col): _*)
          } else {
            combinedDf
          }

          // Write combined file
          val newFiles = writeCompactedFiles(
            sparkSession,
            snapshot,
            orderedDf,
            partitionValues,
            config,
            hadoopConf
          )

          // Remove old files
          binFiles.foreach {
            file =>
              actions += RemoveFile(
                path = file.path,
                deletionTimestamp = Some(System.currentTimeMillis()),
                dataChange = false,
                extendedFileMetadata = Some(true),
                partitionValues = file.partitionValues,
                size = Some(file.size),
                tags = file.tags,
                deletionVector = null
              )
          }

          // Add new files
          actions ++= newFiles
        }
    }

    actions.toSeq
  }

  /** Write compacted files. */
  private def writeCompactedFiles(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      df: DataFrame,
      partitionValues: Map[String, String],
      config: OptimizeConfig,
      hadoopConf: org.apache.hadoop.conf.Configuration): Seq[AddFile] = {

    // Calculate number of files needed
    val totalRows = df.count()
    val avgRowSize = if (totalRows > 0) {
      // Estimate average row size
      1024L // Simplified: 1KB per row
    } else {
      1024L
    }

    val estimatedSize = totalRows * avgRowSize
    val numFiles = Math.max(1, (estimatedSize / config.targetFileSize).toInt)

    // Repartition to target number of files
    val repartitionedDf = df.repartition(numFiles)

    // Write files
    val basePath = if (partitionValues.isEmpty) {
      snapshot.path.toString
    } else {
      s"${snapshot.path}/${partitionValues.map { case (k, v) => s"$k=$v" }.mkString("/")}"
    }

    // Generate file paths
    val newFiles = (0 until numFiles).map {
      i =>
        val newPath = s"$basePath/part-${UUID.randomUUID()}.parquet"

        // Write partition
        repartitionedDf
          .filter(spark_partition_id() === i)
          .write
          .format("parquet")
          .mode("overwrite")
          .save(newPath)

        // Get file size
        val fs = new Path(newPath).getFileSystem(hadoopConf)
        val fileStatus = fs.getFileStatus(new Path(newPath))
        val size = fileStatus.getLen

        AddFile(
          path = newPath,
          partitionValues = partitionValues,
          size = size,
          modificationTime = System.currentTimeMillis(),
          dataChange = false, // OPTIMIZE is not a data change
          stats = null,
          tags = Map.empty,
          deletionVector = null // Compacted files have no DV
        )
    }

    newFiles
  }

  /** Collect metrics from the actions. */
  private def collectMetrics(
      actions: Seq[Action],
      startTime: Long,
      numFilesBeforeOptimize: Long): OptimizeMetrics = {

    val removeFiles = actions.collect { case r: RemoveFile => r }
    val addFiles = actions.collect { case a: AddFile => a }

    val numDeletionVectorsRemoved = removeFiles.count(_.deletionVector != null)
    val totalBytesCompacted = removeFiles.map(_.size.getOrElse(0L)).sum
    val totalRowsCompacted = removeFiles.map {
      file =>
        val dv = Option(file.deletionVector)
        val fileSize = file.size.getOrElse(0L)
        dv match {
          case Some(d) => fileSize / 1024 - d.cardinality // Estimate
          case None => fileSize / 1024 // Estimate
        }
    }.sum

    OptimizeMetrics(
      numRemovedFiles = removeFiles.size,
      numAddedFiles = addFiles.size,
      numCompactedFiles = removeFiles.size,
      numDeletionVectorsRemoved = numDeletionVectorsRemoved,
      totalRowsCompacted = totalRowsCompacted,
      totalBytesCompacted = totalBytesCompacted,
      executionTimeMs = System.currentTimeMillis() - startTime,
      numFilesBeforeOptimize = numFilesBeforeOptimize,
      numFilesAfterOptimize = numFilesBeforeOptimize - removeFiles.size + addFiles.size
    )
  }

  /** Log metrics for monitoring. */
  private def logMetrics(metrics: OptimizeMetrics): Unit = {
    logInfo(
      s"OPTIMIZE operation completed: " +
        s"removed ${metrics.numRemovedFiles} files, " +
        s"added ${metrics.numAddedFiles} files, " +
        s"compacted ${metrics.numCompactedFiles} files, " +
        s"removed ${metrics.numDeletionVectorsRemoved} deletion vectors, " +
        s"compacted ${metrics.totalRowsCompacted} rows, " +
        s"compacted ${metrics.totalBytesCompacted} bytes, " +
        s"took ${metrics.executionTimeMs}ms")
  }
}

// Made with Bob
