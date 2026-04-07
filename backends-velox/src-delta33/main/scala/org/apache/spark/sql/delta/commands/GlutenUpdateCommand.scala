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

/** Metrics for UPDATE operations with deletion vectors. */
case class UpdateMetrics(
    numRemovedFiles: Long,
    numAddedFiles: Long,
    numUpdatedRows: Long,
    numDeletionVectors: Long,
    numRewrittenFiles: Long,
    executionTimeMs: Long,
    numFilesBeforeSkipping: Long,
    numFilesAfterSkipping: Long
)

/**
 * Gluten-optimized UPDATE command with Deletion Vector support.
 *
 * This command extends Delta's UPDATE operation to use deletion vectors for small updates, avoiding
 * expensive file rewrites.
 *
 * UPDATE works in two phases:
 *   1. Mark old rows as deleted using DVs 2. Write new rows with updated values
 */
case class GlutenUpdateCommand(
    deltaLog: DeltaLog,
    target: LogicalPlan,
    updateExpressions: Seq[(String, Expression)],
    condition: Option[Expression])
  extends RunnableCommand
  with DeltaCommand {

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this

  override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(deltaLog, "delta.dml.update") {
      val startTime = System.currentTimeMillis()
      val txn = deltaLog.startTransaction()

      // Get current snapshot
      val snapshot = txn.snapshot
      val numFilesBeforeSkipping = snapshot.numOfFiles

      // Get Hadoop configuration
      val hadoopConf = sparkSession.sessionState.newHadoopConf()

      // Create DML executor
      val executor = GlutenDeltaDMLExecutor
      val config = GlutenDeltaDMLExecutor.DVConfig(
        enabled =
          sparkSession.conf.get("spark.databricks.delta.deletionVectors.enabled", "true").toBoolean
      )

      // Find files that match the update condition
      val filesToModify = findFilesToUpdate(sparkSession, snapshot, condition)
      val numFilesAfterSkipping = filesToModify.size

      if (filesToModify.isEmpty) {
        // No files to modify, return early
        val metrics = UpdateMetrics(
          numRemovedFiles = 0,
          numAddedFiles = 0,
          numUpdatedRows = 0,
          numDeletionVectors = 0,
          numRewrittenFiles = 0,
          executionTimeMs = System.currentTimeMillis() - startTime,
          numFilesBeforeSkipping = numFilesBeforeSkipping,
          numFilesAfterSkipping = 0
        )
        logMetrics(metrics)
        return Seq.empty[Row]
      }

      // Process each file
      val actions = filesToModify.flatMap {
        addFile =>
          processFile(
            sparkSession,
            snapshot,
            addFile,
            updateExpressions,
            condition,
            executor,
            config,
            hadoopConf)
      }

      // Collect metrics
      val metrics = collectMetrics(actions, startTime)

      // Commit the transaction
      val operation = DeltaOperations.Update(condition)
      txn.commit(actions, operation)

      // Log metrics
      logMetrics(metrics)

      // Return metrics
      Seq(
        Row(
          metrics.numRemovedFiles,
          metrics.numUpdatedRows,
          metrics.numDeletionVectors,
          metrics.numRewrittenFiles
        ))
    }
  }

  /** Find files that contain rows matching the update condition. */
  private def findFilesToUpdate(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      condition: Option[Expression]): Seq[AddFile] = {

    // Read the table with the condition
    val df = sparkSession.read.format("delta").load(snapshot.path.toString)
    val filteredDf = condition match {
      case Some(expr) => df.filter(Column(expr))
      case None => df
    }

    // Get the files that contain matching rows
    val fileIndex = snapshot.filesForScan(Seq.empty[Expression], keepNumRecords = false)
    val matchingFiles = fileIndex.files

    matchingFiles.toSeq
  }

  /** Find rows to update in a specific file and return their indices. */
  private def findUpdatedRowsInFile(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      file: AddFile,
      condition: Option[Expression]): Set[Long] = {

    // Read the specific file directly by path (workaround for _metadata.file_path issue)
    // TODO: Once Gluten/Velox supports _metadata.file_path, revert to Delta scan with filter
    val absolutePath = if (file.path.startsWith("/") || file.path.contains("://")) {
      file.path // Already absolute
    } else {
      new Path(snapshot.path, file.path).toString // Make relative path absolute
    }
    val df = sparkSession.read
      .format("parquet")
      .load(absolutePath)

    // Add row indices and filter by condition
    val dfWithRowIndex = df
      .withColumn("__row_index__", monotonically_increasing_id())
      .filter(condition.map(Column(_)).getOrElse(lit(true)))

    // Collect the row indices
    dfWithRowIndex
      .select("__row_index__")
      .collect()
      .map(_.getLong(0))
      .toSet
  }

  /** Write updated rows to a new file. */
  private def writeUpdatedRows(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      file: AddFile,
      updatedRowIndices: Set[Long],
      updateExpressions: Seq[(String, Expression)],
      condition: Option[Expression],
      hadoopConf: org.apache.hadoop.conf.Configuration): AddFile = {

    // Read the specific file directly by path (workaround for _metadata.file_path issue)
    // TODO: Once Gluten/Velox supports _metadata.file_path, revert to Delta scan with filter
    val absolutePath = if (file.path.startsWith("/") || file.path.contains("://")) {
      file.path // Already absolute
    } else {
      new Path(snapshot.path, file.path).toString // Make relative path absolute
    }
    val df = sparkSession.read
      .format("parquet")
      .load(absolutePath)

    // Add row indices
    val dfWithRowIndex = df
      .withColumn("__row_index__", monotonically_increasing_id())

    // Filter to only updated rows
    val updatedDf = dfWithRowIndex
      .filter(col("__row_index__").isin(updatedRowIndices.toSeq: _*))
      .drop("__row_index__")

    // Apply update expressions
    val finalDf = updateExpressions.foldLeft(updatedDf) {
      case (df, (colName, expr)) =>
        df.withColumn(colName, Column(expr))
    }

    // Generate new file path with UUID
    val newPath =
      s"${file.path.split("/").dropRight(1).mkString("/")}/part-${UUID.randomUUID()}.parquet"

    // Write the updated rows
    finalDf.write
      .format("parquet")
      .mode("overwrite")
      .save(newPath)

    // Get file size
    val fs = new Path(newPath).getFileSystem(hadoopConf)
    val fileStatus = fs.getFileStatus(new Path(newPath))
    val size = fileStatus.getLen

    // Create AddFile action
    AddFile(
      path = newPath,
      partitionValues = file.partitionValues,
      size = size,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = null, // Stats will be computed later
      tags = file.tags,
      deletionVector = null // New file has no DV
    )
  }

  /** Rewrite file without updated rows (for threshold exceeded case). */
  private def rewriteFileWithoutUpdatedRows(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      file: AddFile,
      updatedRowIndices: Set[Long],
      hadoopConf: org.apache.hadoop.conf.Configuration): AddFile = {

    // Read the specific file directly by path (workaround for _metadata.file_path issue)
    // TODO: Once Gluten/Velox supports _metadata.file_path, revert to Delta scan with filter
    val absolutePath = if (file.path.startsWith("/") || file.path.contains("://")) {
      file.path // Already absolute
    } else {
      new Path(snapshot.path, file.path).toString // Make relative path absolute
    }
    val df = sparkSession.read
      .format("parquet")
      .load(absolutePath)

    // Add row indices and filter out updated rows
    val dfWithRowIndex = df
      .withColumn("__row_index__", monotonically_increasing_id())

    val remainingDf = dfWithRowIndex
      .filter(!col("__row_index__").isin(updatedRowIndices.toSeq: _*))
      .drop("__row_index__")

    // Generate new file path with UUID
    val newPath =
      s"${file.path.split("/").dropRight(1).mkString("/")}/part-${UUID.randomUUID()}.parquet"

    // Write the remaining rows
    remainingDf.write
      .format("parquet")
      .mode("overwrite")
      .save(newPath)

    // Get file size
    val fs = new Path(newPath).getFileSystem(hadoopConf)
    val fileStatus = fs.getFileStatus(new Path(newPath))
    val size = fileStatus.getLen

    // Create AddFile action
    AddFile(
      path = newPath,
      partitionValues = file.partitionValues,
      size = size,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = null, // Stats will be computed later
      tags = file.tags,
      deletionVector = null // New file has no DV
    )
  }

  /** Process a single file: decide whether to use DV or rewrite. */
  private def processFile(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      file: AddFile,
      updateExpressions: Seq[(String, Expression)],
      condition: Option[Expression],
      executor: GlutenDeltaDMLExecutor.type,
      config: GlutenDeltaDMLExecutor.DVConfig,
      hadoopConf: org.apache.hadoop.conf.Configuration): Seq[Action] = {

    // Find rows to update
    val updatedRows = findUpdatedRowsInFile(sparkSession, snapshot, file, condition)

    if (updatedRows.isEmpty) {
      return Seq.empty
    }

    // Decide whether to use DV or rewrite
    val shouldUseDV = executor.shouldUseDeletionVector(
      file = file,
      numDeletedRows = updatedRows.size,
      config = config
    )

    if (shouldUseDV) {
      // Use deletion vector approach
      // 1. Mark old rows as deleted with DV
      val dvResult = executor.createOrMergeDeletionVector(
        file = file,
        deletedRowIndices = updatedRows,
        tablePath = snapshot.path.toString,
        hadoopConf = hadoopConf
      )

      // 2. Write updated rows to new file
      val updatedFile = writeUpdatedRows(
        sparkSession,
        snapshot,
        file,
        updatedRows,
        updateExpressions,
        condition,
        hadoopConf
      )

      // Return actions: remove old, add with DV, add updated rows
      Seq(
        RemoveFile(
          path = file.path,
          deletionTimestamp = Some(System.currentTimeMillis()),
          dataChange = true,
          extendedFileMetadata = Some(true),
          partitionValues = file.partitionValues,
          size = Some(file.size),
          tags = file.tags,
          deletionVector = file.deletionVector
        ),
        AddFile(
          path = file.path,
          partitionValues = file.partitionValues,
          size = file.size,
          modificationTime = file.modificationTime,
          dataChange = true,
          stats = file.stats,
          tags = file.tags,
          deletionVector = dvResult._2.deletionVector
        ),
        updatedFile
      )
    } else {
      // Rewrite approach: write remaining rows + updated rows
      // 1. Write file without updated rows
      val remainingFile = rewriteFileWithoutUpdatedRows(
        sparkSession,
        snapshot,
        file,
        updatedRows,
        hadoopConf
      )

      // 2. Write updated rows to new file
      val updatedFile = writeUpdatedRows(
        sparkSession,
        snapshot,
        file,
        updatedRows,
        updateExpressions,
        condition,
        hadoopConf
      )

      // Return actions: remove old, add remaining, add updated
      Seq(
        RemoveFile(
          path = file.path,
          deletionTimestamp = Some(System.currentTimeMillis()),
          dataChange = true,
          extendedFileMetadata = Some(true),
          partitionValues = file.partitionValues,
          size = Some(file.size),
          tags = file.tags,
          deletionVector = file.deletionVector
        ),
        remainingFile,
        updatedFile
      )
    }
  }

  /** Collect metrics from the actions. */
  private def collectMetrics(actions: Seq[Action], startTime: Long): UpdateMetrics = {
    val removeFiles = actions.collect { case r: RemoveFile => r }
    val addFiles = actions.collect { case a: AddFile => a }

    // Count updated rows from files with DVs
    val numUpdatedRows = addFiles.map {
      file => Option(file.deletionVector).map(_.cardinality).getOrElse(0L)
    }.sum

    UpdateMetrics(
      numRemovedFiles = removeFiles.size,
      numAddedFiles = addFiles.size,
      numUpdatedRows = numUpdatedRows,
      numDeletionVectors = addFiles.count(_.deletionVector != null),
      numRewrittenFiles = addFiles.count(_.deletionVector == null),
      executionTimeMs = System.currentTimeMillis() - startTime,
      numFilesBeforeSkipping = 0, // Set by caller
      numFilesAfterSkipping = removeFiles.size
    )
  }

  /** Log metrics for monitoring. */
  private def logMetrics(metrics: UpdateMetrics): Unit = {
    logInfo(
      s"UPDATE operation completed: " +
        s"removed ${metrics.numRemovedFiles} files, " +
        s"added ${metrics.numAddedFiles} files, " +
        s"updated ${metrics.numUpdatedRows} rows, " +
        s"created ${metrics.numDeletionVectors} deletion vectors, " +
        s"rewrote ${metrics.numRewrittenFiles} files, " +
        s"took ${metrics.executionTimeMs}ms")
  }
}

// Made with Bob
