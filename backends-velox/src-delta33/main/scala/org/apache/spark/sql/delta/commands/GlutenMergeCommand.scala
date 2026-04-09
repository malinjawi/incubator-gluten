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

/** Metrics for MERGE operations with deletion vectors. */
case class MergeMetrics(
    numRemovedFiles: Long,
    numAddedFiles: Long,
    numUpdatedRows: Long,
    numDeletedRows: Long,
    numInsertedRows: Long,
    numDeletionVectors: Long,
    numRewrittenFiles: Long,
    executionTimeMs: Long,
    numFilesBeforeSkipping: Long,
    numFilesAfterSkipping: Long
)

/** Represents a MERGE clause action. */
sealed trait MergeClause
case class WhenMatched(condition: Option[Expression], action: MergeAction) extends MergeClause
case class WhenNotMatched(condition: Option[Expression], action: MergeAction) extends MergeClause

sealed trait MergeAction
case class UpdateAction(updateExpressions: Seq[(String, Expression)]) extends MergeAction
case object DeleteAction extends MergeAction
case class InsertAction(insertExpressions: Seq[(String, Expression)]) extends MergeAction

/**
 * Gluten-optimized MERGE command with Deletion Vector support.
 *
 * This command extends Delta's MERGE operation to use deletion vectors for matched deletes and
 * updates, avoiding expensive file rewrites.
 *
 * MERGE handles three scenarios:
 *   1. WHEN MATCHED ... UPDATE: Mark old rows as deleted (DV), write updated rows 2. WHEN MATCHED
 *      ... DELETE: Mark rows as deleted (DV) 3. WHEN NOT MATCHED ... INSERT: Write new rows
 */
case class GlutenMergeCommand(
    deltaLog: DeltaLog,
    target: LogicalPlan,
    source: LogicalPlan,
    mergeCondition: Expression,
    matchedClauses: Seq[WhenMatched],
    notMatchedClauses: Seq[WhenNotMatched])
  extends RunnableCommand
  with DeltaCommand {

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this

  override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(deltaLog, "delta.dml.merge") {
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

      // Read target and source
      val targetDf = sparkSession.read.format("delta").load(snapshot.path.toString)
      val sourceDf = Dataset.ofRows(sparkSession, source)

      // Perform join to identify matched and not matched rows
      val joinedDf = targetDf
        .as("target")
        .join(sourceDf.as("source"), Column(mergeCondition), "full_outer")
        .withColumn(
          "__merge_type__",
          when(col("target.*").isNull, lit("NOT_MATCHED"))
            .when(col("source.*").isNull, lit("TARGET_ONLY"))
            .otherwise(lit("MATCHED")))

      // Process matched clauses (updates and deletes)
      val matchedActions = processMatchedClauses(
        sparkSession,
        snapshot,
        joinedDf,
        matchedClauses,
        executor,
        config,
        hadoopConf
      )

      // Process not matched clauses (inserts)
      val notMatchedActions = processNotMatchedClauses(
        sparkSession,
        snapshot,
        joinedDf,
        notMatchedClauses,
        hadoopConf
      )

      val numFilesAfterSkipping = matchedActions.size + notMatchedActions.size

      // Combine all actions
      val actions = matchedActions ++ notMatchedActions

      if (actions.isEmpty) {
        // No changes, return early
        val metrics = MergeMetrics(
          numRemovedFiles = 0,
          numAddedFiles = 0,
          numUpdatedRows = 0,
          numDeletedRows = 0,
          numInsertedRows = 0,
          numDeletionVectors = 0,
          numRewrittenFiles = 0,
          executionTimeMs = System.currentTimeMillis() - startTime,
          numFilesBeforeSkipping = numFilesBeforeSkipping,
          numFilesAfterSkipping = 0
        )
        logMetrics(metrics)
        return Seq.empty[Row]
      }

      // Collect metrics
      val metrics = collectMetrics(actions, startTime)

      // Commit the transaction
      val operation = DeltaOperations.Merge(
        predicate = Some(mergeCondition),
        matchedPredicates = matchedClauses.map(
          c => DeltaOperations.MergePredicate(c.condition.map(_.sql), "matched")),
        notMatchedPredicates = notMatchedClauses.map(
          c => DeltaOperations.MergePredicate(c.condition.map(_.sql), "notMatched")),
        notMatchedBySourcePredicates = Seq.empty
      )
      txn.commit(actions, operation)

      // Log metrics
      logMetrics(metrics)

      // Return metrics
      Seq(
        Row(
          metrics.numRemovedFiles,
          metrics.numUpdatedRows,
          metrics.numDeletedRows,
          metrics.numInsertedRows,
          metrics.numDeletionVectors
        ))
    }
  }

  /** Process WHEN MATCHED clauses (UPDATE and DELETE). */
  private def processMatchedClauses(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      joinedDf: DataFrame,
      clauses: Seq[WhenMatched],
      executor: GlutenDeltaDMLExecutor.type,
      config: GlutenDeltaDMLExecutor.DVConfig,
      hadoopConf: org.apache.hadoop.conf.Configuration): Seq[Action] = {

    // Filter to matched rows
    val matchedDf = joinedDf.filter(col("__merge_type__") === "MATCHED")

    if (matchedDf.isEmpty) {
      return Seq.empty
    }

    // Find files that contain matched rows
    val filesToModify = findFilesWithMatchedRows(sparkSession, snapshot, matchedDf)

    // Process each file
    filesToModify.flatMap {
      file =>
        processMatchedFile(
          sparkSession,
          snapshot,
          file,
          matchedDf,
          clauses,
          executor,
          config,
          hadoopConf
        )
    }
  }

  /** Process a single file for matched clauses. */
  private def processMatchedFile(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      file: AddFile,
      matchedDf: DataFrame,
      clauses: Seq[WhenMatched],
      executor: GlutenDeltaDMLExecutor.type,
      config: GlutenDeltaDMLExecutor.DVConfig,
      hadoopConf: org.apache.hadoop.conf.Configuration): Seq[Action] = {

    // Read the specific file
    val fileDf = sparkSession.read
      .format("delta")
      .load(snapshot.path.toString)
      .filter(col("_metadata.file_path") === file.path)
      .withColumn("__row_index__", monotonically_increasing_id())

    // Join with matched rows to find which rows in this file are matched
    val fileMatchedDf = fileDf.join(matchedDf, fileDf.columns.toSeq, "inner")

    if (fileMatchedDf.isEmpty) {
      return Seq.empty
    }

    // Separate rows by action type
    var rowsToUpdate = Set.empty[Long]
    var rowsToDelete = Set.empty[Long]
    var updateExpressions = Seq.empty[(String, Expression)]

    clauses.foreach {
      case WhenMatched(condition, UpdateAction(exprs)) =>
        val filtered = condition match {
          case Some(cond) => fileMatchedDf.filter(Column(cond))
          case None => fileMatchedDf
        }
        val indices = filtered.select("__row_index__").collect().map(_.getLong(0)).toSet
        rowsToUpdate ++= indices
        updateExpressions = exprs

      case WhenMatched(condition, DeleteAction) =>
        val filtered = condition match {
          case Some(cond) => fileMatchedDf.filter(Column(cond))
          case None => fileMatchedDf
        }
        val indices = filtered.select("__row_index__").collect().map(_.getLong(0)).toSet
        rowsToDelete ++= indices

      case _ => // Ignore other cases
    }

    val allModifiedRows = rowsToUpdate ++ rowsToDelete

    if (allModifiedRows.isEmpty) {
      return Seq.empty
    }

    // Decide whether to use DV or rewrite
    val shouldUseDV = executor.shouldUseDeletionVector(
      file = file,
      numDeletedRows = allModifiedRows.size,
      config = config
    )

    if (shouldUseDV) {
      // Use deletion vector approach
      val dvResult = executor.createOrMergeDeletionVector(
        file = file,
        deletedRowIndices = allModifiedRows,
        tablePath = snapshot.path.toString,
        hadoopConf = hadoopConf
      )

      val actions = scala.collection.mutable.ArrayBuffer[Action]()

      // Remove old file
      actions += RemoveFile(
        path = file.path,
        deletionTimestamp = Some(System.currentTimeMillis()),
        dataChange = true,
        extendedFileMetadata = Some(true),
        partitionValues = file.partitionValues,
        size = Some(file.size),
        tags = file.tags,
        deletionVector = file.deletionVector
      )

      // Add file with DV
      actions += AddFile(
        path = file.path,
        partitionValues = file.partitionValues,
        size = file.size,
        modificationTime = file.modificationTime,
        dataChange = true,
        stats = file.stats,
        tags = file.tags,
        deletionVector = dvResult._2.deletionVector
      )

      // Write updated rows if any
      if (rowsToUpdate.nonEmpty) {
        val updatedFile = writeUpdatedRows(
          sparkSession,
          snapshot,
          file,
          rowsToUpdate,
          updateExpressions,
          hadoopConf
        )
        actions += updatedFile
      }

      actions.toSeq
    } else {
      // Rewrite approach
      val actions = scala.collection.mutable.ArrayBuffer[Action]()

      // Remove old file
      actions += RemoveFile(
        path = file.path,
        deletionTimestamp = Some(System.currentTimeMillis()),
        dataChange = true,
        extendedFileMetadata = Some(true),
        partitionValues = file.partitionValues,
        size = Some(file.size),
        tags = file.tags,
        deletionVector = file.deletionVector
      )

      // Write remaining rows (not modified)
      val remainingFile = writeRemainingRows(
        sparkSession,
        snapshot,
        file,
        allModifiedRows,
        hadoopConf
      )
      actions += remainingFile

      // Write updated rows if any
      if (rowsToUpdate.nonEmpty) {
        val updatedFile = writeUpdatedRows(
          sparkSession,
          snapshot,
          file,
          rowsToUpdate,
          updateExpressions,
          hadoopConf
        )
        actions += updatedFile
      }

      actions.toSeq
    }
  }

  /** Process WHEN NOT MATCHED clauses (INSERT). */
  private def processNotMatchedClauses(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      joinedDf: DataFrame,
      clauses: Seq[WhenNotMatched],
      hadoopConf: org.apache.hadoop.conf.Configuration): Seq[Action] = {

    if (clauses.isEmpty) {
      return Seq.empty
    }

    // Filter to not matched rows
    val notMatchedDf = joinedDf.filter(col("__merge_type__") === "NOT_MATCHED")

    if (notMatchedDf.isEmpty) {
      return Seq.empty
    }

    val actions = scala.collection.mutable.ArrayBuffer[Action]()

    clauses.foreach {
      case WhenNotMatched(condition, InsertAction(exprs)) =>
        val filtered = condition match {
          case Some(cond) => notMatchedDf.filter(Column(cond))
          case None => notMatchedDf
        }

        if (!filtered.isEmpty) {
          // Apply insert expressions
          val insertDf = exprs.foldLeft(filtered) {
            case (df, (colName, expr)) =>
              df.withColumn(colName, Column(expr))
          }

          // Write inserted rows
          val insertedFile = writeInsertedRows(
            sparkSession,
            snapshot,
            insertDf,
            hadoopConf
          )
          actions += insertedFile
        }

      case _ => // Ignore other cases
    }

    actions.toSeq
  }

  /** Find files that contain matched rows. */
  private def findFilesWithMatchedRows(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      matchedDf: DataFrame): Seq[AddFile] = {

    // Get file paths from matched rows
    val filePaths = matchedDf
      .select("target._metadata.file_path")
      .distinct()
      .collect()
      .map(_.getString(0))
      .toSet

    // Filter snapshot files
    snapshot.allFiles.collect().filter(f => filePaths.contains(f.path)).toSeq
  }

  /** Write updated rows to a new file. */
  private def writeUpdatedRows(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      file: AddFile,
      updatedRowIndices: Set[Long],
      updateExpressions: Seq[(String, Expression)],
      hadoopConf: org.apache.hadoop.conf.Configuration): AddFile = {

    // Read the specific file
    val df = sparkSession.read
      .format("delta")
      .load(snapshot.path.toString)
      .filter(col("_metadata.file_path") === file.path)
      .withColumn("__row_index__", monotonically_increasing_id())

    // Filter to updated rows
    val updatedDf = df
      .filter(col("__row_index__").isin(updatedRowIndices.toSeq: _*))
      .drop("__row_index__")

    // Apply update expressions
    val finalDf = updateExpressions.foldLeft(updatedDf) {
      case (df, (colName, expr)) =>
        df.withColumn(colName, Column(expr))
    }

    // Generate new file path
    val newPath =
      s"${file.path.split("/").dropRight(1).mkString("/")}/part-${UUID.randomUUID()}.parquet"

    // Write the file
    finalDf.write.format("parquet").mode("overwrite").save(newPath)

    // Get file size
    val fs = new Path(newPath).getFileSystem(hadoopConf)
    val fileStatus = fs.getFileStatus(new Path(newPath))
    val size = fileStatus.getLen

    AddFile(
      path = newPath,
      partitionValues = file.partitionValues,
      size = size,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = null,
      tags = file.tags,
      deletionVector = null
    )
  }

  /** Write remaining rows (not modified) to a new file. */
  private def writeRemainingRows(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      file: AddFile,
      modifiedRowIndices: Set[Long],
      hadoopConf: org.apache.hadoop.conf.Configuration): AddFile = {

    // Read the specific file
    val df = sparkSession.read
      .format("delta")
      .load(snapshot.path.toString)
      .filter(col("_metadata.file_path") === file.path)
      .withColumn("__row_index__", monotonically_increasing_id())

    // Filter out modified rows
    val remainingDf = df
      .filter(!col("__row_index__").isin(modifiedRowIndices.toSeq: _*))
      .drop("__row_index__")

    // Generate new file path
    val newPath =
      s"${file.path.split("/").dropRight(1).mkString("/")}/part-${UUID.randomUUID()}.parquet"

    // Write the file
    remainingDf.write.format("parquet").mode("overwrite").save(newPath)

    // Get file size
    val fs = new Path(newPath).getFileSystem(hadoopConf)
    val fileStatus = fs.getFileStatus(new Path(newPath))
    val size = fileStatus.getLen

    AddFile(
      path = newPath,
      partitionValues = file.partitionValues,
      size = size,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = null,
      tags = file.tags,
      deletionVector = null
    )
  }

  /** Write inserted rows to a new file. */
  private def writeInsertedRows(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      insertDf: DataFrame,
      hadoopConf: org.apache.hadoop.conf.Configuration): AddFile = {

    // Generate new file path
    val newPath = s"${snapshot.path}/part-${UUID.randomUUID()}.parquet"

    // Write the file
    insertDf.write.format("parquet").mode("overwrite").save(newPath)

    // Get file size
    val fs = new Path(newPath).getFileSystem(hadoopConf)
    val fileStatus = fs.getFileStatus(new Path(newPath))
    val size = fileStatus.getLen

    AddFile(
      path = newPath,
      partitionValues = Map.empty,
      size = size,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = null,
      tags = Map.empty,
      deletionVector = null
    )
  }

  /** Collect metrics from the actions. */
  private def collectMetrics(actions: Seq[Action], startTime: Long): MergeMetrics = {
    val removeFiles = actions.collect { case r: RemoveFile => r }
    val addFiles = actions.collect { case a: AddFile => a }

    // Estimate counts (simplified)
    val numUpdatedRows = addFiles.count(_.deletionVector != null)
    val numDeletedRows =
      addFiles.map(f => Option(f.deletionVector).map(_.cardinality).getOrElse(0L)).sum
    val numInsertedRows = addFiles.count(_.deletionVector == null)

    MergeMetrics(
      numRemovedFiles = removeFiles.size,
      numAddedFiles = addFiles.size,
      numUpdatedRows = numUpdatedRows,
      numDeletedRows = numDeletedRows,
      numInsertedRows = numInsertedRows,
      numDeletionVectors = addFiles.count(_.deletionVector != null),
      numRewrittenFiles = addFiles.count(_.deletionVector == null),
      executionTimeMs = System.currentTimeMillis() - startTime,
      numFilesBeforeSkipping = 0,
      numFilesAfterSkipping = removeFiles.size
    )
  }

  /** Log metrics for monitoring. */
  private def logMetrics(metrics: MergeMetrics): Unit = {
    logInfo(
      s"MERGE operation completed: " +
        s"removed ${metrics.numRemovedFiles} files, " +
        s"added ${metrics.numAddedFiles} files, " +
        s"updated ${metrics.numUpdatedRows} rows, " +
        s"deleted ${metrics.numDeletedRows} rows, " +
        s"inserted ${metrics.numInsertedRows} rows, " +
        s"created ${metrics.numDeletionVectors} deletion vectors, " +
        s"took ${metrics.executionTimeMs}ms")
  }
}

// Made with Bob
