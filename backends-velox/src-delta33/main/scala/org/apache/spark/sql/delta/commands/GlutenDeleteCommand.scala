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

import org.apache.gluten.vectorized.VeloxRowIndexFinderJni

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.{createMetric, createTimingMetric}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.Path

import scala.util.control.NonFatal

/** Metrics for DELETE operations with deletion vectors. */
case class DeleteMetrics(
    numRemovedFiles: Long,
    numAddedFiles: Long,
    numDeletedRows: Long,
    numDeletionVectors: Long,
    numRewrittenFiles: Long,
    executionTimeMs: Long,
    scanTimeMs: Long,
    rewriteTimeMs: Long,
    commitTimeMs: Long,
    numFilesBeforeSkipping: Long,
    numFilesAfterSkipping: Long
)

trait GlutenDeleteCommandMetrics { self: RunnableCommand =>
  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  def createMetrics: Map[String, SQLMetric] = Map[String, SQLMetric](
    "numRemovedFiles" -> createMetric(sc, "number of files removed."),
    "numAddedFiles" -> createMetric(sc, "number of files added."),
    "numDeletedRows" -> createMetric(sc, "number of rows deleted."),
    "numDeletionVectorsAdded" -> createMetric(sc, "number of deletion vectors added."),
    "numDeletionVectorsRemoved" -> createMetric(sc, "number of deletion vectors removed."),
    "numDeletionVectorsUpdated" -> createMetric(sc, "number of deletion vectors updated."),
    "numFilesBeforeSkipping" -> createMetric(sc, "number of files before skipping"),
    "numBytesBeforeSkipping" -> createMetric(sc, "number of bytes before skipping"),
    "numFilesAfterSkipping" -> createMetric(sc, "number of files after skipping"),
    "numBytesAfterSkipping" -> createMetric(sc, "number of bytes after skipping"),
    "numPartitionsAfterSkipping" -> createMetric(sc, "number of partitions after skipping"),
    "numPartitionsAddedTo" -> createMetric(sc, "number of partitions added"),
    "numPartitionsRemovedFrom" -> createMetric(sc, "number of partitions removed"),
    "numCopiedRows" -> createMetric(sc, "number of rows copied"),
    "numAddedBytes" -> createMetric(sc, "number of bytes added"),
    "numRemovedBytes" -> createMetric(sc, "number of bytes removed"),
    "executionTimeMs" -> createTimingMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" -> createTimingMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" -> createTimingMetric(sc, "time taken to rewrite the matched files"),
    "numAddedChangeFiles" -> createMetric(sc, "number of change data capture files generated"),
    "changeFileBytes" -> createMetric(sc, "total size of change data capture files generated"),
    "numTouchedRows" -> createMetric(sc, "number of rows touched")
  )
}

/**
 * Gluten-optimized DELETE command with Deletion Vector support.
 *
 * This command extends Delta's DELETE operation to use deletion vectors for small deletes, avoiding
 * expensive file rewrites.
 */
case class GlutenDeleteCommand(
    deltaLog: DeltaLog,
    target: LogicalPlan,
    condition: Option[Expression])
  extends RunnableCommand
  with DeltaCommand
  with GlutenDeleteCommandMetrics {

  override lazy val metrics: Map[String, SQLMetric] = createMetrics

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this

  override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(deltaLog, "delta.dml.delete") {
      val startTime = System.currentTimeMillis()
      val (txn, snapshot, numFilesBeforeSkipping) =
        GlutenDeltaSessionUtils.withGlutenDisabled(sparkSession) {
          val transaction = deltaLog.startTransaction()
          val currentSnapshot = transaction.snapshot
          (transaction, currentSnapshot, currentSnapshot.numOfFiles)
        }

      // Get Hadoop configuration
      val hadoopConf = sparkSession.sessionState.newHadoopConf()

      // Create DML executor
      val executor = GlutenDeltaDMLExecutor
      val config = GlutenDeltaDMLExecutor.DVConfig(
        enabled = DeletionVectorUtils.deletionVectorsWritable(snapshot),
        minFileSizeForDV =
          sparkSession.conf.get("spark.gluten.delta.deletionVectors.minFileSize", "0").toLong
      )

      // Find files that match the delete condition
      val scanStartTime = System.currentTimeMillis()
      val filesToModify = findFilesToDelete(sparkSession, snapshot, condition)
      val numFilesAfterSkipping = filesToModify.size

      if (filesToModify.isEmpty) {
        // No files to modify, return early
        val metrics = DeleteMetrics(
          numRemovedFiles = 0,
          numAddedFiles = 0,
          numDeletedRows = 0,
          numDeletionVectors = 0,
          numRewrittenFiles = 0,
          executionTimeMs = System.currentTimeMillis() - startTime,
          scanTimeMs = System.currentTimeMillis() - scanStartTime,
          rewriteTimeMs = 0L,
          commitTimeMs = 0L,
          numFilesBeforeSkipping = numFilesBeforeSkipping,
          numFilesAfterSkipping = 0
        )
        logMetrics(metrics)
        return Seq.empty[Row]
      }

      val deletedRowsByFile =
        findDeletedRowsByFile(sparkSession, snapshot, filesToModify, condition)
      val totalDeletedRows = deletedRowsByFile.valuesIterator.map(_.size.toLong).sum
      val scanTimeMs = System.currentTimeMillis() - scanStartTime

      // For each file, find rows to delete
      val actionStartTime = System.currentTimeMillis()
      val actions = filesToModify.flatMap {
        file =>
          val deletedRowIndices = deletedRowsByFile.getOrElse(file, Seq.empty[Long])
          val deletedRowCount = deletedRowIndices.size.toLong

          if (deletedRowCount == 0L) {
            // No rows to delete in this file
            None
          } else if (deletedRowCount == file.numPhysicalRecords.getOrElse(0L)) {
            // All rows deleted - just remove the file
            Some(
              Seq(RemoveFile(
                path = file.path,
                deletionTimestamp = Some(System.currentTimeMillis()),
                dataChange = true,
                extendedFileMetadata = Some(true),
                partitionValues = file.partitionValues,
                size = Some(file.size),
                tags = file.tags
              )))
          } else {
            // Partial delete - decide DV vs. rewrite
            if (executor.shouldUseDeletionVector(file, deletedRowCount, config)) {
              // Use deletion vector
              val (removeFile, addFile) = executor.createOrMergeDeletionVector(
                file,
                deletedRowIndices,
                deltaLog.dataPath.toString,
                hadoopConf)
              Some(Seq(removeFile, addFile))
            } else {
              // Rewrite file without deleted rows
              val newFile = rewriteFileWithoutDeletedRows(sparkSession, file, deletedRowIndices)
              Some(
                Seq(
                  RemoveFile(
                    path = file.path,
                    deletionTimestamp = Some(System.currentTimeMillis()),
                    dataChange = true
                  ),
                  newFile
                ))
            }
          }
      }.flatten
      val rewriteTimeMs = System.currentTimeMillis() - actionStartTime

      val commitStartTime = System.currentTimeMillis()
      val deleteMetrics = collectMetrics(
        actions = actions,
        totalDeletedRows = totalDeletedRows,
        startTime = startTime,
        scanTimeMs = scanTimeMs,
        rewriteTimeMs = rewriteTimeMs,
        commitTimeMs = 0L,
        numFilesBeforeSkipping = numFilesBeforeSkipping,
        numFilesAfterSkipping = numFilesAfterSkipping
      )
      updateSqlMetrics(sparkSession, deleteMetrics, snapshot, actions)
      if (sparkSession.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        txn.registerSQLMetrics(sparkSession, metrics)
      }

      val operation = DeltaOperations.Delete(condition.toSeq)
      GlutenDeltaSessionUtils.withGlutenDisabled(sparkSession) {
        txn.commit(actions, operation)
      }
      val commitTimeMs = System.currentTimeMillis() - commitStartTime
      val finalDeleteMetrics = deleteMetrics.copy(
        commitTimeMs = commitTimeMs,
        executionTimeMs = System.currentTimeMillis() - startTime)
      metrics("executionTimeMs").set(finalDeleteMetrics.executionTimeMs)
      sendDriverMetrics(sparkSession, metrics)

      // Log metrics
      logMetrics(finalDeleteMetrics)

      Seq.empty[Row]
    }
  }

  /** Collect metrics from the actions. */
  private def collectMetrics(
      actions: Seq[Action],
      totalDeletedRows: Long,
      startTime: Long,
      scanTimeMs: Long,
      rewriteTimeMs: Long,
      commitTimeMs: Long,
      numFilesBeforeSkipping: Long,
      numFilesAfterSkipping: Long): DeleteMetrics = {
    val removeFiles = actions.collect { case r: RemoveFile => r }
    val addFiles = actions.collect { case a: AddFile => a }

    DeleteMetrics(
      numRemovedFiles = removeFiles.size,
      numAddedFiles = addFiles.size,
      numDeletedRows = totalDeletedRows,
      numDeletionVectors = addFiles.count(_.deletionVector != null),
      numRewrittenFiles = addFiles.count(_.deletionVector == null),
      executionTimeMs = System.currentTimeMillis() - startTime,
      scanTimeMs = scanTimeMs,
      rewriteTimeMs = rewriteTimeMs,
      commitTimeMs = commitTimeMs,
      numFilesBeforeSkipping = numFilesBeforeSkipping,
      numFilesAfterSkipping = numFilesAfterSkipping
    )
  }

  /** Log metrics for monitoring. */
  private def logMetrics(metrics: DeleteMetrics): Unit = {
    logInfo(
      s"DELETE operation completed: " +
        s"removed ${metrics.numRemovedFiles} files, " +
        s"added ${metrics.numAddedFiles} files, " +
        s"deleted ${metrics.numDeletedRows} rows, " +
        s"created ${metrics.numDeletionVectors} deletion vectors, " +
        s"rewrote ${metrics.numRewrittenFiles} files, " +
        s"scan=${metrics.scanTimeMs}ms, " +
        s"rewrite=${metrics.rewriteTimeMs}ms, " +
        s"commit=${metrics.commitTimeMs}ms, " +
        s"took ${metrics.executionTimeMs}ms")
  }

  /** Find files that contain rows matching the delete condition. */
  private def findFilesToDelete(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      condition: Option[Expression]): Seq[AddFile] = {

    condition match {
      case None =>
        // Delete all rows - return all files
        import scala.collection.JavaConverters._
        GlutenDeltaSessionUtils.withGlutenDisabled(sparkSession) {
          snapshot.allFiles.toLocalIterator().asScala.toSeq
        }

      case Some(expr) =>
        // Use data skipping to find candidate files
        val candidateFiles = GlutenDeltaSessionUtils.withGlutenDisabled(sparkSession) {
          snapshot
            .filesForScan(Seq(expr), keepNumRecords = true)
            .files
        }

        candidateFiles
    }
  }

  /** Find row indices to delete within a specific file. */
  private def findDeletedRowsByFile(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      files: Seq[AddFile],
      condition: Option[Expression]): Map[AddFile, Iterable[Long]] = {
    condition match {
      case None =>
        files.map(file => file -> (0L until file.numPhysicalRecords.getOrElse(0L))).toMap
      case Some(expr) if files.isEmpty =>
        Map.empty
      case Some(expr) =>
        val absolutePaths = files.map(file => file -> absoluteFilePath(file))
        val rowFinderParallelism = buildRowIndexFinderParallelism(sparkSession, absolutePaths.size)
        val rowFinderConfig =
          buildRowIndexFinderConfig(sparkSession, rowFinderParallelism)

        try {
          // Delta snapshot files generally share a compatible physical schema. Use table metadata
          // rather than probing a parquet file through Spark to keep the delete hot path native.
          val sharedSchema = buildNativeRowFinderSchema(snapshot)
          val serializedFilter = VeloxRowIndexFinderJni.serializeFilter(Some(expr), sharedSchema)
          val serializedSchema = VeloxRowIndexFinderJni.serializeSchema(sharedSchema)
          val deletedRowsByPath = VeloxRowIndexFinderJni.findMatchingRowsSerializedBatch(
            filePaths = absolutePaths.map(_._2),
            serializedFilter = serializedFilter,
            serializedSchema = serializedSchema,
            config = rowFinderConfig,
            parallelism = rowFinderParallelism
          )

          absolutePaths.map {
            case (file, path) =>
              file -> scala.collection.mutable.WrappedArray.make(
                deletedRowsByPath.getOrElse(path, Array.empty[Long]))
          }.toMap
        } catch {
          case NonFatal(e) =>
            logWarning(
              s"Metadata-schema native row discovery failed for DELETE; " +
                s"falling back to per-file schema scans. ${e.getMessage}")
            files.map {
              file => file -> findDeletedRowsInFile(sparkSession, file, condition, rowFinderConfig)
            }.toMap
        }
    }
  }

  private def findDeletedRowsInFile(
      sparkSession: SparkSession,
      file: AddFile,
      condition: Option[Expression],
      rowFinderConfig: VeloxRowIndexFinderJni.Config = VeloxRowIndexFinderJni.Config())
      : Iterable[Long] = {
    condition match {
      case None =>
        // Delete all rows
        0L until file.numPhysicalRecords.getOrElse(0L)

      case Some(expr) =>
        val schema = sparkSession.read.parquet(absoluteFilePath(file)).schema
        scala.collection.mutable.WrappedArray.make(
          VeloxRowIndexFinderJni.findMatchingRows(
            filePath = absoluteFilePath(file),
            filter = Some(expr),
            schema = schema,
            config = rowFinderConfig))
    }
  }

  private def buildNativeRowFinderSchema(snapshot: Snapshot): StructType = {
    DeltaColumnMapping.createPhysicalSchema(
      snapshot.metadata.schema,
      snapshot.metadata.schema,
      snapshot.metadata.columnMappingMode)
  }

  private def buildRowIndexFinderConfig(
      sparkSession: SparkSession,
      rowFinderParallelism: Int): VeloxRowIndexFinderJni.Config = {
    VeloxRowIndexFinderJni.Config(
      batchSize = sparkSession.conf
        .getOption("spark.gluten.delta.rowIndexFinder.batchSize")
        .map(_.toInt)
        .getOrElse(32768),
      numThreads = sparkSession.conf
        .getOption("spark.gluten.delta.rowIndexFinder.numThreads")
        .map(_.toInt)
        .getOrElse(1),
      memoryLimitMB =
        sparkSession.conf.get("spark.gluten.delta.rowIndexFinder.memoryLimitMB", "1024").toInt
    )
  }

  private def buildRowIndexFinderParallelism(sparkSession: SparkSession, fileCount: Int): Int = {
    val configured =
      sparkSession.conf
        .getOption("spark.gluten.delta.rowIndexFinder.parallelism")
        .map(_.toInt)
        .getOrElse(math.max(1, sparkSession.sparkContext.defaultParallelism))
    math.min(fileCount, math.max(1, configured))
  }

  /** Rewrite a file without the deleted rows. */
  private def rewriteFileWithoutDeletedRows(
      sparkSession: SparkSession,
      file: AddFile,
      deletedRowIndices: Iterable[Long]): AddFile = {

    val df = readFileWithRowIndex(sparkSession, file)

    // Filter out deleted rows
    val filteredDf = df
      .filter(!col("__row_index__").isin(deletedRowIndices.toSeq: _*))
      .drop("__row_index__")

    // Write a single replacement parquet file and store its logical table path.
    val (newLogicalPath, newAbsolutePath) = buildReplacementPath(file)
    val newSize = writeSingleParquetFile(filteredDf, newAbsolutePath, sparkSession)

    // Calculate new record counts
    val newRecordCount =
      file.numPhysicalRecords.getOrElse(0L) - deletedRowIndices.size

    // Create new AddFile
    file.copy(
      path = newLogicalPath,
      size = newSize,
      modificationTime = System.currentTimeMillis(),
      deletionVector = null
    )
  }

  private def absoluteFilePath(file: AddFile): String = {
    if (file.path.startsWith("/") || file.path.contains("://")) {
      file.path
    } else {
      DeltaFileOperations.absolutePath(deltaLog.dataPath.toString, file.path).toString
    }
  }

  private def updateSqlMetrics(
      sparkSession: SparkSession,
      deleteMetrics: DeleteMetrics,
      snapshot: Snapshot,
      actions: Seq[Action]): Unit = {
    val addFiles = actions.collect { case a: AddFile => a }
    val removeFiles = actions.collect { case r: RemoveFile => r }
    val numDvAdded = addFiles.count(_.deletionVector != null)
    val numDvRemoved = removeFiles.count(_.deletionVector != null)
    val numDvUpdated =
      addFiles.count {
        add =>
          add.deletionVector != null &&
          removeFiles.exists(remove => remove.path == add.path && remove.deletionVector != null)
      }

    metrics("numRemovedFiles").set(deleteMetrics.numRemovedFiles)
    metrics("numAddedFiles").set(deleteMetrics.numAddedFiles)
    metrics("numDeletedRows").set(deleteMetrics.numDeletedRows)
    metrics("numDeletionVectorsAdded").set(numDvAdded)
    metrics("numDeletionVectorsRemoved").set(numDvRemoved)
    metrics("numDeletionVectorsUpdated").set(numDvUpdated)
    metrics("numFilesBeforeSkipping").set(deleteMetrics.numFilesBeforeSkipping)
    metrics("numBytesBeforeSkipping").set(GlutenDeltaSessionUtils.withGlutenDisabled(sparkSession) {
      snapshot.sizeInBytes
    })
    metrics("numFilesAfterSkipping").set(deleteMetrics.numFilesAfterSkipping)
    metrics("numBytesAfterSkipping").set(0L)
    metrics("numPartitionsAfterSkipping").set(0L)
    metrics("numPartitionsAddedTo").set(0L)
    metrics("numPartitionsRemovedFrom").set(0L)
    metrics("numCopiedRows").set(0L)
    metrics("numAddedBytes").set(addFiles.map(_.size).sum)
    metrics("numRemovedBytes").set(removeFiles.flatMap(_.size).sum)
    metrics("executionTimeMs").set(deleteMetrics.executionTimeMs)
    metrics("scanTimeMs").set(deleteMetrics.scanTimeMs)
    metrics("rewriteTimeMs").set(deleteMetrics.rewriteTimeMs)
    metrics("numAddedChangeFiles").set(0L)
    metrics("changeFileBytes").set(0L)
    metrics("numTouchedRows").set(deleteMetrics.numDeletedRows)
  }

  private def buildReplacementPath(file: AddFile): (String, Path) = {
    val newFileName = s"part-${java.util.UUID.randomUUID()}.snappy.parquet"
    val originalPath = new Path(file.path)
    val parent =
      Option(originalPath.getParent).map(_.toString).filter(p => p.nonEmpty && p != ".")

    if (file.path.startsWith("/") || file.path.contains("://")) {
      val absoluteParent =
        Option(new Path(file.path).getParent).getOrElse(deltaLog.dataPath)
      val absolutePath = new Path(absoluteParent, newFileName)
      (absolutePath.toString, absolutePath)
    } else {
      val logicalPath = parent.map(p => new Path(p, newFileName).toString).getOrElse(newFileName)
      val absolutePath = new Path(deltaLog.dataPath, logicalPath)
      (logicalPath, absolutePath)
    }
  }

  private def writeSingleParquetFile(
      df: DataFrame,
      finalPath: Path,
      sparkSession: SparkSession): Long = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val fs = finalPath.getFileSystem(hadoopConf)
    val tempDir =
      new Path(deltaLog.dataPath, s".gluten-delete-rewrite-${java.util.UUID.randomUUID()}")

    df.coalesce(1)
      .write
      .mode("overwrite")
      .parquet(tempDir.toString)

    val partFiles = fs.listStatus(tempDir).filter {
      status =>
        status.isFile &&
        status.getPath.getName.startsWith("part-") &&
        status.getPath.getName.endsWith(".parquet")
    }

    if (partFiles.lengthCompare(1) != 0) {
      throw new IllegalStateException(
        s"Expected exactly one rewritten parquet file under $tempDir but found ${partFiles.length}.")
    }

    if (fs.exists(finalPath) && !fs.delete(finalPath, true)) {
      throw new IllegalStateException(s"Unable to delete existing rewrite target $finalPath.")
    }

    if (!fs.rename(partFiles.head.getPath, finalPath)) {
      throw new IllegalStateException(
        s"Unable to move rewritten parquet file ${partFiles.head.getPath} to $finalPath.")
    }

    fs.delete(tempDir, true)
    fs.getFileStatus(finalPath).getLen
  }

  private def readFileWithRowIndex(sparkSession: SparkSession, file: AddFile): DataFrame = {
    val df = sparkSession.read.parquet(absoluteFilePath(file))
    val rowIndexCol = s"${FileFormat.METADATA_NAME}.${ParquetFileFormat.ROW_INDEX}"
    df.select(df.columns.map(col) :+ col(rowIndexCol).cast("long").as("__row_index__"): _*)
  }

  private def rebindConditionToOutput(
      sparkSession: SparkSession,
      expr: Expression,
      output: Seq[Attribute]): Expression = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    expr.transform {
      case attr: AttributeReference =>
        val matches = output.filter(candidate => resolver(candidate.name, attr.name))
        if (matches.lengthCompare(1) == 0) {
          matches.head
        } else if (matches.isEmpty) {
          throw new IllegalStateException(
            s"Unable to rebind delete predicate attribute `${attr.name}` to file scan output.")
        } else {
          throw new IllegalStateException(
            s"Ambiguous delete predicate attribute `${attr.name}` in file scan output.")
        }
    }
  }
}

// Made with Bob
