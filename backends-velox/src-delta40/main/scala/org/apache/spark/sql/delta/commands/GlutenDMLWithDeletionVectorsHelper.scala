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

import org.apache.gluten.columnarbatch.{ColumnarBatches, IndicatorVector}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaLog, NoMapping, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, FileAction}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.util.{JsonUtils, Utils => DeltaUtils}
import org.apache.spark.sql.delta.util.DeltaFileOperations.absolutePath
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.FileFormat.{FILE_PATH, METADATA_NAME}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetFooterReaderShim}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{Utils => SparkUtils}

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.format.converter.ParquetMetadataConverter

import scala.collection.mutable
import scala.util.control.NonFatal

object GlutenDMLWithDeletionVectorsHelper extends DeltaCommand {
  private val ParquetFieldIdMetadataKey = "parquet.field.id"

  private val driverMergeMaxFilesKey =
    "spark.gluten.sql.delta.delete.dv.driverMergeMaxFiles"
  private val nativeDmlRowIndexScanKey =
    "spark.gluten.sql.delta.enableNativeDmlRowIndexScan"
  private val nativeColumnarBitmapMergeKey =
    "spark.gluten.sql.delta.delete.dv.enableNativeColumnarBitmapMerge"
  private val nativeBitmapAggregationKey =
    "spark.gluten.sql.delta.delete.dv.enableNativeBitmapAggregation"
  private val plainParquetTargetScanKey =
    "spark.gluten.sql.delta.delete.dv.enablePlainParquetTargetScan"
  private val driverStatsFooterMaxFilesKey =
    "spark.gluten.sql.delta.delete.dv.driverStatsFooterMaxFiles"
  private val plainParquetDriverBitmapScanGlutenEnabledKey =
    "spark.gluten.sql.delta.delete.dv.plainParquetDriverBitmapScan.glutenEnabled"
  private val driverColumnarBitmapMergeKey =
    "spark.gluten.sql.delta.delete.dv.driverColumnarBitmapMerge.enabled"

  private object CardinalityAndBitmapStruct {
    val name: String = "CardinalityAndBitmapStruct"
    def cardinality: String = s"$name.cardinality"
    def bitmap: String = s"$name.bitmap"
  }

  def findTouchedFiles(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      hasDVsEnabled: Boolean,
      deltaLog: DeltaLog,
      targetDf: DataFrame,
      fileIndex: TahoeBatchFileIndex,
      condition: Expression,
      opName: String): Seq[TouchedFileWithDV] = {
    require(
      DMLWithDeletionVectorsHelper.SUPPORTED_DML_COMMANDS.contains(opName),
      s"Expecting opName to be one of " +
        s"${DMLWithDeletionVectorsHelper.SUPPORTED_DML_COMMANDS.mkString(", ")}, " +
        s"but got '$opName'."
    )

    recordDeltaOperation(deltaLog, opType = s"$opName.findTouchedFiles.gluten") {
      val candidateFiles = fileIndex.addFiles
      val matchedRowIndexSets = buildRowIndexSetsForFilesMatchingCondition(
        sparkSession,
        txn,
        hasDVsEnabled,
        targetDf,
        candidateFiles,
        condition)

      val nameToAddFileMap = generateCandidateFileMap(txn.deltaLog.dataPath, candidateFiles)
      DMLWithDeletionVectorsHelper.findFilesWithMatchingRows(
        txn,
        nameToAddFileMap,
        matchedRowIndexSets)
    }
  }

  def processUnmodifiedData(
      spark: SparkSession,
      touchedFiles: Seq[TouchedFileWithDV],
      snapshot: Snapshot): (Seq[FileAction], Map[String, Long]) = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(spark)
    val start = GlutenDeltaDeleteTiming.now()
    val (fullyRemovedFiles, notFullyRemovedFiles) = touchedFiles.partition(_.isFullyReplaced())
    val missingNumRecordsCount =
      notFullyRemovedFiles.count(_.fileLogEntry.numPhysicalRecords.isEmpty)

    val canUseLocalStatsRewrite =
      !spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS) &&
        missingNumRecordsCount <= getDriverStatsFooterMaxFiles(spark)
    if (!canUseLocalStatsRewrite) {
      val result = DMLWithDeletionVectorsHelper.processUnmodifiedData(spark, touchedFiles, snapshot)
      GlutenDeltaDeleteTiming.logIfEnabled(
        timingEnabled,
        s"processUnmodifiedData totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
          s"mode=deltaStatsRewrite touchedFiles=${touchedFiles.size} " +
          s"fullyRemovedFiles=${fullyRemovedFiles.size} " +
          s"notFullyRemovedFiles=${notFullyRemovedFiles.size} actions=${result._1.size}"
      )
      return result
    }

    try {
      val numModifiedRows = touchedFiles.map(_.numberOfModifiedRows).sum.toLong
      val numRemovedFiles = fullyRemovedFiles.size.toLong
      val timestamp = System.currentTimeMillis()
      val fullyRemoved = fullyRemovedFiles.map(_.fileLogEntry.removeWithTimestamp(timestamp))
      val hadoopConf = snapshot.deltaLog.newDeltaHadoopConf()

      val dvUpdates = notFullyRemovedFiles.map {
        fileWithDVInfo =>
          withWideStatsForDeletionVector(hadoopConf, snapshot, fileWithDVInfo.fileLogEntry)
            .removeRows(
              deletionVector = fileWithDVInfo.newDeletionVector,
              updateStats = false)
      }
      val (dvAddFiles, dvRemoveFiles) = dvUpdates.unzip

      var (numDeletionVectorsAdded, numDeletionVectorsRemoved, numDeletionVectorsUpdated) =
        dvUpdates.foldLeft((0L, 0L, 0L)) {
          case ((added, removed, updated), (addFile, removeFile)) =>
            (Option(addFile.deletionVector), Option(removeFile.deletionVector)) match {
              case (Some(_), Some(_)) => (added, removed, updated + 1)
              case (None, Some(_)) => (added, removed + 1, updated)
              case (Some(_), None) => (added + 1, removed, updated)
              case _ => (added, removed, updated)
            }
        }
      numDeletionVectorsRemoved += fullyRemoved.count(_.deletionVector != null)

      val metricMap = Map(
        "numModifiedRows" -> numModifiedRows,
        "numRemovedFiles" -> numRemovedFiles,
        "numDeletionVectorsAdded" -> numDeletionVectorsAdded,
        "numDeletionVectorsRemoved" -> numDeletionVectorsRemoved,
        "numDeletionVectorsUpdated" -> numDeletionVectorsUpdated
      )
      val actions: Seq[FileAction] = fullyRemoved ++ dvAddFiles ++ dvRemoveFiles
      GlutenDeltaDeleteTiming.logIfEnabled(
        timingEnabled,
        s"processUnmodifiedData totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
          s"mode=localStatsWideBounds touchedFiles=${touchedFiles.size} " +
          s"fullyRemovedFiles=${fullyRemovedFiles.size} " +
          s"notFullyRemovedFiles=${notFullyRemovedFiles.size} " +
          s"missingNumRecords=$missingNumRecordsCount actions=${actions.size}"
      )
      (actions, metricMap)
    } catch {
      case NonFatal(e) =>
        GlutenDeltaDeleteTiming.logIfEnabled(
          timingEnabled,
          s"processUnmodifiedData localStatsWideBoundsFailed " +
            s"error=${e.getClass.getName}: ${Option(e.getMessage).getOrElse("")}"
        )
        val result =
          DMLWithDeletionVectorsHelper.processUnmodifiedData(spark, touchedFiles, snapshot)
        GlutenDeltaDeleteTiming.logIfEnabled(
          timingEnabled,
          s"processUnmodifiedData totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
            s"mode=deltaStatsRewriteAfterLocalFailure touchedFiles=${touchedFiles.size} " +
            s"fullyRemovedFiles=${fullyRemovedFiles.size} " +
            s"notFullyRemovedFiles=${notFullyRemovedFiles.size} " +
            s"missingNumRecords=$missingNumRecordsCount actions=${result._1.size}"
        )
        result
    }
  }

  private def withWideStatsForDeletionVector(
      hadoopConf: Configuration,
      snapshot: Snapshot,
      addFile: AddFile): AddFile = {
    val stats =
      if (addFile.stats == null || addFile.stats.isEmpty) {
        JsonUtils.mapper.createObjectNode()
      } else {
        JsonUtils.mapper.readTree(addFile.stats).asInstanceOf[ObjectNode]
      }

    val fileWithNumRecords =
      if (addFile.numPhysicalRecords.isEmpty) {
        stats.put("numRecords", readParquetNumRecords(hadoopConf, snapshot, addFile))
        addFile.copy(stats = JsonUtils.mapper.writer.writeValueAsString(stats))
      } else {
        addFile
      }

    val hadTightBounds =
      Option(stats.get("tightBounds")).exists(node => !node.isNull && node.asBoolean(false))
    if (hadTightBounds) {
      for {
        logicalRecords <- fileWithNumRecords.numLogicalRecords
        physicalRecords <- fileWithNumRecords.numPhysicalRecords
        nullCount <- Option(stats.get("nullCount")).collect { case node: ObjectNode => node }
      } {
        widenAllNullCounts(nullCount, logicalRecords, physicalRecords)
      }
    }
    stats.put("tightBounds", false)
    fileWithNumRecords.copy(stats = JsonUtils.mapper.writer.writeValueAsString(stats))
  }

  private def readParquetNumRecords(
      hadoopConf: Configuration,
      snapshot: Snapshot,
      addFile: AddFile): Long = {
    val path = absolutePath(snapshot.deltaLog.dataPath.toString, addFile.path)
    val footer =
      ParquetFooterReaderShim.readFooter(hadoopConf, path, ParquetMetadataConverter.NO_FILTER)
    val blocks = footer.getBlocks
    var rowCount = 0L
    var index = 0
    while (index < blocks.size()) {
      rowCount += blocks.get(index).getRowCount
      index += 1
    }
    rowCount
  }

  private def widenAllNullCounts(
      nullCount: ObjectNode,
      logicalRecords: Long,
      physicalRecords: Long): Unit = {
    val fields = nullCount.fields()
    while (fields.hasNext) {
      val field = fields.next()
      field.getValue match {
        case child: ObjectNode =>
          widenAllNullCounts(child, logicalRecords, physicalRecords)
        case value if value != null && value.isNumber && value.asLong() == logicalRecords =>
          nullCount.put(field.getKey, physicalRecords)
        case _ =>
      }
    }
  }

  private def buildRowIndexSetsForFilesMatchingCondition(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      tableHasDVs: Boolean,
      targetDf: DataFrame,
      candidateFiles: Seq[AddFile],
      condition: Expression): Seq[DeletionVectorResult] = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(sparkSession)
    val start = GlutenDeltaDeleteTiming.now()
    val useMetadataRowIndex =
      sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)
    val fileNameColumn = col(s"$METADATA_NAME.$FILE_PATH")
    val rowIndexColumn =
      if (useMetadataRowIndex) {
        col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}")
      } else {
        col(ROW_INDEX_COLUMN_NAME)
      }

    val matchedRowsDfStart = GlutenDeltaDeleteTiming.now()
    val (matchedRowsDf, matchedRowsSource) =
      maybeCreatePlainParquetMatchedRowsDf(sparkSession, txn, candidateFiles, condition)
        .map(_ -> "plainParquet")
        .getOrElse {
          targetDf
            .withColumn(DeletionVectorBitmapGenerator.FILE_NAME_COL, fileNameColumn)
            .filter(Column(condition))
            .withColumn(DeletionVectorBitmapGenerator.ROW_INDEX_COL, rowIndexColumn)
            .select(
              col(DeletionVectorBitmapGenerator.FILE_NAME_COL),
              col(DeletionVectorBitmapGenerator.ROW_INDEX_COL)) -> "delta"
        }
    val matchedRowsDfMs = GlutenDeltaDeleteTiming.elapsedMs(matchedRowsDfStart)
    val preferDriverBitmapMerge =
      matchedRowsSource == "plainParquet" &&
        !shouldUseNativeBitmapAggregation(sparkSession) &&
        candidateFiles.size <= getDriverMergeMaxFiles(sparkSession)
    val useNativeBitmapAggregation =
      !preferDriverBitmapMerge && shouldUseNativeBitmapAggregation(sparkSession)

    val dvIdMapStart = GlutenDeltaDeleteTiming.now()
    val dvIdByCanonicalPath =
      if (tableHasDVs) {
        val basePath = txn.deltaLog.dataPath.toString
        candidateFiles.map {
          add =>
            val canonicalPath = SparkPath.fromPath(absolutePath(basePath, add.path)).urlEncoded
            canonicalPath -> Option(add.deletionVector).map(_.serializeToBase64())
        }.toMap
      } else {
        Map.empty[String, Option[String]]
      }
    val dvIdMapMs = GlutenDeltaDeleteTiming.elapsedMs(dvIdMapStart)
    val existingDvReadStart = GlutenDeltaDeleteTiming.now()
    val existingDvByCanonicalPath =
      if (matchedRowsSource == "plainParquet" && !useNativeBitmapAggregation) {
        readExistingDeletionVectorBitmaps(sparkSession, txn, candidateFiles)
      } else {
        Map.empty[String, Array[Byte]]
      }
    val existingDvReadMs = GlutenDeltaDeleteTiming.elapsedMs(existingDvReadStart)

    if (useNativeBitmapAggregation) {
      return buildDeletionVectorsWithBitmapAggregation(
        sparkSession,
        txn,
        tableHasDVs,
        matchedRowsDf,
        dvIdByCanonicalPath,
        matchedRowsSource,
        matchedRowsDfMs,
        dvIdMapMs,
        existingDvReadMs,
        start,
        candidateFiles.size
      )
    }

    if (
      !preferDriverBitmapMerge &&
      shouldUseNativeColumnarBitmapMerge(sparkSession, matchedRowsDf)
    ) {
      return buildDeletionVectorsWithColumnarMerge(
        sparkSession,
        txn,
        tableHasDVs,
        matchedRowsDf,
        dvIdByCanonicalPath,
        matchedRowsSource,
        matchedRowsDfMs,
        dvIdMapMs,
        existingDvReadMs,
        start,
        candidateFiles.size
      )
    }

    val broadcastDvIdByCanonicalPath =
      sparkSession.sparkContext.broadcast(dvIdByCanonicalPath)
    val broadcastExistingDvByCanonicalPath =
      sparkSession.sparkContext.broadcast(existingDvByCanonicalPath)

    try {
      val driverMergeMaxFiles = getDriverMergeMaxFiles(sparkSession)
      if (candidateFiles.size <= driverMergeMaxFiles) {
        val scanGlutenEnabled =
          useGlutenForPlainParquetDriverBitmapScan(
            sparkSession,
            matchedRowsSource,
            tableHasDVs)
        val collectStart = GlutenDeltaDeleteTiming.now()
        var columnarDriverMergeUsed = false
        val partialData =
          withPlainParquetDriverBitmapScanConfs(sparkSession, matchedRowsSource, tableHasDVs) {
            val queryExecution = matchedRowsDf.queryExecution
            val executedPlan = queryExecution.executedPlan
            val columnarDriverBitmapPlan =
              if (
                matchedRowsSource == "plainParquet" &&
                scanGlutenEnabled &&
                isDriverColumnarBitmapMergeEnabled(sparkSession)
              ) {
                columnarBitmapInputPlan(executedPlan)
              } else {
                None
              }
            SQLExecution.withNewExecutionId(
              queryExecution,
              Some("Gluten Delta DELETE deletion-vector row-index scan")) {
              columnarDriverBitmapPlan match {
                case Some(columnarPlan) =>
                  columnarDriverMergeUsed = true
                  columnarPlan
                    .executeColumnar()
                    .mapPartitions {
                      batches =>
                        buildPartialDeletionVectorDataFromColumnar(
                          batches,
                          tableHasDVs,
                          broadcastDvIdByCanonicalPath.value,
                          broadcastExistingDvByCanonicalPath.value)
                    }
                    .collect()
                    .toSeq
                case None =>
                  queryExecution.toRdd
                    .mapPartitions {
                      rows =>
                        buildPartialDeletionVectorData(
                          rows,
                          tableHasDVs,
                          broadcastDvIdByCanonicalPath.value,
                          broadcastExistingDvByCanonicalPath.value)
                    }
                    .collect()
                    .toSeq
              }
            }
          }
        val collectMs = GlutenDeltaDeleteTiming.elapsedMs(collectStart)
        val mergeStart = GlutenDeltaDeleteTiming.now()
        val mergedData = mergeDeletionVectorDataIfNeeded(partialData)
        val mergeMs = GlutenDeltaDeleteTiming.elapsedMs(mergeStart)
        val storeStart = GlutenDeltaDeleteTiming.now()
        val result = storeDeletionVectorsOnDriver(sparkSession, txn, mergedData)
        val storeMs = GlutenDeltaDeleteTiming.elapsedMs(storeStart)
        GlutenDeltaDeleteTiming.logIfEnabled(
          timingEnabled,
          s"buildDeletionVectors totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
            s"strategy=driverJvmBitmap matchedRowsSource=$matchedRowsSource " +
            s"scanGlutenEnabled=$scanGlutenEnabled " +
            s"columnarDriverMerge=$columnarDriverMergeUsed " +
            s"matchedRowsDfMs=$matchedRowsDfMs dvIdMapMs=$dvIdMapMs collectMs=$collectMs " +
            s"existingDvReadMs=$existingDvReadMs mergeMs=$mergeMs storeMs=$storeMs " +
            s"candidateFiles=${candidateFiles.size} " +
            s"partialBitmaps=${partialData.size} mergedBitmaps=${mergedData.size} " +
            s"storedDVs=${result.size}"
        )
        result
      } else {
        import sparkSession.implicits._

        val partialBitmaps = matchedRowsDf.mapPartitions {
          rows =>
            val dvIds = broadcastDvIdByCanonicalPath.value
            val bitmaps = mutable.HashMap.empty[(String, Option[String]), RoaringBitmapArray]
            rows.foreach {
              row =>
                val filePath = row.getString(0)
                val rowIndex = row.getLong(1)
                val deletionVectorId =
                  if (tableHasDVs) {
                    dvIds.getOrElse(
                      filePath,
                      throw new IllegalStateException(
                        s"Could not find deletion vector metadata for matched file $filePath"))
                  } else {
                    None
                  }
                val bitmap =
                  bitmaps.getOrElseUpdate((filePath, deletionVectorId), new RoaringBitmapArray())
                bitmap.add(rowIndex)
            }
            bitmaps.iterator.map {
              case ((filePath, deletionVectorId), bitmap) =>
                bitmap.runOptimize()
                DeletionVectorData(
                  filePath = filePath,
                  deletionVectorId = deletionVectorId,
                  deletedRowIndexSet =
                    bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
                  deletedRowIndexCount = bitmap.cardinality
                )
            }
        }(DeletionVectorData.encoder)

        val deletionVectorData = partialBitmaps
          .groupByKey(_.filePath)
          .mapGroups {
            (_: String, partials: Iterator[DeletionVectorData]) =>
              mergeDeletionVectorData(partials.toSeq).head
          }(DeletionVectorData.encoder)

        val prefixLen = DeltaUtils.getRandomPrefixLength(txn.metadata)
        val storageMapper = DeletionVectorWriter.createMapperToStoreDeletionVectors(
          sparkSession,
          txn.deltaLog.newDeltaHadoopConf(),
          txn.deltaLog.dataPath,
          prefixLen)
        val distributedStart = GlutenDeltaDeleteTiming.now()
        val result = deletionVectorData.mapPartitions(storageMapper)(
          DeletionVectorResult.encoder).collect().toSeq
        GlutenDeltaDeleteTiming.logIfEnabled(
          timingEnabled,
          s"buildDeletionVectors totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
            s"strategy=distributedSparkBitmap matchedRowsSource=$matchedRowsSource " +
            s"matchedRowsDfMs=$matchedRowsDfMs dvIdMapMs=$dvIdMapMs " +
            s"collectAndStoreMs=${GlutenDeltaDeleteTiming.elapsedMs(distributedStart)} " +
            s"candidateFiles=${candidateFiles.size} storedDVs=${result.size}"
        )
        result
      }
    } finally {
      broadcastDvIdByCanonicalPath.destroy()
      broadcastExistingDvByCanonicalPath.destroy()
    }
  }

  private def isNativeDmlRowIndexScanEnabled(sparkSession: SparkSession): Boolean =
    sparkSession.sessionState.conf.getConfString(nativeDmlRowIndexScanKey, "false").toBoolean

  private def getDriverMergeMaxFiles(sparkSession: SparkSession): Int =
    sparkSession.sessionState.conf.getConfString(driverMergeMaxFilesKey, "64").toInt

  private def getDriverStatsFooterMaxFiles(sparkSession: SparkSession): Int =
    sparkSession.sessionState.conf.getConfString(driverStatsFooterMaxFilesKey, "64").toInt

  private def isDriverColumnarBitmapMergeEnabled(sparkSession: SparkSession): Boolean =
    sparkSession.sessionState.conf.getConfString(driverColumnarBitmapMergeKey, "false").toBoolean

  private def useGlutenForPlainParquetDriverBitmapScan(
      sparkSession: SparkSession,
      matchedRowsSource: String,
      tableHasDVs: Boolean): Boolean = {
    if (matchedRowsSource != "plainParquet") {
      return true
    }
    Option(
      sparkSession.sessionState.conf
        .getConfString(plainParquetDriverBitmapScanGlutenEnabledKey, null))
      .map(_.toBoolean)
      // Existing-DV plain-Parquet scans filter prior DV rows before bitmap creation.
      .getOrElse(true)
  }

  private def columnarBitmapInputPlan(plan: SparkPlan): Option[SparkPlan] = plan match {
    case transition: ColumnarToRowTransition if transition.child.supportsColumnar =>
      Some(transition.child)
    case _ if plan.supportsColumnar =>
      Some(plan)
    case _ =>
      None
  }

  private def withPlainParquetDriverBitmapScanConfs[T](
      sparkSession: SparkSession,
      matchedRowsSource: String,
      tableHasDVs: Boolean)(f: => T): T = {
    if (useGlutenForPlainParquetDriverBitmapScan(sparkSession, matchedRowsSource, tableHasDVs)) {
      f
    } else {
      withSQLConfStrings(sparkSession, GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        f
      }
    }
  }

  private def withSQLConfStrings[T](
      sparkSession: SparkSession,
      confs: (String, String)*)(f: => T): T = {
    val sqlConf = sparkSession.sessionState.conf
    val previousValues =
      confs.map { case (key, _) => key -> Option(sqlConf.getConfString(key, null)) }
    confs.foreach { case (key, value) => sqlConf.setConfString(key, value) }
    try {
      f
    } finally {
      previousValues.foreach {
        case (key, Some(value)) => sqlConf.setConfString(key, value)
        case (key, None) => sqlConf.unsetConf(key)
      }
    }
  }

  private def maybeCreatePlainParquetMatchedRowsDf(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      candidateFiles: Seq[AddFile],
      condition: Expression): Option[DataFrame] = {
    if (!shouldUsePlainParquetTargetScan(sparkSession, txn, candidateFiles)) {
      return None
    }

    val basePath = txn.deltaLog.dataPath.toString
    val absolutePaths = candidateFiles.map(add => absolutePath(basePath, add.path).toString)

    val parquetDf = sparkSession.read
      .format("parquet")
      .schema(plainParquetTargetScanSchema(txn))
      .option("basePath", basePath)
      .load(absolutePaths: _*)

    rebaseConditionToDataFrame(sparkSession, parquetDf, txn, condition).map {
      rebasedCondition =>
        parquetDf
          .withColumn(
            DeletionVectorBitmapGenerator.FILE_NAME_COL,
            col(s"$METADATA_NAME.$FILE_PATH"))
          .filter(Column(rebasedCondition))
          .withColumn(
            DeletionVectorBitmapGenerator.ROW_INDEX_COL,
            col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}"))
          .select(
            col(DeletionVectorBitmapGenerator.FILE_NAME_COL),
            col(DeletionVectorBitmapGenerator.ROW_INDEX_COL))
    }
  }

  private def rebaseConditionToDataFrame(
      sparkSession: SparkSession,
      df: DataFrame,
      txn: OptimisticTransaction,
      condition: Expression): Option[Expression] = {
    val resolver = sparkSession.sessionState.conf.resolver
    val output = df.queryExecution.analyzed.output
    val columnMappingMode = txn.metadata.columnMappingMode
    val referenceSchema = txn.snapshot.schema

    def outputAttributeForNames(names: Seq[String]): Option[AttributeReference] = {
      output.collectFirst {
        case out: AttributeReference if names.exists(name => resolver(out.name, name)) => out
      }
    }

    def attributeNames(attr: AttributeReference): Seq[String] = {
      if (columnMappingMode == NoMapping) {
        Seq(attr.name)
      } else {
        try {
          val physicalAttr =
            DeltaColumnMapping
              .createPhysicalAttributes(Seq(attr), referenceSchema, columnMappingMode)
              .head
          Seq(physicalAttr.name, attr.name).distinct
        } catch {
          case NonFatal(_) => Seq(attr.name)
        }
      }
    }

    def unresolvedAttributeNames(attr: UnresolvedAttribute): Seq[String] = {
      val logicalName = attr.nameParts.lastOption.getOrElse(attr.name)
      if (columnMappingMode == NoMapping) {
        Seq(logicalName)
      } else {
        val physicalName = DeltaColumnMapping
          .getLogicalNameToPhysicalNameMap(referenceSchema)
          .collectFirst {
            case (logicalPath, physicalPath)
                if logicalPath.size == attr.nameParts.size &&
                  logicalPath.zip(attr.nameParts).forall {
                    case (logicalPart, namePart) => resolver(logicalPart, namePart)
                  } =>
              physicalPath.last
          }
        (physicalName.toSeq :+ logicalName).distinct
      }
    }

    var missingAttribute = false
    val rebased = condition.transformUp {
      case attr: AttributeReference =>
        outputAttributeForNames(attributeNames(attr)) match {
          case Some(replacement) => replacement
          case None =>
            missingAttribute = true
            attr
        }
      case attr: UnresolvedAttribute =>
        outputAttributeForNames(unresolvedAttributeNames(attr)) match {
          case Some(replacement) => replacement
          case None =>
            missingAttribute = true
            attr
        }
    }
    if (missingAttribute) None else Some(rebased)
  }

  private def plainParquetTargetScanSchema(txn: OptimisticTransaction) = {
    val columnMappingMode = txn.metadata.columnMappingMode
    val schema = txn.snapshot.schema
    if (columnMappingMode == NoMapping) {
      schema
    } else {
      stripParquetFieldIds(
        DeltaColumnMapping.createPhysicalSchema(schema, schema, columnMappingMode))
    }
  }

  private def stripParquetFieldIds(schema: StructType): StructType =
    StructType(schema.fields.map(stripParquetFieldIds))

  private def stripParquetFieldIds(field: StructField): StructField = {
    val metadata = new MetadataBuilder()
      .withMetadata(field.metadata)
      .remove(ParquetFieldIdMetadataKey)
      .build()
    field.copy(
      dataType = stripParquetFieldIds(field.dataType),
      metadata = metadata)
  }

  private def stripParquetFieldIds(dataType: DataType): DataType = dataType match {
    case struct: StructType => stripParquetFieldIds(struct)
    case ArrayType(elementType, containsNull) =>
      ArrayType(stripParquetFieldIds(elementType), containsNull)
    case MapType(keyType, valueType, valueContainsNull) =>
      MapType(
        stripParquetFieldIds(keyType),
        stripParquetFieldIds(valueType),
        valueContainsNull)
    case other => other
  }

  private def readExistingDeletionVectorBitmaps(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      candidateFiles: Seq[AddFile]): Map[String, Array[Byte]] = {
    val filesWithDeletionVectors = candidateFiles.filter(_.deletionVector != null)
    if (filesWithDeletionVectors.isEmpty) {
      return Map.empty
    }

    val dvStore = DeletionVectorStore.createInstance(sparkSession.sessionState.newHadoopConf())
    val tablePath = txn.deltaLog.dataPath
    val basePath = tablePath.toString
    filesWithDeletionVectors.map {
      add =>
        val canonicalPath = SparkPath.fromPath(absolutePath(basePath, add.path)).urlEncoded
        val bitmap = dvStore.read(add.deletionVector, tablePath)
        canonicalPath -> bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    }.toMap
  }

  private def shouldUsePlainParquetTargetScan(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      candidateFiles: Seq[AddFile]): Boolean = {
    val enabled =
      sparkSession.sessionState.conf
        .getConfString(plainParquetTargetScanKey, "true")
        .toBoolean
    val hasExistingDeletionVectors = candidateFiles.exists(_.deletionVector != null)
    val canUseExistingDeletionVectors =
      !hasExistingDeletionVectors ||
        candidateFiles.size <= getDriverMergeMaxFiles(sparkSession) ||
        shouldUseNativeBitmapAggregation(sparkSession)

    enabled &&
    isNativeDmlRowIndexScanEnabled(sparkSession) &&
    candidateFiles.nonEmpty &&
    canUseExistingDeletionVectors
  }

  private def shouldUseNativeColumnarBitmapMerge(
      sparkSession: SparkSession,
      matchedRowsDf: DataFrame): Boolean = {
    val nativeColumnarBitmapMergeEnabled =
      sparkSession.sessionState.conf
        .getConfString(nativeColumnarBitmapMergeKey, "false")
        .toBoolean
    isNativeDmlRowIndexScanEnabled(sparkSession) &&
    nativeColumnarBitmapMergeEnabled &&
    matchedRowsDf.queryExecution.executedPlan.supportsColumnar
  }

  private def shouldUseNativeBitmapAggregation(sparkSession: SparkSession): Boolean = {
    val nativeBitmapAggregationEnabled =
      sparkSession.sessionState.conf.getConfString(nativeBitmapAggregationKey, "true").toBoolean
    isNativeDmlRowIndexScanEnabled(sparkSession) && nativeBitmapAggregationEnabled
  }

  private def buildDeletionVectorsWithColumnarMerge(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      tableHasDVs: Boolean,
      matchedRowsDf: DataFrame,
      dvIds: Map[String, Option[String]],
      matchedRowsSource: String,
      matchedRowsDfMs: Long,
      dvIdMapMs: Long,
      existingDvReadMs: Long,
      start: Long,
      candidateFileCount: Long): Seq[DeletionVectorResult] = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(sparkSession)
    val broadcastDvIdByCanonicalPath = sparkSession.sparkContext.broadcast(dvIds)
    try {
      val queryExecution = matchedRowsDf.queryExecution
      val collectStart = GlutenDeltaDeleteTiming.now()
      val partialData = SQLExecution.withNewExecutionId(
        queryExecution,
        Some("Gluten Delta DELETE deletion-vector native columnar row-index scan")) {
        queryExecution.executedPlan
          .executeColumnar()
          .mapPartitions {
            batches =>
              buildPartialDeletionVectorDataFromColumnar(
                batches,
                tableHasDVs,
                broadcastDvIdByCanonicalPath.value,
                Map.empty[String, Array[Byte]])
          }
          .collect()
          .toSeq
      }
      val collectMs = GlutenDeltaDeleteTiming.elapsedMs(collectStart)
      val mergeStart = GlutenDeltaDeleteTiming.now()
      val mergedData = mergeDeletionVectorDataIfNeeded(partialData)
      val mergeMs = GlutenDeltaDeleteTiming.elapsedMs(mergeStart)
      val storeStart = GlutenDeltaDeleteTiming.now()
      val result = storeDeletionVectorsOnDriver(sparkSession, txn, mergedData)
      val storeMs = GlutenDeltaDeleteTiming.elapsedMs(storeStart)
      GlutenDeltaDeleteTiming.logIfEnabled(
        timingEnabled,
        s"buildDeletionVectors totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
          s"strategy=nativeColumnarMerge matchedRowsSource=$matchedRowsSource " +
          s"matchedRowsDfMs=$matchedRowsDfMs dvIdMapMs=$dvIdMapMs " +
          s"existingDvReadMs=$existingDvReadMs collectMs=$collectMs " +
          s"mergeMs=$mergeMs storeMs=$storeMs candidateFiles=$candidateFileCount " +
          s"partialBitmaps=${partialData.size} mergedBitmaps=${mergedData.size} " +
          s"storedDVs=${result.size}"
      )
      result
    } finally {
      broadcastDvIdByCanonicalPath.destroy()
    }
  }

  private def buildDeletionVectorsWithBitmapAggregation(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      tableHasDVs: Boolean,
      matchedRowsDf: DataFrame,
      dvIds: Map[String, Option[String]],
      matchedRowsSource: String,
      matchedRowsDfMs: Long,
      dvIdMapMs: Long,
      existingDvReadMs: Long,
      start: Long,
      candidateFileCount: Long): Seq[DeletionVectorResult] = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(sparkSession)
    val bitmapAggregator = new BitmapAggregator(
      col(DeletionVectorBitmapGenerator.ROW_INDEX_COL).expr,
      RoaringBitmapArrayFormat.Portable)
    val aggregated = matchedRowsDf
      .groupBy(col(DeletionVectorBitmapGenerator.FILE_NAME_COL))
      .agg(
        Column(bitmapAggregator.toAggregateExpression(isDistinct = false))
          .as(CardinalityAndBitmapStruct.name))
      .select(
        col(DeletionVectorBitmapGenerator.FILE_NAME_COL),
        col(CardinalityAndBitmapStruct.bitmap)
          .as(DeletionVectorBitmapGenerator.DELETED_ROW_INDEX_BITMAP),
        col(CardinalityAndBitmapStruct.cardinality)
          .as(DeletionVectorBitmapGenerator.DELETED_ROW_INDEX_COUNT)
      )

    val queryExecution = aggregated.queryExecution
    val aggregationStart = GlutenDeltaDeleteTiming.now()
    val deletionVectorData = SQLExecution.withNewExecutionId(
      queryExecution,
      Some("Gluten Delta DELETE deletion-vector native bitmap aggregation")) {
      queryExecution.toRdd
        .map {
          row =>
            val filePath = row.getUTF8String(0).toString
            val deletionVectorId =
              if (tableHasDVs) {
                dvIds.getOrElse(
                  filePath,
                  throw new IllegalStateException(
                    s"Could not find deletion vector metadata for matched file $filePath"))
              } else {
                None
              }
            DeletionVectorData(
              filePath = filePath,
              deletionVectorId = deletionVectorId,
              deletedRowIndexSet = row.getBinary(1),
              deletedRowIndexCount = row.getLong(2))
        }
        .collect()
        .toSeq
    }
    val aggregationMs = GlutenDeltaDeleteTiming.elapsedMs(aggregationStart)

    val storeStart = GlutenDeltaDeleteTiming.now()
    val result = storeDeletionVectorsOnDriver(
      sparkSession,
      txn,
      deletionVectorData,
      skipUnchangedExistingDvs = tableHasDVs)
    val storeMs = GlutenDeltaDeleteTiming.elapsedMs(storeStart)
    GlutenDeltaDeleteTiming.logIfEnabled(
      timingEnabled,
      s"buildDeletionVectors totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
        s"strategy=nativeBitmapAggregation matchedRowsSource=$matchedRowsSource " +
        s"matchedRowsDfMs=$matchedRowsDfMs dvIdMapMs=$dvIdMapMs " +
        s"existingDvReadMs=$existingDvReadMs " +
        s"aggregationCollectMs=$aggregationMs storeMs=$storeMs " +
        s"candidateFiles=$candidateFileCount aggregatedBitmaps=${deletionVectorData.size} " +
        s"storedDVs=${result.size}"
    )
    result
  }

  private def buildPartialDeletionVectorDataFromColumnar(
      batches: Iterator[ColumnarBatch],
      tableHasDVs: Boolean,
      dvIds: Map[String, Option[String]],
      existingDVs: Map[String, Array[Byte]]): Iterator[DeletionVectorData] = {
    val bitmaps = mutable.HashMap.empty[(String, Option[String]), RoaringBitmapArray]
    var currentFilePathUtf8: UTF8String = null
    var currentFilePath: String = null
    var currentDeletionVectorId: Option[String] = None
    var currentExistingDeletionVector: RoaringBitmapArray = null
    var currentBitmap: RoaringBitmapArray = null

    def switchFilePath(filePathUtf8: UTF8String): Unit = {
      currentFilePathUtf8 = filePathUtf8.copy()
      currentFilePath = currentFilePathUtf8.toString
      currentDeletionVectorId =
        if (tableHasDVs) {
          dvIds.getOrElse(
            currentFilePath,
            throw new IllegalStateException(
              s"Could not find deletion vector metadata for matched file $currentFilePath"))
        } else {
          None
        }
      currentBitmap = bitmaps.getOrElseUpdate(
        (currentFilePath, currentDeletionVectorId),
        new RoaringBitmapArray())
      currentExistingDeletionVector =
        existingDVs.get(currentFilePath).map(RoaringBitmapArray.readFrom).orNull
    }

    def addRowIndex(rowIndex: Long): Unit = {
      if (
        currentExistingDeletionVector == null ||
        !currentExistingDeletionVector.contains(rowIndex)
      ) {
        currentBitmap.add(rowIndex)
      }
    }

    batches.foreach {
      batch =>
        val loadedBatch =
          if (batch.numCols() > 0 && batch.column(0).isInstanceOf[IndicatorVector]) {
            ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), batch)
          } else {
            batch
          }
        try {
          val filePathColumn = loadedBatch.column(0)
          val rowIndexColumn = loadedBatch.column(1)
          val numRows = loadedBatch.numRows()
          if (numRows > 0) {
            val firstFilePathUtf8 = filePathColumn.getUTF8String(0)
            val lastFilePathUtf8 = filePathColumn.getUTF8String(numRows - 1)
            var rowId = 0
            if (firstFilePathUtf8.equals(lastFilePathUtf8)) {
              if (currentFilePathUtf8 == null || !currentFilePathUtf8.equals(firstFilePathUtf8)) {
                switchFilePath(firstFilePathUtf8)
              }
              while (rowId < numRows) {
                addRowIndex(rowIndexColumn.getLong(rowId))
                rowId += 1
              }
            } else {
              while (rowId < numRows) {
                val filePathUtf8 = filePathColumn.getUTF8String(rowId)
                if (currentFilePathUtf8 == null || !currentFilePathUtf8.equals(filePathUtf8)) {
                  switchFilePath(filePathUtf8)
                }
                addRowIndex(rowIndexColumn.getLong(rowId))
                rowId += 1
              }
            }
          }
        } finally {
          loadedBatch.close()
        }
    }

    bitmaps.iterator.filterNot(_._2.isEmpty).map {
      case ((filePath, deletionVectorId), bitmap) =>
        bitmap.runOptimize()
        DeletionVectorData(
          filePath = filePath,
          deletionVectorId = deletionVectorId,
          deletedRowIndexSet = bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
          deletedRowIndexCount = bitmap.cardinality
        )
    }
  }

  private def buildPartialDeletionVectorData(
      rows: Iterator[InternalRow],
      tableHasDVs: Boolean,
      dvIds: Map[String, Option[String]],
      existingDVs: Map[String, Array[Byte]]): Iterator[DeletionVectorData] = {
    val bitmaps = mutable.HashMap.empty[(String, Option[String]), RoaringBitmapArray]
    var currentFilePathUtf8: UTF8String = null
    var currentFilePath: String = null
    var currentDeletionVectorId: Option[String] = None
    var currentExistingDeletionVector: RoaringBitmapArray = null
    var currentBitmap: RoaringBitmapArray = null

    rows.foreach {
      row =>
        val filePathUtf8 = row.getUTF8String(0)
        if (currentFilePathUtf8 == null || !currentFilePathUtf8.equals(filePathUtf8)) {
          currentFilePathUtf8 = filePathUtf8.copy()
          currentFilePath = currentFilePathUtf8.toString
          currentDeletionVectorId =
            if (tableHasDVs) {
              dvIds.getOrElse(
                currentFilePath,
                throw new IllegalStateException(
                  s"Could not find deletion vector metadata for matched file $currentFilePath"))
            } else {
              None
            }
          currentBitmap = bitmaps.getOrElseUpdate(
            (currentFilePath, currentDeletionVectorId),
            new RoaringBitmapArray())
          currentExistingDeletionVector =
            existingDVs.get(currentFilePath).map(RoaringBitmapArray.readFrom).orNull
        }

        val rowIndex = row.getLong(1)
        if (
          currentExistingDeletionVector == null ||
          !currentExistingDeletionVector.contains(rowIndex)
        ) {
          currentBitmap.add(rowIndex)
        }
    }

    bitmaps.iterator.filterNot(_._2.isEmpty).map {
      case ((filePath, deletionVectorId), bitmap) =>
        bitmap.runOptimize()
        DeletionVectorData(
          filePath = filePath,
          deletionVectorId = deletionVectorId,
          deletedRowIndexSet = bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
          deletedRowIndexCount = bitmap.cardinality
        )
    }
  }

  private def mergeDeletionVectorData(
      partials: Seq[DeletionVectorData]): Seq[DeletionVectorData] = {
    partials.groupBy(_.filePath).values.toSeq.map {
      filePartials =>
        val first = filePartials.head
        val mergedBitmap = new RoaringBitmapArray()
        filePartials.foreach {
          partial => mergedBitmap.merge(RoaringBitmapArray.readFrom(partial.deletedRowIndexSet))
        }
        mergedBitmap.runOptimize()
        DeletionVectorData(
          filePath = first.filePath,
          deletionVectorId = first.deletionVectorId,
          deletedRowIndexSet = mergedBitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
          deletedRowIndexCount = mergedBitmap.cardinality
        )
    }
  }

  private def mergeDeletionVectorDataIfNeeded(
      partials: Seq[DeletionVectorData]): Seq[DeletionVectorData] = {
    val seenFilePaths = mutable.HashSet.empty[String]
    val hasDuplicateFilePath = partials.exists(partial => !seenFilePaths.add(partial.filePath))

    if (hasDuplicateFilePath) {
      mergeDeletionVectorData(partials)
    } else {
      partials
    }
  }

  private def storeDeletionVectorsOnDriver(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      deletionVectorData: Seq[DeletionVectorData],
      skipUnchangedExistingDvs: Boolean = false): Seq[DeletionVectorResult] = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(sparkSession)
    if (deletionVectorData.isEmpty) {
      GlutenDeltaDeleteTiming.logIfEnabled(
        timingEnabled,
        "storeDeletionVectorsOnDriver totalMs=0 setupMs=0 writeRowsMs=0 rows=0")
      return Seq.empty
    }

    val start = GlutenDeltaDeleteTiming.now()
    val hadoopConf = txn.deltaLog.newDeltaHadoopConf()
    val dvStore = DeletionVectorStore.createInstance(hadoopConf)
    val tablePath = txn.deltaLog.dataPath
    val tablePathWithFS = dvStore.pathWithFileSystem(tablePath)
    val prefix = DeltaUtils.getRandomPrefix(DeltaUtils.getRandomPrefixLength(txn.metadata))
    val (writer, fileId) = DeletionVectorWriter.createWriter(dvStore, tablePathWithFS, prefix)
    val context = DeletionVectorWriter.DeletionVectorMapperContext(
      dvStore = dvStore,
      writer = writer,
      tablePath = tablePath,
      fileId = fileId,
      prefix = prefix)
    val setupMs = GlutenDeltaDeleteTiming.elapsedMs(start)

    var writeRowsMs: Long = 0
    val result = SparkUtils.tryWithResource(writer) {
      _ =>
        val writeRowsStart = GlutenDeltaDeleteTiming.now()
        val stored = deletionVectorData.map {
          row =>
            if (skipUnchangedExistingDvs) {
              storeBitmapAndGenerateResultSkippingUnchangedExistingDv(context, row)
            } else {
              DeletionVectorWriter.storeBitmapAndGenerateResult(context, row)
            }
        }
        writeRowsMs = GlutenDeltaDeleteTiming.elapsedMs(writeRowsStart)
        stored
    }
    GlutenDeltaDeleteTiming.logIfEnabled(
      timingEnabled,
      s"storeDeletionVectorsOnDriver totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
        s"setupMs=$setupMs writeRowsMs=$writeRowsMs rows=${deletionVectorData.size}"
    )
    result
  }

  private def storeBitmapAndGenerateResultSkippingUnchangedExistingDv(
      ctx: DeletionVectorWriter.DeletionVectorMapperContext,
      row: DeletionVectorData): DeletionVectorResult = {
    assert(
      row.filePath != null,
      s"""
         |Encountered a non matched file path.
         |It is likely that _metadata.file_path is not encoded by Spark as expected.
         |""".stripMargin
    )

    row.deletionVectorId match {
      case Some(serializedExistingDv) if row.deletedRowIndexCount > 0 =>
        val existingDvDescriptor =
          DeletionVectorDescriptor.deserializeFromBase64(serializedExistingDv)
        val existingBitmap =
          StoredBitmap.create(existingDvDescriptor, ctx.tablePath).load(ctx.dvStore)
        val previousCardinality = existingBitmap.cardinality
        val newBitmap =
          DeletionVectorUtils.deserialize(row.deletedRowIndexSet, Some(ctx.tablePath))
        existingBitmap.merge(newBitmap)
        val mergedCardinality = existingBitmap.cardinality
        val finalDvDescriptor =
          if (mergedCardinality == previousCardinality) {
            existingDvDescriptor
          } else {
            val serializedBitmap = DeletionVectorUtils.serialize(
              existingBitmap,
              RoaringBitmapArrayFormat.Portable,
              Some(ctx.tablePath),
              debugInfo = Map("existingDvDescriptor" -> existingDvDescriptor))
            DeletionVectorWriter.storeSerializedBitmap(
              ctx,
              serializedBitmap,
              mergedCardinality)
          }
        DeletionVectorResult(
          filePath = row.filePath,
          deletionVector = finalDvDescriptor,
          matchedRowCount = mergedCardinality - previousCardinality)
      case _ =>
        DeletionVectorWriter.storeBitmapAndGenerateResult(ctx, row)
    }
  }
}
