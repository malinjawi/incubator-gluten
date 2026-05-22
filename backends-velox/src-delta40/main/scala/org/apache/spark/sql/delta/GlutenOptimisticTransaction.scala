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

import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.extension.columnar.transition.Transitions

import org.apache.spark.sql.{AnalysisException, Dataset}
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.GlutenDeltaDeleteTiming
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints, DeltaInvariantCheckerExec}
import org.apache.spark.sql.delta.coordinatedcommits.TableCommitCoordinatorClient
import org.apache.spark.sql.delta.files.{GlutenDeltaFileFormatWriter, TransactionalWrite}
import org.apache.spark.sql.delta.hooks.{AutoCompact, ChecksumHook, PostCommitHook}
import org.apache.spark.sql.delta.perf.{DeltaOptimizedWriterExec, GlutenDeltaOptimizedWriterExec}
import org.apache.spark.sql.delta.schema.InnerInvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ScalaExtensions.OptionExt
import org.apache.spark.util.SerializableConfiguration

import io.delta.storage.commit.Commit

import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

object GlutenOptimisticTransaction {
  val SkipAllFilesInCrcForDvDeleteKey: String =
    "spark.gluten.sql.delta.delete.dv.checksum.skipAllFilesInCrc"
  val SkipChecksumForDvDeleteKey: String =
    "spark.gluten.sql.delta.delete.dv.checksum.skipWrite"
}

class GlutenOptimisticTransaction(delegate: OptimisticTransaction)
  extends OptimisticTransaction(
    delegate.deltaLog,
    delegate.catalogTable,
    delegate.snapshot
  ) {

  override def commit(actions: Seq[Action], op: DeltaOperations.Operation): Long = {
    withChecksumConfsForDvDelete(actions, op) {
      super.commit(actions, op)
    }
  }

  override def commit(
      actions: Seq[Action],
      op: DeltaOperations.Operation,
      tags: Map[String, String]): Long = {
    withChecksumConfsForDvDelete(actions, op) {
      super.commit(actions, op, tags)
    }
  }

  override def commitIfNeeded(
      actions: Seq[Action],
      op: DeltaOperations.Operation,
      tags: Map[String, String]): Option[Long] = {
    withChecksumConfsForDvDelete(actions, op) {
      super.commitIfNeeded(actions, op, tags)
    }
  }

  override protected def incrementallyDeriveChecksum(
      attemptVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo): Option[VersionChecksum] = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(spark)
    val start = GlutenDeltaDeleteTiming.now()
    val skipChecksum = shouldSkipChecksumForDvDelete(currentTransactionInfo)
    val skipAllFilesInCrc = shouldSkipAllFilesInCrcForDvDelete(currentTransactionInfo)
    val mode =
      if (skipChecksum) "skipChecksum"
      else if (skipAllFilesInCrc) "skipAllFilesInCrc"
      else "default"

    val result =
      if (skipChecksum) {
        None
      } else if (!skipAllFilesInCrc) {
        super.incrementallyDeriveChecksum(attemptVersion, currentTransactionInfo)
      } else {
        incrementallyDeriveChecksum(
          spark,
          deltaLog,
          attemptVersion,
          actions = currentTransactionInfo.finalActionsToCommit,
          metadataOpt = Some(currentTransactionInfo.metadata),
          protocolOpt = Some(currentTransactionInfo.protocol),
          operationName = currentTransactionInfo.op.name,
          txnIdOpt = Some(currentTransactionInfo.txnId),
          previousVersionState = scala.Left(snapshot),
          includeAddFilesInCrc = false
        ).toOption
      }

    GlutenDeltaDeleteTiming.logIfEnabled(
      timingEnabled,
      s"transactionIncrementalChecksum totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
        s"mode=$mode result=${if (result.isDefined) "checksum" else "none"} " +
        s"attemptVersion=$attemptVersion operation=${currentTransactionInfo.op.name} " +
        actionSummary(currentTransactionInfo.finalActionsToCommit)
    )
    result
  }

  override protected def doCommit(
      attemptVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo,
      attemptNumber: Int,
      isolationLevel: IsolationLevel): Snapshot = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(spark)
    val start = GlutenDeltaDeleteTiming.now()
    try {
      super.doCommit(attemptVersion, currentTransactionInfo, attemptNumber, isolationLevel)
    } finally {
      GlutenDeltaDeleteTiming.logIfEnabled(
        timingEnabled,
        s"transactionDoCommit totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
          s"attemptVersion=$attemptVersion attemptNumber=$attemptNumber " +
          s"operation=${currentTransactionInfo.op.name} isolationLevel=$isolationLevel " +
          actionSummary(currentTransactionInfo.finalActionsToCommit)
      )
    }
  }

  override protected def writeCommitFile(
      attemptVersion: Long,
      jsonActions: Iterator[String],
      currentTransactionInfo: CurrentTransactionInfo): (Option[VersionChecksum], Commit) = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(spark)
    val start = GlutenDeltaDeleteTiming.now()
    val result = super.writeCommitFile(attemptVersion, jsonActions, currentTransactionInfo)
    GlutenDeltaDeleteTiming.logIfEnabled(
      timingEnabled,
      s"transactionWriteCommitFile totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
        s"attemptVersion=$attemptVersion operation=${currentTransactionInfo.op.name} " +
        s"checksum=${result._1.isDefined} commitVersion=${result._2.getVersion} " +
        actionSummary(currentTransactionInfo.finalActionsToCommit)
    )
    result
  }

  override protected def writeCommitFileImpl(
      attemptVersion: Long,
      jsonActions: Iterator[String],
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      currentTransactionInfo: CurrentTransactionInfo): Commit = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(spark)
    val start = GlutenDeltaDeleteTiming.now()
    val result =
      super.writeCommitFileImpl(
        attemptVersion,
        jsonActions,
        tableCommitCoordinatorClient,
        currentTransactionInfo)
    GlutenDeltaDeleteTiming.logIfEnabled(
      timingEnabled,
      s"transactionCommitFileImpl totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
        s"attemptVersion=$attemptVersion operation=${currentTransactionInfo.op.name} " +
        s"commitVersion=${result.getVersion} " +
        actionSummary(currentTransactionInfo.finalActionsToCommit)
    )
    result
  }

  override protected def runPostCommitHook(
      hook: PostCommitHook,
      committedTransaction: CommittedTransaction): Unit = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(spark)
    val start = GlutenDeltaDeleteTiming.now()
    try {
      if (timingEnabled && hook == ChecksumHook) {
        runChecksumHookWithTiming(committedTransaction)
      } else {
        super.runPostCommitHook(hook, committedTransaction)
      }
    } finally {
      GlutenDeltaDeleteTiming.logIfEnabled(
        timingEnabled,
        s"transactionPostCommitHook totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
          s"hook=${hook.name} version=${committedTransaction.committedVersion} " +
          actionSummary(committedTransaction.committedActions)
      )
    }
  }

  private def runChecksumHookWithTiming(committedTransaction: CommittedTransaction): Unit = {
    val totalStart = GlutenDeltaDeleteTiming.now()
    val committedVersion = committedTransaction.committedVersion
    val postCommitSnapshot = committedTransaction.postCommitSnapshot
    val allFilesInCrcEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED)
    val checksumWriteEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED)
    val checksumDvMetricsEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHECKSUM_DV_METRICS_ENABLED)
    val deletedRecordCountsHistogramEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED)

    if (postCommitSnapshot.version != committedVersion) {
      GlutenDeltaDeleteTiming.logIfEnabled(
        enabled = true,
        s"transactionChecksumHookDetail status=skippedVersionMismatch " +
          s"totalMs=${GlutenDeltaDeleteTiming.elapsedMs(totalStart)} " +
          s"snapshotVersion=${postCommitSnapshot.version} committedVersion=$committedVersion " +
          s"allFilesInCrcEnabled=$allFilesInCrcEnabled " +
          s"checksumWriteEnabled=$checksumWriteEnabled " +
          s"checksumDvMetricsEnabled=$checksumDvMetricsEnabled " +
          s"deletedRecordCountsHistogramEnabled=$deletedRecordCountsHistogramEnabled"
      )
      return
    }

    if (!checksumWriteEnabled) {
      GlutenDeltaDeleteTiming.logIfEnabled(
        enabled = true,
        s"transactionChecksumHookDetail status=writeDisabled " +
          s"totalMs=${GlutenDeltaDeleteTiming.elapsedMs(totalStart)} " +
          s"snapshotVersion=${postCommitSnapshot.version} committedVersion=$committedVersion " +
          s"allFilesInCrcEnabled=$allFilesInCrcEnabled " +
          s"checksumWriteEnabled=$checksumWriteEnabled " +
          s"checksumDvMetricsEnabled=$checksumDvMetricsEnabled " +
          s"deletedRecordCountsHistogramEnabled=$deletedRecordCountsHistogramEnabled"
      )
      return
    }

    val version = postCommitSnapshot.version
    val eventData = mutable.Map[String, Any]("operationSucceeded" -> false)
    var status = "failed"
    var computeChecksumMs = -1L
    var jsonSerializationMs = -1L
    var writerCreateMs = -1L
    var createAtomicMs = -1L
    var writeCloseMs = -1L
    var recordEventMs = -1L
    var allFilesCount = -1
    var setTransactionsCount = -1
    var checksumLength = -1

    try {
      val computeStart = GlutenDeltaDeleteTiming.now()
      val checksumWithoutTxnId = postCommitSnapshot.computeChecksum
      computeChecksumMs = GlutenDeltaDeleteTiming.elapsedMs(computeStart)
      val checksum = checksumWithoutTxnId.copy(txnId = Some(committedTransaction.txnId))
      allFilesCount = checksum.allFiles.map(_.size).getOrElse(-1)
      setTransactionsCount = checksum.setTransactions.map(_.size).getOrElse(-1)
      eventData("numAddFileActions") = allFilesCount
      eventData("numSetTransactionActions") = setTransactionsCount

      val checksumWriteStartMs = System.currentTimeMillis()
      val jsonStart = GlutenDeltaDeleteTiming.now()
      val toWrite = JsonUtils.toJson(checksum) + "\n"
      jsonSerializationMs = GlutenDeltaDeleteTiming.elapsedMs(jsonStart)
      checksumLength = toWrite.length
      eventData("jsonSerializationTimeTakenMs") = jsonSerializationMs
      eventData("checksumLength") = checksumLength

      val writerStart = GlutenDeltaDeleteTiming.now()
      val writer = CheckpointFileManager.create(deltaLog.logPath, deltaLog.newDeltaHadoopConf())
      writerCreateMs = GlutenDeltaDeleteTiming.elapsedMs(writerStart)

      val createStart = GlutenDeltaDeleteTiming.now()
      val stream = writer.createAtomic(
        FileNames.checksumFile(deltaLog.logPath, version),
        overwriteIfPossible = false)
      createAtomicMs = GlutenDeltaDeleteTiming.elapsedMs(createStart)
      try {
        val writeStart = GlutenDeltaDeleteTiming.now()
        stream.write(toWrite.getBytes(UTF_8))
        stream.close()
        writeCloseMs = GlutenDeltaDeleteTiming.elapsedMs(writeStart)
        eventData("overallTimeTakenMs") = System.currentTimeMillis() - checksumWriteStartMs
        eventData("operationSucceeded") = true
        status = "written"
      } catch {
        case NonFatal(e) =>
          status = "failedWrite"
          logWarning(s"Failed to write the checksum for version: $version", e)
          try {
            stream.cancel()
          } catch {
            case NonFatal(cancelError) =>
              logWarning(s"Failed to cancel checksum write for version: $version", cancelError)
          }
      }
    } catch {
      case NonFatal(e) =>
        status = "failedSetup"
        logWarning(s"Failed to write the checksum for version: $version", e)
    } finally {
      val recordEventStart = GlutenDeltaDeleteTiming.now()
      recordDeltaEvent(
        deltaLog,
        opType = "delta.checksum.write",
        data = eventData)
      recordEventMs = GlutenDeltaDeleteTiming.elapsedMs(recordEventStart)
      GlutenDeltaDeleteTiming.logIfEnabled(
        enabled = true,
        s"transactionChecksumHookDetail status=$status " +
          s"totalMs=${GlutenDeltaDeleteTiming.elapsedMs(totalStart)} " +
          s"computeChecksumMs=$computeChecksumMs jsonSerializationMs=$jsonSerializationMs " +
          s"writerCreateMs=$writerCreateMs createAtomicMs=$createAtomicMs " +
          s"writeCloseMs=$writeCloseMs recordEventMs=$recordEventMs " +
          s"allFilesCount=$allFilesCount setTransactionsCount=$setTransactionsCount " +
          s"checksumLength=$checksumLength snapshotVersion=$version " +
          s"committedVersion=$committedVersion " +
          s"allFilesInCrcEnabled=$allFilesInCrcEnabled " +
          s"checksumWriteEnabled=$checksumWriteEnabled " +
          s"checksumDvMetricsEnabled=$checksumDvMetricsEnabled " +
          s"deletedRecordCountsHistogramEnabled=$deletedRecordCountsHistogramEnabled"
      )
    }
  }

  private def shouldSkipAllFilesInCrcForDvDelete(
      currentTransactionInfo: CurrentTransactionInfo): Boolean = {
    shouldApplyDvDeleteChecksumConf(
      currentTransactionInfo.finalActionsToCommit,
      currentTransactionInfo.op,
      GlutenOptimisticTransaction.SkipAllFilesInCrcForDvDeleteKey)
  }

  private def shouldSkipChecksumForDvDelete(
      currentTransactionInfo: CurrentTransactionInfo): Boolean = {
    shouldApplyDvDeleteChecksumConf(
      currentTransactionInfo.finalActionsToCommit,
      currentTransactionInfo.op,
      GlutenOptimisticTransaction.SkipChecksumForDvDeleteKey)
  }

  private def withChecksumConfsForDvDelete[T](
      actions: Seq[Action],
      op: DeltaOperations.Operation)(f: => T): T = {
    val skipAllFilesInCrc = shouldApplyDvDeleteChecksumConf(
      actions,
      op,
      GlutenOptimisticTransaction.SkipAllFilesInCrcForDvDeleteKey)
    val skipChecksum = shouldApplyDvDeleteChecksumConf(
      actions,
      op,
      GlutenOptimisticTransaction.SkipChecksumForDvDeleteKey)
    if (!skipAllFilesInCrc && !skipChecksum) {
      return f
    }

    val keysToDisable =
      Seq(
        Option.when(skipAllFilesInCrc)(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key),
        Option.when(skipChecksum)(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key)
      ).flatten
    val previousValues = keysToDisable.map(key => key -> spark.conf.getOption(key))
    keysToDisable.foreach(key => spark.conf.set(key, "false"))
    try {
      f
    } finally {
      previousValues.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  private def shouldApplyDvDeleteChecksumConf(
      actions: Seq[Action],
      op: DeltaOperations.Operation,
      key: String): Boolean = {
    val enabled = spark.sessionState.conf
      .getConfString(key, "false")
      .toBoolean
    enabled &&
    op.name == "DELETE" &&
    actions.exists {
      case add: AddFile => add.deletionVector != null
      case remove: RemoveFile => remove.deletionVector != null
      case _ => false
    }
  }

  private def actionSummary(actions: Seq[Action]): String = {
    var addFiles = 0
    var removeFiles = 0
    var dvAddFiles = 0
    var dvRemoveFiles = 0
    actions.foreach {
      case add: AddFile =>
        addFiles += 1
        if (add.deletionVector != null) {
          dvAddFiles += 1
        }
      case remove: RemoveFile =>
        removeFiles += 1
        if (remove.deletionVector != null) {
          dvRemoveFiles += 1
        }
      case _ =>
    }
    s"actions=${actions.size} addFiles=$addFiles removeFiles=$removeFiles " +
      s"dvAddFiles=$dvAddFiles dvRemoveFiles=$dvRemoveFiles"
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      isOptimize: Boolean,
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    hasWritten = true

    val spark = inputData.sparkSession

    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints, trackFromData) =
      normalizeData(deltaLog, writeOptions, data)
    // Use the track set from the transaction if set,
    // otherwise use the track set from `normalizeData()`.
    val trackIdentityHighWaterMarks = trackHighWaterMarks.getOrElse(trackFromData)

    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = getCommitter(outputPath)

    val (statsDataSchema, _) = getStatsSchema(output, partitionSchema)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    val optionalStatsTracker =
      getOptionalStatsTrackerAndStatsCollection(output, outputPath, partitionSchema, data)._1

    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    val identityTrackerOpt = IdentityColumn
      .createIdentityColumnStatsTracker(
        spark,
        deltaLog.newDeltaHadoopConf(),
        outputPath,
        metadata.schema,
        statsDataSchema,
        trackIdentityHighWaterMarks
      )

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(outputPath.toString, Map.empty, output)

      val empty2NullPlan =
        convertEmptyToNullIfNeeded(queryExecution.executedPlan, partitioningColumns, constraints)
      val maybeCheckInvariants = if (constraints.isEmpty) {
        // Compared to vanilla Delta, we simply avoid adding the invariant checker
        // when the constraint list is empty, to prevent the unnecessary transitions
        // from being added around the invariant checker.
        empty2NullPlan
      } else {
        DeltaInvariantCheckerExec(spark, empty2NullPlan, constraints)
      }
      def toVeloxPlan(plan: SparkPlan): SparkPlan = plan match {
        case aqe: AdaptiveSparkPlanExec =>
          assert(!aqe.isFinalPlan)
          aqe.copy(supportsColumnar = true)
        case _ => Transitions.toBatchPlan(maybeCheckInvariants, VeloxBatchType)
      }
      // No need to plan optimized write if the write command is OPTIMIZE, which aims to produce
      // evenly-balanced data files already.
      val physicalPlan =
        if (
          !isOptimize &&
          shouldOptimizeWrite(writeOptions, spark.sessionState.conf)
        ) {
          // We uniformly convert the query plan to a columnar plan. If
          // the further write operation turns out to be non-offload-able, the
          // columnar plan will be converted back to a row-based plan.
          val veloxPlan = toVeloxPlan(maybeCheckInvariants)
          try {
            val glutenWriterExec =
              GlutenDeltaOptimizedWriterExec(veloxPlan, metadata.partitionColumns, deltaLog)
            val validationResult = glutenWriterExec.doValidate()
            if (validationResult.ok()) {
              glutenWriterExec
            } else {
              logInfo(
                s"GlutenDeltaOptimizedWriterExec: Internal shuffle validated negative," +
                  s" reason: ${validationResult.reason()}. Falling back to row-based shuffle.")
              DeltaOptimizedWriterExec(maybeCheckInvariants, metadata.partitionColumns, deltaLog)
            }
          } catch {
            case e: AnalysisException =>
              logWarning(
                s"GlutenDeltaOptimizedWriterExec: Failed to create internal shuffle," +
                  s" reason: ${e.getMessage()}. Falling back to row-based shuffle.")
              DeltaOptimizedWriterExec(maybeCheckInvariants, metadata.partitionColumns, deltaLog)
          }
        } else {
          val veloxPlan = toVeloxPlan(maybeCheckInvariants)
          veloxPlan
        }

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      // Iceberg spec requires partition columns in data files
      val writePartitionColumns = IcebergCompat.isAnyEnabled(metadata)
      // Retain only a minimal selection of Spark writer options to avoid any potential
      // compatibility issues
      val options =
        (writeOptions match {
          case None => Map.empty[String, String]
          case Some(writeOptions) =>
            writeOptions.options.filterKeys {
              key =>
                key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
                key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
            }.toMap
        }) + (DeltaOptions.WRITE_PARTITION_COLUMNS -> writePartitionColumns.toString)

      try {
        GlutenDeltaFileFormatWriter.write(
          sparkSession = spark,
          plan = physicalPlan,
          fileFormat = new GlutenDeltaParquetFileFormat(
            protocol,
            metadata
          ), // This is changed to Gluten's Delta format.
          committer = committer,
          outputSpec = outputSpec,
          // scalastyle:off deltahadoopconfiguration
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          // scalastyle:on deltahadoopconfiguration
          partitionColumns = partitioningColumns,
          bucketSpec = None,
          statsTrackers =
            optionalStatsTracker.toSeq
              ++ statsTrackers
              ++ identityTrackerOpt.toSeq,
          options = options
        )
      } catch {
        case InnerInvariantViolationException(violationException) =>
          // Pull an InvariantViolationException up to the top level if it was the root cause.
          throw violationException
      }
      statsTrackers.foreach {
        case tracker: BasicWriteJobStatsTracker =>
          val numOutputRowsOpt = tracker.driverSideMetrics.get("numOutputRows").map(_.value)
          IdentityColumn.logTableWrite(snapshot, trackIdentityHighWaterMarks, numOutputRowsOpt)
        case _ => ()
      }
    }

    var resultFiles =
      (if (optionalStatsTracker.isDefined) {
         committer.addedStatuses.map {
           a =>
             a.copy(stats = optionalStatsTracker
               .map(_.recordedStats(a.toPath.getName))
               .getOrElse(a.stats))
         }
       } else {
         committer.addedStatuses
       })
        .filter {
          // In some cases, we can write out an empty `inputData`. Some examples of this (though, they
          // may be fixed in the future) are the MERGE command when you delete with empty source, or
          // empty target, or on disjoint tables. This is hard to catch before the write without
          // collecting the DF ahead of time. Instead, we can return only the AddFiles that
          // a) actually add rows, or
          // b) don't have any stats so we don't know the number of rows at all
          case a: AddFile => a.numLogicalRecords.forall(_ > 0)
          case _ => true
        }

    // add [[AddFile.Tags.ICEBERG_COMPAT_VERSION.name]] tags to addFiles
    if (IcebergCompatV2.isEnabled(metadata)) {
      resultFiles = resultFiles.map {
        addFile =>
          val tags = if (addFile.tags != null) addFile.tags else Map.empty[String, String]
          addFile.copy(tags = tags + (AddFile.Tags.ICEBERG_COMPAT_VERSION.name -> "2"))
      }
    }

    if (resultFiles.nonEmpty && !isOptimize) registerPostCommitHook(AutoCompact)
    // Record the updated high water marks to be used during transaction commit.
    identityTrackerOpt.ifDefined {
      tracker => updatedIdentityHighWaterMarks.appendAll(tracker.highWaterMarks.toSeq)
    }

    resultFiles.toSeq ++ committer.changeFiles
  }

  private def shouldOptimizeWrite(
      writeOptions: Option[DeltaOptions],
      sessionConf: SQLConf): Boolean = {
    writeOptions
      .flatMap(_.optimizeWrite)
      .getOrElse(TransactionalWrite.shouldOptimizeWrite(metadata, sessionConf))
  }
}
