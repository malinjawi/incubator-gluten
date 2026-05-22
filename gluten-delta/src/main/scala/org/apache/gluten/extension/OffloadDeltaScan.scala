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
package org.apache.gluten.extension

import org.apache.gluten.execution.{DeltaParquetScanTransformer, DeltaScanTransformer}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

import org.apache.spark.sql.delta.{DeltaParquetFileFormat, NoMapping, SnapshotDescriptor}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.deletionVectorsReadable
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.util.SparkVersionUtil

import scala.util.control.NonFatal

case class OffloadDeltaScan() extends OffloadSingleNode {
  private val enableNativeDeletionVectorDmlRowIndexScanKey =
    "spark.gluten.sql.delta.enableNativeDmlRowIndexScan"
  private val enableNativeDeletionVectorBitmapAggregationKey =
    "spark.gluten.sql.delta.delete.dv.enableNativeBitmapAggregation"
  private val enableNativeDeltaWriteKey =
    "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite"

  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec if isDeltaLogScan(scan) =>
      FallbackTags.add(scan, "fallback Delta _delta_log scan")
      scan
    case scan: FileSourceScanExec if shouldFallbackSpark34DeletionVectorScan(scan) =>
      FallbackTags.add(scan, "fallback Spark 3.4 Delta DV scan")
      scan
    case scan: FileSourceScanExec if shouldFallbackDeletionVectorDmlScan(scan) =>
      FallbackTags.add(scan, "fallback Delta DV DML row-index scan")
      scan
    case scan: FileSourceScanExec if shouldUseNativeParquetDmlRowIndexScan(scan) =>
      DeltaParquetScanTransformer(scan)
    case scan: FileSourceScanExec if isDeltaScan(scan) =>
      DeltaScanTransformer(scan)
    case other => other
  }

  private def isDeltaScan(scan: FileSourceScanExec): Boolean = {
    DeltaDeletionVectorDmlUtils.isDeltaScan(scan)
  }

  private def isDeltaLogScan(scan: FileSourceScanExec): Boolean = {
    scan.relation.location.rootPaths.exists {
      path =>
        val root = path.toString
        root.contains("/_delta_log") || root.contains("\\_delta_log") || root.endsWith("_delta_log")
    }
  }

  private def shouldUseNativeParquetDmlRowIndexScan(scan: FileSourceScanExec): Boolean = {
    val hasDeletionVectors = scanHasDeletionVectors(scan)
    DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(scan) &&
    !hasColumnMapping(scan) &&
    (!hasDeletionVectors || canUseNativeParquetDmlRowIndexScanWithExistingDvs(scan))
  }

  private def canUseNativeParquetDmlRowIndexScanWithExistingDvs(
      scan: FileSourceScanExec): Boolean = {
    val nativeBitmapAggregationEnabled =
      scan.relation.sparkSession.sessionState.conf
        .getConfString(enableNativeDeletionVectorBitmapAggregationKey, "false")
        .toBoolean

    nativeBitmapAggregationEnabled &&
    scan.relation.location.isInstanceOf[TahoeBatchFileIndex] &&
    scan.relation.partitionSchema.isEmpty
  }

  private def hasColumnMapping(scan: FileSourceScanExec): Boolean = {
    scan.relation.fileFormat match {
      case format: DeltaParquetFileFormat => format.columnMappingMode != NoMapping
      case format if format.getClass.getSimpleName == "GlutenDeltaParquetFileFormat" =>
        try {
          val columnMappingMode = format.getClass.getMethod("columnMappingMode").invoke(format)
          columnMappingMode != null && columnMappingMode.toString != NoMapping.name
        } catch {
          case NonFatal(_) => true
        }
      case _ => false
    }
  }

  private def scanHasDeletionVectors(scan: FileSourceScanExec): Boolean = {
    scan.relation.location match {
      case preparedIndex: PreparedDeltaFileIndex =>
        preparedIndex.preparedScan.files.exists(_.deletionVector != null)
      case batchIndex: TahoeBatchFileIndex =>
        batchIndex.addFiles.exists(_.deletionVector != null)
      case _ =>
        hasDeletionVectorsFromFileFormat(scan)
    }
  }

  private def hasDeletionVectorsFromFileFormat(scan: FileSourceScanExec): Boolean = {
    try {
      val format = scan.relation.fileFormat
      val broadcastDvMap = Option(format.getClass.getMethod("broadcastDvMap").invoke(format))
        .collect { case o: Option[_] => o }
        .flatten
        .collect { case b: org.apache.spark.broadcast.Broadcast[_] => b.value }
        .collect { case m: scala.collection.Map[_, _] => m }
        .getOrElse(Map.empty)
      broadcastDvMap.nonEmpty
    } catch {
      case _: NoSuchMethodException => false
      case NonFatal(_) => true
    }
  }

  private def shouldFallbackDeletionVectorDmlScan(scan: FileSourceScanExec): Boolean = {
    val enableNativeDeltaWrite =
      scan.relation.sparkSession.sessionState.conf
        .getConfString(enableNativeDeltaWriteKey, "false")
        .toBoolean
    val enableNativeDmlRowIndexScan =
      scan.relation.sparkSession.sessionState.conf
        .getConfString(enableNativeDeletionVectorDmlRowIndexScanKey, "false")
        .toBoolean
    if (enableNativeDeltaWrite && enableNativeDmlRowIndexScan) {
      return false
    }

    // DELETE/UPDATE/MERGE with persistent deletion vectors needs the target scan to expose
    // per-file row indexes so Delta can build updated DV bitmaps. Gluten can scan the target
    // natively, but the bitmap aggregation/update itself still runs in Spark today. Keeping this
    // scan on Spark avoids a native scan plus row transition immediately before Spark's DV
    // aggregation path.
    DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(scan)
  }

  private def shouldFallbackSpark34DeletionVectorScan(scan: FileSourceScanExec): Boolean = {
    if (SparkVersionUtil.gteSpark35) {
      return false
    }

    scan.relation.location match {
      case preparedIndex: PreparedDeltaFileIndex =>
        preparedIndex.preparedScan.files.exists(_.deletionVector != null)
      case index: TahoeFileIndex =>
        val snapshot = index.asInstanceOf[SnapshotDescriptor]
        deletionVectorsReadable(snapshot.protocol, snapshot.metadata)
      case _ =>
        false
    }
  }
}
