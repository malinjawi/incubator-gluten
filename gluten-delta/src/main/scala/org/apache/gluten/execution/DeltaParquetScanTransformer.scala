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
package org.apache.gluten.execution

import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, Expression, FileSourceConstantMetadataStructField, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.delta.util.DeltaFileOperations.absolutePath
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation, InMemoryFileIndex, NoopCache, PartitionedFile, PartitionSpec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

import java.lang.{Byte => JByte}

/**
 * Native Parquet scan for Delta DML row-index reads that can use the plain Parquet path.
 *
 * For DELETE/UPDATE/MERGE row-index scans over selected Delta files, the Delta reader often adds no
 * value but keeps the scan on the slower Delta connector path. This transformer keeps metadata
 * columns and advertises the physical files as plain Parquet. For unpartitioned scans it also
 * replaces the prepared Delta file index with a plain Parquet file index over the already selected
 * files, avoiding the slower Delta split-planning path.
 */
case class DeltaParquetScanTransformer(
    @transient override val relation: HadoopFsRelation,
    @transient stream: Option[SparkDataStream],
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false,
    override val pushDownFilters: Option[Seq[Expression]] = None,
    constantMetadataColumnValues: Map[String, Object] = Map.empty)
  extends FileSourceScanExecTransformerBase(
    relation,
    stream,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan
  ) {

  override lazy val fileFormat: ReadFileFormat = ReadFileFormat.ParquetReadFormat

  override def getProperties: Map[String, String] = {
    val properties = super.getProperties +
      (DeltaParquetScanTransformer.SkipDeltaMetadataNormalizationKey -> "true")
    if (
      constantMetadataColumnValues.contains(DeltaParquetScanTransformer.DeltaIsRowDeletedColumnName)
    ) {
      properties + (DeltaParquetScanTransformer.TreatDeltaIsRowDeletedAsMetadataKey -> "true")
    } else {
      properties
    }
  }

  override def getPartitions: Seq[Partition] = {
    super.getPartitions.map {
      case filePartition: FilePartition =>
        FilePartition(
          filePartition.index,
          filePartition.files.map {
            DeltaParquetScanTransformer.sanitizePartitionedFile(
              _,
              constantMetadataColumnValues)
          })
      case other => other
    }
  }

  override def doCanonicalize(): DeltaParquetScanTransformer = {
    DeltaParquetScanTransformer(
      relation,
      None,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan,
      pushDownFilters.map(QueryPlan.normalizePredicates(_, output)),
      constantMetadataColumnValues
    )
  }

  override def withNewPushdownFilters(filters: Seq[Expression]): BasicScanExecTransformer =
    copy(pushDownFilters = Some(filters))
}

object DeltaParquetScanTransformer {
  val SkipDeltaMetadataNormalizationKey: String =
    "gluten.delta.skip_metadata_normalization"
  val TreatDeltaIsRowDeletedAsMetadataKey: String =
    "gluten.delta.treat_is_row_deleted_as_metadata"
  val UsePlainFileIndexForDmlRowIndexScanKey: String =
    "spark.gluten.sql.delta.dmlRowIndexScan.usePlainParquetFileIndex"

  val DeltaIsRowDeletedColumnName = "__delta_internal_is_row_deleted"
  private val DeltaKeepRowValue = JByte.valueOf(0.toByte)

  private val DeltaMetadataMarkerKeys: Set[String] =
    Set(
      "table_format",
      "delta_dv_cardinality",
      "delta_dv_payload_index",
      "row_index_filter_id_encoded",
      "row_index_filter_type")

  private def sanitizePartitionedFile(
      file: PartitionedFile,
      constantMetadataColumnValues: Map[String, Object]): PartitionedFile = {
    val metadata =
      (file.otherConstantMetadataColumnValues -- DeltaMetadataMarkerKeys) ++
        constantMetadataColumnValues
    if (metadata == file.otherConstantMetadataColumnValues) {
      file
    } else {
      file.copy(otherConstantMetadataColumnValues = metadata)
    }
  }

  private def selectedDeltaFiles(scanExec: FileSourceScanExec): Option[(String, Seq[AddFile])] = {
    scanExec.relation.location match {
      case index: PreparedDeltaFileIndex =>
        Some(index.path.toString -> index.preparedScan.files)
      case index: TahoeBatchFileIndex =>
        Some(index.path.toString -> index.addFiles)
      case _ => None
    }
  }

  private def shouldUsePlainFileIndex(scanExec: FileSourceScanExec): Boolean = {
    scanExec.relation.sparkSession.sessionState.conf
      .getConfString(UsePlainFileIndexForDmlRowIndexScanKey, "true")
      .toBoolean &&
    scanExec.relation.partitionSchema.isEmpty
  }

  private def maybeCreatePlainParquetRelation(
      scanExec: FileSourceScanExec): Option[(HadoopFsRelation, Boolean)] = {
    if (!shouldUsePlainFileIndex(scanExec)) {
      return None
    }

    selectedDeltaFiles(scanExec).flatMap {
      case (_, files) if files.isEmpty =>
        None
      case (basePath, files) =>
        val spark = scanExec.relation.sparkSession
        val hasDeletionVectors = files.exists(_.deletionVector != null)
        val paths = files.map(add => new Path(absolutePath(basePath, add.path).toString))
        val options = scanExec.relation.options + ("basePath" -> basePath)
        val dataSchema =
          if (hasDeletionVectors) {
            removePlainParquetConstantMetadataColumns(scanExec.relation.dataSchema)
          } else {
            scanExec.relation.dataSchema
          }
        val fileIndex = new InMemoryFileIndex(
          spark,
          paths,
          options,
          Some(dataSchema),
          NoopCache,
          Some(PartitionSpec.emptySpec),
          None)
        Some(
          scanExec.relation.copy(
            location = fileIndex,
            dataSchema = dataSchema,
            fileFormat = new ParquetFileFormat())(spark) -> hasDeletionVectors)
    }
  }

  private def removePlainParquetConstantMetadataColumns(schema: StructType): StructType =
    StructType(schema.fields.filterNot(_.name == DeltaIsRowDeletedColumnName))

  private def markPlainParquetConstantMetadataColumns(output: Seq[Attribute]): Seq[Attribute] =
    output.map {
      case attr: AttributeReference if attr.name == DeltaIsRowDeletedColumnName =>
        attr.withMetadata(FileSourceConstantMetadataStructField.metadata(attr.name))
      case attr => attr
    }

  private def plainParquetConstantMetadataColumnValues(
      scanExec: FileSourceScanExec): Map[String, Object] = {
    if (scanExec.requiredSchema.fieldNames.contains(DeltaIsRowDeletedColumnName)) {
      Map(DeltaIsRowDeletedColumnName -> DeltaKeepRowValue)
    } else {
      Map.empty
    }
  }

  private def isDeltaKeepRowLiteral(literal: Literal): Boolean = {
    literal.value match {
      case number: java.lang.Number => number.longValue() == 0L
      case _ => false
    }
  }

  private def isDeltaIsRowDeletedKeepPredicate(expr: Expression): Boolean = expr match {
    case EqualTo(attr: Attribute, literal: Literal)
        if attr.name == DeltaIsRowDeletedColumnName && isDeltaKeepRowLiteral(literal) =>
      true
    case EqualTo(literal: Literal, attr: Attribute)
        if attr.name == DeltaIsRowDeletedColumnName && isDeltaKeepRowLiteral(literal) =>
      true
    case _ => false
  }

  private def stripDeltaIsRowDeletedKeepPredicate(expr: Expression): Option[Expression] = {
    expr match {
      case And(left, right) =>
        (
          stripDeltaIsRowDeletedKeepPredicate(left),
          stripDeltaIsRowDeletedKeepPredicate(right)
        ) match {
          case (Some(cleanLeft), Some(cleanRight)) => Some(And(cleanLeft, cleanRight))
          case (Some(cleanLeft), None) => Some(cleanLeft)
          case (None, Some(cleanRight)) => Some(cleanRight)
          case (None, None) => None
        }
      case predicate if isDeltaIsRowDeletedKeepPredicate(predicate) =>
        None
      case other =>
        Some(other)
    }
  }

  def apply(scanExec: FileSourceScanExec): BasicScanExecTransformer = {
    maybeCreatePlainParquetRelation(scanExec) match {
      case Some((relation, false)) =>
        FileSourceScanExecTransformer(
          relation,
          SparkShimLoader.getSparkShims.getFileSourceScanStream(scanExec),
          scanExec.output,
          scanExec.requiredSchema,
          scanExec.partitionFilters,
          scanExec.optionalBucketSet,
          scanExec.optionalNumCoalescedBuckets,
          scanExec.dataFilters,
          scanExec.tableIdentifier,
          scanExec.disableBucketedScan
        )
      case Some((relation, true)) =>
        val constantMetadataColumnValues = plainParquetConstantMetadataColumnValues(scanExec)
        val cleanedDataFilters =
          scanExec.dataFilters.flatMap(stripDeltaIsRowDeletedKeepPredicate)
        new DeltaParquetScanTransformer(
          relation,
          SparkShimLoader.getSparkShims.getFileSourceScanStream(scanExec),
          markPlainParquetConstantMetadataColumns(scanExec.output),
          removePlainParquetConstantMetadataColumns(scanExec.requiredSchema),
          scanExec.partitionFilters,
          scanExec.optionalBucketSet,
          scanExec.optionalNumCoalescedBuckets,
          cleanedDataFilters,
          scanExec.tableIdentifier,
          scanExec.disableBucketedScan,
          constantMetadataColumnValues = constantMetadataColumnValues
        )
      case None =>
        new DeltaParquetScanTransformer(
          scanExec.relation,
          SparkShimLoader.getSparkShims.getFileSourceScanStream(scanExec),
          scanExec.output,
          scanExec.requiredSchema,
          scanExec.partitionFilters,
          scanExec.optionalBucketSet,
          scanExec.optionalNumCoalescedBuckets,
          scanExec.dataFilters,
          scanExec.tableIdentifier,
          scanExec.disableBucketedScan
        )
    }
  }
}
