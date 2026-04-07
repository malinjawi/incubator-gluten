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

import org.apache.gluten.execution.{DeltaScanTransformer, FilterExecTransformerBase, ProjectExecTransformer, ProjectExecTransformerBase}
import org.apache.gluten.extension.columnar.transition.{ColumnarToRowLike, RowToColumnarLike}
import org.apache.gluten.extension.columnar.transition.RemoveTransitions
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualTo, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, Literal}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{ColumnarInputAdapter, FileSourceScanExec, FilterExec, InputIteratorTransformer, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

object DeltaPostTransformRules {
  def postRules: Seq[Rule[SparkPlan]] =
    RemoveTransitions ::
      pushDownInputFileExprRule ::
      columnMappingRule ::
      nativeDeletionVectorRule ::
      Nil

  private val DELTA_SKIP_ROW_COLUMN = "__delta_internal_is_row_deleted"
  private val DELTA_ROW_INDEX_COLUMN = "__delta_internal_row_index"
  private val DeltaParquetFormatNames =
    Set("DeltaParquetFileFormat", "GlutenDeltaParquetFileFormat")
  private val NoMappingClassName = "org.apache.spark.sql.delta.NoMapping$"
  private val DeltaColumnMappingObjectClassName = "org.apache.spark.sql.delta.DeltaColumnMapping$"

  private val COLUMN_MAPPING_RULE_TAG: TreeNodeTag[String] =
    TreeNodeTag[String]("org.apache.gluten.delta.column.mapping")

  private def notAppliedColumnMappingRule(plan: SparkPlan): Boolean = {
    plan.getTagValue(COLUMN_MAPPING_RULE_TAG).isEmpty
  }

  private def tagColumnMappingRule(plan: SparkPlan): Unit = {
    plan.setTagValue(COLUMN_MAPPING_RULE_TAG, null)
  }

  val columnMappingRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformWithSubqueries {
      // If it enables Delta Column Mapping(e.g. nameMapping and idMapping),
      // transform the metadata of Delta into Parquet's,
      // so that gluten can read Delta File using Parquet Reader.
      case p: DeltaScanTransformer
          if isDeltaColumnMappingFileFormat(p.relation.fileFormat) && notAppliedColumnMappingRule(
            p) =>
        transformColumnMappingPlan(p)
    }

  val pushDownInputFileExprRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case p @ ProjectExec(projectList, child: DeltaScanTransformer)
          if projectList.exists(containsInputFileRelatedExpr) =>
        child.copy(output = p.output)
    }

  val nativeDeletionVectorRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case scan: DeltaScanTransformer
          if hasNativeDeletionVectorFilter(scan.dataFilters) ||
            scan.pushDownFilters.exists(hasNativeDeletionVectorFilter) =>
        rewriteDeletionVectorScan(scan.output, scan)
      case scan: FileSourceScanExec
          if isDeltaFileSourceScan(scan) && hasNativeDeletionVectorFilter(scan.dataFilters) =>
        rewriteDeletionVectorScan(scan.output, scan)
      case p: ProjectExec =>
        findNativeDeletionVectorScan(p.child) match {
          case Some(scan) =>
            rewriteDeletionVectorScan(p.output, scan)
          case None => p
        }
      case p: ProjectExecTransformerBase =>
        findNativeDeletionVectorScan(p.input) match {
          case Some(scan) =>
            rewriteDeletionVectorScan(p.output, scan)
          case None => p
        }
    }

  private def hasNativeDeletionVectorFilter(filters: Seq[Expression]): Boolean = {
    filters.exists(isNativeDeletionVectorFilter)
  }

  private def isDeltaLikeFileFormat(fileFormat: FileFormat): Boolean = {
    DeltaParquetFormatNames.contains(fileFormat.getClass.getSimpleName)
  }

  private def isDeltaColumnMappingFileFormat(fileFormat: FileFormat): Boolean = {
    isDeltaLikeFileFormat(fileFormat) &&
    invokeNoArg(fileFormat, "columnMappingMode")
      .exists(_.getClass.getName != NoMappingClassName)
  }

  private def isDeltaFileSourceScan(scan: FileSourceScanExec): Boolean =
    isDeltaLikeFileFormat(scan.relation.fileFormat) && !isDeltaLogScan(scan)

  private def isDeltaLogScan(scan: FileSourceScanExec): Boolean = {
    scan.relation.location.rootPaths.exists {
      path =>
        val root = path.toString
        root.contains("/_delta_log") || root.contains("\\_delta_log") || root.endsWith("_delta_log")
    }
  }

  private def containsInputFileRelatedExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => expr.children.exists(containsInputFileRelatedExpr)
    }
  }

  private def isNativeDeletionVectorFilter(condition: Expression): Boolean = {
    condition match {
      case And(left, right) =>
        isNativeDeletionVectorFilter(left) || isNativeDeletionVectorFilter(right)
      case EqualTo(attribute: AttributeReference, Literal(value, _))
          if attribute.name == DELTA_SKIP_ROW_COLUMN =>
        isKeepRowLiteral(value)
      case EqualTo(Literal(value, _), attribute: AttributeReference)
          if attribute.name == DELTA_SKIP_ROW_COLUMN =>
        isKeepRowLiteral(value)
      case _ =>
        false
    }
  }

  private def unwrapDeltaScan(plan: SparkPlan): Option[SparkPlan] = plan match {
    case scan: DeltaScanTransformer => Some(scan)
    case scan: FileSourceScanExec if isDeltaFileSourceScan(scan) => Some(scan)
    case ProjectExec(_, child) => unwrapDeltaScan(child)
    case project: ProjectExecTransformerBase => unwrapDeltaScan(project.input)
    case InputIteratorTransformer(child) => unwrapDeltaScan(child)
    case ColumnarInputAdapter(child) => unwrapDeltaScan(child)
    case RowToColumnarLike(child) => unwrapDeltaScan(child)
    case ColumnarToRowLike(child) => unwrapDeltaScan(child)
    case _ => None
  }

  private def findNativeDeletionVectorScan(plan: SparkPlan): Option[SparkPlan] =
    plan match {
      case FilterExec(condition, child) if isNativeDeletionVectorFilter(condition) =>
        unwrapDeltaScan(child)
      case filter: FilterExecTransformerBase if isNativeDeletionVectorFilter(filter.cond) =>
        unwrapDeltaScan(filter.input)
      case ProjectExec(_, child) =>
        findNativeDeletionVectorScan(child)
      case project: ProjectExecTransformerBase =>
        findNativeDeletionVectorScan(project.input)
      case InputIteratorTransformer(child) =>
        findNativeDeletionVectorScan(child)
      case ColumnarInputAdapter(child) =>
        findNativeDeletionVectorScan(child)
      case RowToColumnarLike(child) =>
        findNativeDeletionVectorScan(child)
      case ColumnarToRowLike(child) =>
        findNativeDeletionVectorScan(child)
      case _ =>
        None
    }

  private def isKeepRowLiteral(value: Any): Boolean = {
    value match {
      case number: java.lang.Number => number.longValue() == 0L
      case _ => false
    }
  }

  private def pruneDeletionVectorSchema(requiredSchema: StructType): StructType = {
    SparkShimLoader.getSparkShims.structFromAttributes(
      SparkShimLoader.getSparkShims.attributesFromStruct(requiredSchema).filterNot {
        attribute =>
          attribute.name == DELTA_SKIP_ROW_COLUMN ||
          attribute.name == DELTA_ROW_INDEX_COLUMN ||
          attribute.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
      })
  }

  private def stripNativeDeletionVectorFilter(condition: Expression): Option[Expression] = {
    condition match {
      case And(left, right) =>
        (stripNativeDeletionVectorFilter(left), stripNativeDeletionVectorFilter(right)) match {
          case (Some(newLeft), Some(newRight)) => Some(And(newLeft, newRight))
          case (Some(newLeft), None) => Some(newLeft)
          case (None, Some(newRight)) => Some(newRight)
          case (None, None) => None
        }
      case EqualTo(attribute: AttributeReference, Literal(value, _))
          if attribute.name == DELTA_SKIP_ROW_COLUMN && isKeepRowLiteral(value) =>
        None
      case EqualTo(Literal(value, _), attribute: AttributeReference)
          if attribute.name == DELTA_SKIP_ROW_COLUMN && isKeepRowLiteral(value) =>
        None
      case other =>
        Some(other)
    }
  }

  private def stripNativeDeletionVectorFilters(filters: Seq[Expression]): Seq[Expression] = {
    filters.flatMap(stripNativeDeletionVectorFilter)
  }

  private def rewriteDeletionVectorScan(output: Seq[Attribute], child: SparkPlan): SparkPlan = {
    child match {
      case scan: DeltaScanTransformer =>
        val rewrittenScan =
          scan.copy(
            output = output,
            requiredSchema = pruneDeletionVectorSchema(scan.requiredSchema),
            dataFilters = stripNativeDeletionVectorFilters(scan.dataFilters),
            pushDownFilters = scan.pushDownFilters.map(stripNativeDeletionVectorFilters)
          )
        rewrittenScan.copyTagsFrom(scan)
        rewrittenScan
      case scan: FileSourceScanExec =>
        val rewrittenScan =
          scan.copy(
            output = output,
            requiredSchema = pruneDeletionVectorSchema(scan.requiredSchema),
            dataFilters = stripNativeDeletionVectorFilters(scan.dataFilters))
        rewrittenScan.copyTagsFrom(scan)
        rewrittenScan
      case other =>
        other
    }
  }

  private def isInputFileRelatedAttribute(attr: Attribute): Boolean = {
    attr match {
      case AttributeReference(name, _, _, _) =>
        Seq(InputFileName(), InputFileBlockStart(), InputFileBlockLength())
          .map(_.prettyName)
          .contains(name)
      case _ => false
    }
  }

  private[gluten] def containsIncrementMetricExpr(expr: Expression): Boolean = {
    expr match {
      case e if e.prettyName == "increment_metric" => true
      case _ => expr.children.exists(containsIncrementMetricExpr)
    }
  }

  /**
   * This method is only used for Delta ColumnMapping FileFormat(e.g. nameMapping and idMapping)
   * transform the metadata of Delta into Parquet's, each plan should only be transformed once.
   */
  private def transformColumnMappingPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: DeltaScanTransformer =>
      val fileFormat = plan.relation.fileFormat
      val relation = plan.relation
      val newPartitionSchema =
        createPhysicalSchema(relation.partitionSchema, fileFormat).getOrElse(return plan)
      val newDataSchema =
        createPhysicalSchema(relation.dataSchema, fileFormat).getOrElse(return plan)
      val newRequiredSchema =
        createPhysicalSchema(plan.requiredSchema, fileFormat).getOrElse(return plan)

      // transform HadoopFsRelation
      val newFsRelation = relation.copy(
        partitionSchema = newPartitionSchema,
        dataSchema = newDataSchema
      )(SparkSession.active)
      // transform output's name into physical name so Reader can read data correctly
      // should keep the columns order the same as the origin output
      val originColumnNames = ListBuffer.empty[String]
      val transformedAttrs = ListBuffer.empty[Attribute]
      def mapAttribute(attr: Attribute) = {
        val newAttr = if (plan.isMetadataColumn(attr)) {
          attr
        } else if (isInputFileRelatedAttribute(attr)) {
          attr
        } else {
          createPhysicalAttributes(Seq(attr), fileFormat)
            .flatMap(_.headOption)
            .getOrElse(attr)
        }
        if (!originColumnNames.contains(attr.name)) {
          transformedAttrs += newAttr
          originColumnNames += attr.name
        }
        newAttr
      }
      val newOutput = plan.output.map(o => mapAttribute(o))
      // transform dataFilters
      val newDataFilters = plan.dataFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              mapAttribute(attr)
          }
      }
      // transform partitionFilters
      val newPartitionFilters = plan.partitionFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              mapAttribute(attr)
          }
      }
      // replace tableName in schema with physicalName
      val scanExecTransformer = new DeltaScanTransformer(
        newFsRelation,
        plan.stream,
        newOutput,
        newRequiredSchema,
        newPartitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        newDataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan
      )
      scanExecTransformer.copyTagsFrom(plan)
      tagColumnMappingRule(scanExecTransformer)

      // alias physicalName into tableName
      val expr = (transformedAttrs, originColumnNames).zipped.map {
        (attr, columnName) => Alias(attr, columnName)(exprId = attr.exprId)
      }
      val projectExecTransformer = ProjectExecTransformer(expr.toSeq, scanExecTransformer)
      projectExecTransformer
    case _ => plan
  }

  private def invokeNoArg(target: AnyRef, methodName: String): Option[AnyRef] = {
    target.getClass.getMethods
      .find(method => method.getName == methodName && method.getParameterCount == 0)
      .map(_.invoke(target))
      .collect { case value: AnyRef => value }
  }

  private def loadScalaObject(className: String): Option[AnyRef] = {
    try {
      // scalastyle:off classforname
      val cls = Class.forName(className, false, Thread.currentThread().getContextClassLoader)
      // scalastyle:on classforname
      Option(cls.getField("MODULE$").get(null).asInstanceOf[AnyRef])
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => None
    }
  }

  private def invokeModuleMethod(
      moduleClassName: String,
      methodName: String,
      args: Seq[AnyRef]): Option[AnyRef] = {
    loadScalaObject(moduleClassName).flatMap {
      module =>
        module.getClass.getMethods
          .find(method => method.getName == methodName && method.getParameterCount == args.size)
          .map(_.invoke(module, args: _*))
          .collect { case value: AnyRef => value }
    }
  }

  private def createPhysicalSchema(
      inputSchema: StructType,
      fileFormat: FileFormat): Option[StructType] = {
    for {
      referenceSchema <- invokeNoArg(fileFormat, "referenceSchema").collect {
        case schema: StructType => schema
      }
      columnMappingMode <- invokeNoArg(fileFormat, "columnMappingMode")
      transformed <- invokeModuleMethod(
        DeltaColumnMappingObjectClassName,
        "createPhysicalSchema",
        Seq(inputSchema, referenceSchema, columnMappingMode))
        .collect { case schema: StructType => schema }
    } yield transformed
  }

  private def createPhysicalAttributes(
      attributes: Seq[Attribute],
      fileFormat: FileFormat): Option[Seq[Attribute]] = {
    for {
      referenceSchema <- invokeNoArg(fileFormat, "referenceSchema").collect {
        case schema: StructType => schema
      }
      columnMappingMode <- invokeNoArg(fileFormat, "columnMappingMode")
      transformed <- invokeModuleMethod(
        DeltaColumnMappingObjectClassName,
        "createPhysicalAttributes",
        Seq(attributes, referenceSchema, columnMappingMode))
        .collect { case attrs: Seq[_] => attrs.collect { case attr: Attribute => attr } }
    } yield transformed
  }
}
