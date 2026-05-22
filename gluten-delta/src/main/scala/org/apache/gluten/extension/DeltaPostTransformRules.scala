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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{DeltaScanTransformer, FilterExecTransformerBase, ProjectExecTransformer}
import org.apache.gluten.extension.columnar.transition.RemoveTransitions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, CreateNamedStruct, Expression, GetStructField, If, InputFileBlockLength, InputFileBlockStart, InputFileName, IsNull, LambdaFunction, Literal, NamedExpression, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, TransformKeys, TransformValues}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DeltaPostTransformRules {
  def rules: Seq[Rule[SparkPlan]] =
    RemoveTransitions ::
      nativeDeletionVectorRule ::
      pushDownInputFileExprRule ::
      columnMappingRule :: Nil

  private val deletionVectorDeletedRowColumnName = "__delta_internal_is_row_deleted"
  private val deletionVectorRowIndexColumnNames =
    Set("__delta_internal_row_index", "_tmp_metadata_row_index", "row_index")
  private val deletionVectorInternalColumnNames =
    deletionVectorRowIndexColumnNames + deletionVectorDeletedRowColumnName
  private val deltaMetadataColumnName = "_metadata"

  private val COLUMN_MAPPING_RULE_TAG: TreeNodeTag[String] =
    TreeNodeTag[String]("org.apache.gluten.delta.column.mapping")
  private val PRESERVE_DELETION_VECTOR_ROW_INDEX_TAG: TreeNodeTag[Boolean] =
    TreeNodeTag[Boolean]("org.apache.gluten.delta.preserve.deletion.vector.row.index")
  private val PRESERVE_DELTA_METADATA_TAG: TreeNodeTag[Boolean] =
    TreeNodeTag[Boolean]("org.apache.gluten.delta.preserve.metadata")

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

  /**
   * Spark Delta injects synthetic deletion-vector predicates and columns into the plan. Those are
   * needed for the JVM reader path, but for the native Delta scan path they must be stripped or
   * they will be applied twice with incompatible semantics.
   */
  val nativeDeletionVectorRule: Rule[SparkPlan] = (plan: SparkPlan) => {
    tagRowIndexRequiredSubtrees(plan)
    plan.transformUp {
      case scan: DeltaScanTransformer =>
        val cleanedDataFilters = scan.dataFilters.flatMap(stripDeletionVectorPredicate)
        val cleanedPushDownFilters =
          scan.pushDownFilters.map(_.flatMap(stripDeletionVectorPredicate))
        val preserveRowIndex = shouldPreserveDeletionVectorRowIndex(scan)
        val cleanedOutput = stripDeletionVectorInternalOutput(
          scan,
          preserveRowIndex,
          shouldPreserveDeltaMetadata(scan))
        val cleanedRequiredSchema =
          stripDeletionVectorInternalSchema(scan.requiredSchema, preserveRowIndex)
        if (
          cleanedDataFilters == scan.dataFilters &&
          cleanedPushDownFilters == scan.pushDownFilters &&
          cleanedOutput == scan.output &&
          cleanedRequiredSchema == scan.requiredSchema
        ) {
          scan
        } else {
          scan.copy(
            output = cleanedOutput,
            requiredSchema = cleanedRequiredSchema,
            dataFilters = cleanedDataFilters,
            pushDownFilters = cleanedPushDownFilters)
        }
      case project: ProjectExecTransformer if containsNativeDeltaScan(project.child) =>
        val cleanedProjectList = stripDeletionVectorInternalProjectList(
          project.projectList,
          shouldPreserveDeletionVectorRowIndex(project),
          shouldPreserveDeltaMetadata(project))
        if (cleanedProjectList == project.projectList) {
          project
        } else if (cleanedProjectList.isEmpty) {
          project.child
        } else {
          ProjectExecTransformer(cleanedProjectList, project.child)
        }
      case project: ProjectExec if containsNativeDeltaScan(project.child) =>
        val cleanedProjectList = stripDeletionVectorInternalProjectList(
          project.projectList,
          shouldPreserveDeletionVectorRowIndex(project),
          shouldPreserveDeltaMetadata(project))
        if (cleanedProjectList == project.projectList) {
          project
        } else if (cleanedProjectList.isEmpty) {
          project.child
        } else {
          ProjectExec(cleanedProjectList, project.child)
        }
      case filter: FilterExecTransformerBase if containsNativeDeltaScan(filter.child) =>
        stripDeletionVectorPredicate(filter.cond) match {
          case Some(cleanCondition) if cleanCondition != filter.cond =>
            BackendsApiManager.getSparkPlanExecApiInstance
              .genFilterExecTransformer(cleanCondition, filter.child)
          case Some(_) =>
            filter
          case None =>
            filter.child
        }
      case filter: FilterExec if containsNativeDeltaScan(filter.child) =>
        stripDeletionVectorPredicate(filter.condition) match {
          case Some(cleanCondition) if cleanCondition != filter.condition =>
            FilterExec(cleanCondition, filter.child)
          case Some(_) =>
            filter
          case None =>
            filter.child
        }
    }
  }

  private def containsNativeDeltaScan(plan: SparkPlan): Boolean = {
    plan.exists {
      case _: DeltaScanTransformer => true
      case _ => false
    }
  }

  private def isDeltaColumnMappingFileFormat(fileFormat: FileFormat): Boolean = fileFormat match {
    case d: DeltaParquetFileFormat if d.columnMappingMode != NoMapping =>
      true
    case _ =>
      false
  }

  private def containsInputFileRelatedExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => expr.children.exists(containsInputFileRelatedExpr)
    }
  }

  private def referencesDeletionVectorInternalColumn(expr: Expression): Boolean = {
    expr.references.exists(attr => deletionVectorInternalColumnNames.contains(attr.name))
  }

  private def referencesDeletionVectorDeletedRow(expr: Expression): Boolean = {
    expr.references.exists(_.name == deletionVectorDeletedRowColumnName)
  }

  private def referencesDeletionVectorRowIndex(expr: Expression): Boolean = {
    expr.references.exists(attr => deletionVectorRowIndexColumnNames.contains(attr.name))
  }

  private def referencesDeltaMetadata(expr: Expression): Boolean = {
    expr.references.exists(_.name == deltaMetadataColumnName)
  }

  private def isDeletionVectorInternalMetadataProject(expr: NamedExpression): Boolean = {
    expr.name == deltaMetadataColumnName && referencesDeletionVectorRowIndex(expr)
  }

  private def tagRowIndexRequiredSubtrees(plan: SparkPlan): Unit = {
    def tagSubtree(subtree: SparkPlan): Unit = {
      subtree.foreach(_.setTagValue(PRESERVE_DELETION_VECTOR_ROW_INDEX_TAG, true))
    }

    def tagMetadataSubtree(subtree: SparkPlan): Unit = {
      subtree.foreach(_.setTagValue(PRESERVE_DELTA_METADATA_TAG, true))
    }

    def visit(node: SparkPlan): Unit = {
      if (expressionsRequireDeletionVectorRowIndex(node)) {
        node.children.foreach(tagSubtree)
      }
      if (expressionsRequireDeltaMetadata(node)) {
        node.children.foreach(tagMetadataSubtree)
      }
      node.children.foreach(visit)
    }

    visit(plan)
  }

  private def shouldPreserveDeletionVectorRowIndex(plan: SparkPlan): Boolean = {
    plan.getTagValue(PRESERVE_DELETION_VECTOR_ROW_INDEX_TAG).contains(true) ||
    shouldPreserveDeltaMetadata(plan) ||
    (!plan.isInstanceOf[DeltaScanTransformer] && expressionsRequireDeletionVectorRowIndex(plan))
  }

  private def shouldPreserveDeltaMetadata(plan: SparkPlan): Boolean = {
    plan.getTagValue(PRESERVE_DELTA_METADATA_TAG).contains(true) ||
    (!plan.isInstanceOf[DeltaScanTransformer] && expressionsRequireDeltaMetadata(plan))
  }

  private def expressionsRequireDeletionVectorRowIndex(plan: SparkPlan): Boolean = {
    plan.expressions.exists(containsIncrementMetricExpr) ||
    (plan match {
      case filter: FilterExecTransformerBase =>
        stripDeletionVectorPredicate(filter.cond).exists(referencesDeletionVectorRowIndex)
      case filter: FilterExec =>
        stripDeletionVectorPredicate(filter.condition).exists(referencesDeletionVectorRowIndex)
      case project: ProjectExecTransformer =>
        stripDeletionVectorInternalProjectList(
          project.projectList,
          preserveRowIndex = false,
          preserveMetadata = false)
          .exists(referencesDeletionVectorRowIndex)
      case project: ProjectExec =>
        stripDeletionVectorInternalProjectList(
          project.projectList,
          preserveRowIndex = false,
          preserveMetadata = false)
          .exists(referencesDeletionVectorRowIndex)
      case _ =>
        plan.expressions.exists(referencesDeletionVectorRowIndex)
    })
  }

  private def expressionsRequireDeltaMetadata(plan: SparkPlan): Boolean = {
    plan.expressions.exists(referencesDeltaMetadata)
  }

  private def shouldStripDeletionVectorInternalColumn(
      columnName: String,
      preserveRowIndex: Boolean): Boolean = {
    columnName == deletionVectorDeletedRowColumnName ||
    (!preserveRowIndex && deletionVectorRowIndexColumnNames.contains(columnName))
  }

  private def stripDeletionVectorInternalOutput(
      scan: DeltaScanTransformer,
      preserveRowIndex: Boolean,
      preserveMetadata: Boolean): Seq[Attribute] = {
    scan.output.filterNot(
      attr =>
        shouldStripDeletionVectorInternalColumn(attr.name, preserveRowIndex) ||
          (!preserveMetadata && scan.isMetadataColumn(attr)))
  }

  private def stripDeletionVectorInternalProjectList(
      projectList: Seq[NamedExpression],
      preserveRowIndex: Boolean,
      preserveMetadata: Boolean): Seq[NamedExpression] = {
    projectList.filterNot(
      expr =>
        shouldStripDeletionVectorInternalColumn(expr.name, preserveRowIndex) ||
          referencesDeletionVectorDeletedRow(expr) ||
          (!preserveMetadata && isDeletionVectorInternalMetadataProject(expr)))
  }

  private def stripDeletionVectorInternalSchema(
      schema: StructType,
      preserveRowIndex: Boolean): StructType = {
    StructType(
      schema.filterNot(
        field => shouldStripDeletionVectorInternalColumn(field.name, preserveRowIndex)))
  }

  private[gluten] def stripDeletionVectorPredicate(expr: Expression): Option[Expression] = {
    expr match {
      case And(left, right) =>
        (stripDeletionVectorPredicate(left), stripDeletionVectorPredicate(right)) match {
          case (Some(cleanLeft), Some(cleanRight)) => Some(And(cleanLeft, cleanRight))
          case (Some(cleanLeft), None) => Some(cleanLeft)
          case (None, Some(cleanRight)) => Some(cleanRight)
          case (None, None) => None
        }
      case other if referencesDeletionVectorInternalColumn(other) =>
        None
      case other =>
        Some(other)
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
   * Checks whether two structurally compatible DataTypes have different struct field names at any
   * nesting level.
   */
  private def nestedFieldNamesDiffer(logical: DataType, physical: DataType): Boolean = {
    (logical, physical) match {
      case (l: StructType, p: StructType) if l.length == p.length =>
        l.zip(p).exists {
          case (lf, pf) =>
            lf.name != pf.name || nestedFieldNamesDiffer(lf.dataType, pf.dataType)
        }
      case (l: ArrayType, p: ArrayType) =>
        nestedFieldNamesDiffer(l.elementType, p.elementType)
      case (l: MapType, p: MapType) =>
        nestedFieldNamesDiffer(l.keyType, p.keyType) ||
        nestedFieldNamesDiffer(l.valueType, p.valueType)
      case _ => false
    }
  }

  /**
   * Rebuilds an expression tree so that nested struct field names match the logical schema. Uses
   * positional extraction and reconstruction instead of Cast, so correctness does not depend on
   * Velox's cast_match_struct_by_name config.
   */
  private def reconcileFieldNames(
      expr: Expression,
      logical: DataType,
      physical: DataType): Expression = {
    (logical, physical) match {
      case (l: StructType, p: StructType) if l.length == p.length =>
        val rebuiltFields = l.zip(p).zipWithIndex.flatMap {
          case ((lf, pf), i) =>
            val extracted = GetStructField(expr, i, None)
            val reconciled = reconcileFieldNames(extracted, lf.dataType, pf.dataType)
            Seq(Literal(lf.name), reconciled)
        }
        val rebuilt = CreateNamedStruct(rebuiltFields)
        If(IsNull(expr), Literal.create(null, l), rebuilt)
      case (l: ArrayType, p: ArrayType) if nestedFieldNamesDiffer(l.elementType, p.elementType) =>
        val lambdaVar = NamedLambdaVariable("element", p.elementType, p.containsNull)
        val body = reconcileFieldNames(lambdaVar, l.elementType, p.elementType)
        ArrayTransform(expr, LambdaFunction(body, Seq(lambdaVar)))
      case (l: MapType, p: MapType) =>
        val needKeys = nestedFieldNamesDiffer(l.keyType, p.keyType)
        val needValues = nestedFieldNamesDiffer(l.valueType, p.valueType)
        var result = expr
        if (needValues) {
          val keyVar = NamedLambdaVariable("key", p.keyType, false)
          val valueVar = NamedLambdaVariable("value", p.valueType, p.valueContainsNull)
          val body = reconcileFieldNames(valueVar, l.valueType, p.valueType)
          result = TransformValues(result, LambdaFunction(body, Seq(keyVar, valueVar)))
        }
        if (needKeys) {
          val keyVar = NamedLambdaVariable("key", p.keyType, false)
          val valueVar = NamedLambdaVariable(
            "value",
            if (needValues) l.valueType else p.valueType,
            p.valueContainsNull)
          val body = reconcileFieldNames(keyVar, l.keyType, p.keyType)
          result = TransformKeys(result, LambdaFunction(body, Seq(keyVar, valueVar)))
        }
        result
      case _ => expr
    }
  }

  /**
   * This method is only used for Delta ColumnMapping FileFormat(e.g. nameMapping and idMapping)
   * transform the metadata of Delta into Parquet's, each plan should only be transformed once.
   */
  private def transformColumnMappingPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: DeltaScanTransformer =>
      val fmt = plan.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]

      // transform HadoopFsRelation
      val relation = plan.relation
      val newFsRelation = relation.copy(
        partitionSchema = DeltaColumnMapping.createPhysicalSchema(
          relation.partitionSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode),
        dataSchema = DeltaColumnMapping.createPhysicalSchema(
          relation.dataSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode)
      )(SparkSession.active)
      // transform output's name into physical name so Reader can read data correctly
      // should keep the columns order the same as the origin output
      case class ColumnMapping(logicalName: String, logicalType: DataType, physicalAttr: Attribute)
      val columnMappings = ListBuffer.empty[ColumnMapping]
      val seenNames = mutable.Set.empty[String]
      def mapAttribute(attr: Attribute) = {
        val newAttr = if (plan.isMetadataColumn(attr)) {
          attr
        } else if (isInputFileRelatedAttribute(attr)) {
          attr
        } else {
          DeltaColumnMapping
            .createPhysicalAttributes(Seq(attr), fmt.referenceSchema, fmt.columnMappingMode)
            .head
        }
        if (seenNames.add(attr.name)) {
          columnMappings += ColumnMapping(attr.name, attr.dataType, newAttr)
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
        DeltaColumnMapping.createPhysicalSchema(
          plan.requiredSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode),
        newPartitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        newDataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan
      )
      scanExecTransformer.copyTagsFrom(plan)
      tagColumnMappingRule(scanExecTransformer)

      // Alias physical names back to logical names. For struct-typed columns, Delta column
      // mapping renames internal field names to physical UUIDs. A top-level Alias only restores
      // the column name, not the struct's internal field names.
      val expr = columnMappings.map {
        cm =>
          val projectedExpr: Expression =
            if (nestedFieldNamesDiffer(cm.logicalType, cm.physicalAttr.dataType)) {
              reconcileFieldNames(cm.physicalAttr, cm.logicalType, cm.physicalAttr.dataType)
            } else {
              cm.physicalAttr
            }
          Alias(projectedExpr, cm.logicalName)(exprId = cm.physicalAttr.exprId)
      }
      val projectExecTransformer = ProjectExecTransformer(expr.toSeq, scanExecTransformer)
      projectExecTransformer
    case _ => plan
  }
}
