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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._

import com.google.protobuf.StringValue

import java.util.{ArrayList => JArrayList, List => JList}

/**
 * @param groupingSetAggregation
 *   When true, this Expand sits directly on top of a partial aggregation at the finest grain and
 *   its projections expand aggregation states rather than raw rows. The flag is passed to the
 *   backend as an advanced-extension marker on the ExpandRel; a backend that ignores the marker
 *   still produces correct (merely un-reduced) partial states, so the flag is a hint, not a
 *   contract. Only the Velox backend's LazyAggregateExpandRule sets it, and only when
 *   spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.enabled is on.
 */
case class ExpandExecTransformer(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: SparkPlan,
    groupingSetAggregation: Boolean = false)
  extends UnaryExecNode
  with UnaryTransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genExpandTransformerMetrics(sparkContext)

  @transient
  override lazy val references: AttributeSet = {
    AttributeSet.fromAttributeSets(projections.flatten.map(_.references))
  }

  override def isNoop: Boolean = projections == null || projections.isEmpty

  override def metricsUpdater(): MetricsUpdater = if (isNoop) {
    MetricsUpdater.None
  } else {
    BackendsApiManager.getMetricsApiInstance.genExpandTransformerMetricsUpdater(metrics)
  }

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  def getRelNode(
      context: SubstraitContext,
      projections: Seq[Seq[Expression]],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val projectSetExprNodes = new JArrayList[JList[ExpressionNode]]()
    projections.foreach {
      projectSet =>
        val projectExprNodes = new JArrayList[ExpressionNode]()
        projectSet.foreach {
          project =>
            val projectExprNode = ExpressionConverter
              .replaceWithExpressionTransformer(project, originalInputAttributes)
              .doTransform(context)
            projectExprNodes.add(projectExprNode)
        }
        projectSetExprNodes.add(projectExprNodes)
    }

    if (!validation) {
      if (groupingSetAggregation) {
        // Marker read by the native plan converter, which fuses this Expand and its child
        // aggregation into a single grouping-set aggregation operator. childGrouped would advertise
        // that the child delivers rows already grouped at the finest grain, letting the native
        // operator skip the hash table for the all-keys-active set (its "bypass lane").
        //
        // DECISION (defensibility blocker B5): childGrouped is fixed at 0 -- the bypass lane is
        // deliberately NOT engaged from Gluten, and no bypass speedup may be quoted for the
        // integrated path. The child here is the FlushableHashAggregateExecTransformer emitted by
        // LazyAggregateExpandRule; a flushable aggregate may abandon itself and stream rows
        // through, so it does NOT reliably deliver pre-grouped rows and cannot honestly claim
        // childGrouped=1. Honestly claiming the lane would require making the child a NON-flushable
        // Regular aggregate, which cannot abandon -- trading away the abandon safety valve that
        // protects high-cardinality keys (GLUTEN-7986). That trade is not safely determinable in
        // the rule for arbitrary input cardinality, so we keep the safety valve and forgo the lane.
        // The native side reads childGrouped= via configSetInOptimization, which only treats
        // "childGrouped=1" as set, so emitting "childGrouped=0" leaves the native childGroupedSet
        // as nullopt. The 2.29x figure a C++ harness measured for the lane in isolation is
        // therefore NOT achievable through this path and must not appear as a headline number.
        val optimization = BackendsApiManager.getTransformerApiInstance.packPBMessage(
          StringValue.newBuilder
            .setValue("isRollup=1\nchildGrouped=0\n")
            .build)
        val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, null)
        RelBuilder.makeExpandRel(input, projectSetExprNodes, extensionNode, context, operatorId)
      } else {
        RelBuilder.makeExpandRel(input, projectSetExprNodes, context, operatorId)
      }
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }

      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeExpandRel(input, projectSetExprNodes, extensionNode, context, operatorId)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!BackendsApiManager.getSettings.supportExpandExec()) {
      return ValidationResult.failed("Current backend does not support expand")
    }
    if (projections.isEmpty) {
      return ValidationResult.failed("Current backend does not support empty projections in expand")
    }

    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode =
      getRelNode(substraitContext, projections, child.output, operatorId, null, validation = true)

    doNativeValidation(substraitContext, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    if (isNoop) {
      // The computing for this Expand is not needed.
      return childCtx
    }

    val operatorId = context.nextOperatorId(this.nodeName)
    val currRel =
      getRelNode(context, projections, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Expand Rel should be valid")
    TransformContext(output, currRel)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExpandExecTransformer =
    copy(child = newChild)
}
