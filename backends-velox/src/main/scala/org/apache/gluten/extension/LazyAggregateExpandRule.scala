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

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

/**
 * Reorders partial aggregation for grouping sets so raw rows are aggregated once at the finest
 * grain.
 *
 * Spark's plan:
 *
 * partial aggregate <- expand <- child
 *
 * Rewritten plan:
 *
 * partial-merge aggregate <- expand (intermediate states) <- partial aggregate <- child
 *
 * This reduces hash work when the finest grouping has substantially fewer rows than the input. The
 * pre-shuffle merge bounds shuffle volume, and flushable aggregation keeps both stages adaptive for
 * high-cardinality keys. The rule is disabled when flushable partial aggregation is disabled.
 *
 * Rewrite invariants:
 *   - The rewritten sub-plan's output attributes equal the original partial aggregate's output, so
 *     no operator above the matched aggregate needs adjustment.
 *   - The new expand's output is exactly `grouping attributes ++ aggregation buffer attributes` in
 *     the original order. The partial-merge aggregate binds its buffer inputs by name and position
 *     against `child.output.drop(groupingExpressions.size)`, so this ordering is load-bearing.
 *   - Aggregate filters are only evaluated in the finest-grain (raw input) aggregate. The
 *     partial-merge copies drop them, mirroring Spark's AggUtils.mayRemoveAggFilters.
 *
 * With fusedGroupingSetAggregate enabled, eligible plans tag the Expand for replacement by Gluten's
 * native grouping-set operator and omit the separate partial-merge stage.
 */
case class LazyAggregateExpandRule(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (
      !VeloxConfig.get.enableVeloxLazyAggregateExpand ||
      !VeloxConfig.get.enableVeloxFlushablePartialAggregation
    ) {
      return plan
    }
    plan.transformUp {
      case agg: RegularHashAggregateExecTransformer if isEligibleAggregate(agg) =>
        agg.child match {
          case expand: ExpandExecTransformer =>
            rewrite(agg, expand, preProject = None, preFilter = None).getOrElse(agg)
          case project @ ProjectExecTransformer(_, expand: ExpandExecTransformer) =>
            rewrite(agg, expand, preProject = Some(project), preFilter = None).getOrElse(agg)
          case filter @ FilterExecTransformer(_, expand: ExpandExecTransformer) =>
            rewrite(agg, expand, preProject = None, preFilter = Some(filter)).getOrElse(agg)
          case _ => agg
        }
    }
  }

  // Matches only Partial-mode regular aggregates with a zero buffer offset. The offset check
  // keeps the rule idempotent for measure-less aggregates (whose mode list is empty): every
  // partial-merge aggregate this rule emits carries offset >= 1, while Spark plans partial
  // aggregates with offset 0.
  private def isEligibleAggregate(agg: RegularHashAggregateExecTransformer): Boolean = {
    agg.initialInputBufferOffset == 0 &&
    agg.groupingExpressions.forall(_.isInstanceOf[Attribute]) &&
    agg.aggregateExpressions.forall(_.mode == Partial) &&
    agg.aggregateExpressions.forall(isSupportedAggregateExpression) &&
    !hasUnsafeFloatingPointAggregate(agg.aggregateExpressions)
  }

  private def isSupportedAggregateExpression(aggExpr: AggregateExpression): Boolean = {
    if (aggExpr.filter.isDefined || aggExpr.isDistinct) {
      return false
    }
    aggExpr.aggregateFunction match {
      case s: Sum => !s.prettyName.equals("try_sum")
      case a: Average => !a.prettyName.equals("try_avg")
      case _: Count => true
      case _: Min => true
      case _: Max => true
      case _ => false
    }
  }

  // The rewrite reorders how partial states are merged, which can change the result of
  // floating-point sum/avg bitwise. Apply the same policy as FlushableHashAggregateRule.
  private def hasUnsafeFloatingPointAggregate(aggExprs: Seq[AggregateExpression]): Boolean = {
    if (VeloxConfig.get.floatingPointMode == "loose") {
      return false
    }

    def isFloatingPointType(dataType: DataType): Boolean = {
      dataType == DoubleType || dataType == FloatType
    }

    aggExprs.exists {
      aggExpr =>
        aggExpr.aggregateFunction match {
          case s: Sum => isFloatingPointType(s.child.dataType)
          // Spark accumulates a non-decimal average in DOUBLE even when its input is integral.
          // Reassociating those partial sums can therefore change the result's low bits.
          case a: Average => isFloatingPointType(a.sumDataType)
          case _ => false
        }
    }
  }

  private def rewrite(
      agg: RegularHashAggregateExecTransformer,
      expand: ExpandExecTransformer,
      preProject: Option[ProjectExecTransformer],
      preFilter: Option[FilterExecTransformer]): Option[SparkPlan] = {
    val numKeys = agg.groupingExpressions.length
    val expandChildOutput = expand.child.output

    // A Partial aggregate's result expressions are its grouping attributes followed by the
    // flattened aggregation buffer attributes. Anything else means an unexpected plan shape.
    val numBufferAttributes =
      agg.aggregateExpressions.map(_.aggregateFunction.aggBufferAttributes.length).sum
    if (
      agg.resultExpressions.length != numKeys + numBufferAttributes ||
      !agg.resultExpressions.forall(_.isInstanceOf[Attribute]) ||
      !agg.resultExpressions
        .take(numKeys)
        .zip(agg.groupingExpressions)
        .forall { case (result, key) => result.toAttribute.semanticEquals(key.toAttribute) }
    ) {
      logDebug(s"Lazy expand: unexpected partial aggregate output shape: ${agg.resultExpressions}")
      return None
    }
    val bufferAttributes = agg.resultExpressions.drop(numKeys).map(_.toAttribute)

    // A pull-out pre-projection between the aggregate and the expand computes aggregate inputs
    // (e.g. `_pre_1 = coalesce(a * b, 0)`) from columns that pass through the expand unchanged.
    // It can be re-grounded onto the expand's child iff its computed expressions reference only
    // pre-expand columns.
    val preProjectAliases = preProject.map(_.projectList.collect { case a: Alias => a })
    if (
      !preProject.forall(
        _.projectList.forall {
          case _: Attribute => true
          // Non-deterministic expressions must keep their original per-expanded-row evaluation;
          // moving them below the expand would share one draw across all grouping sets.
          case a: Alias => a.child.deterministic && resolvableFrom(a.references, expandChildOutput)
          case _ => false
        })
    ) {
      logDebug("Lazy expand: pre-project is not re-groundable onto the expand's child")
      return None
    }

    // Aggregate inputs must come from columns that pass through the expand unchanged (or from
    // the re-grounded pre-projection). This also rejects the look-alike Expand produced by
    // RewriteDistinctAggregates, whose aggregate functions reference expand-created attributes.
    val aggregateInputCandidates =
      expandChildOutput ++ preProjectAliases.getOrElse(Seq.empty).map(_.toAttribute)
    if (
      !agg.aggregateExpressions.forall(
        ae => resolvableFrom(ae.aggregateFunction.references, aggregateInputCandidates))
    ) {
      logDebug("Lazy expand: aggregate inputs are not pass-through columns of the expand")
      return None
    }

    // A filter between the aggregate and the expand may only reference grouping columns; it then
    // filters whole (group, grouping set) rows and can equivalently run above the new expand.
    if (
      !preFilter.forall(
        f =>
          f.condition.deterministic &&
            resolvableFrom(f.condition.references, agg.groupingExpressions.map(_.toAttribute)))
    ) {
      logDebug("Lazy expand: filter references non-grouping columns of the expand")
      return None
    }

    // Maps each expand output attribute to the pre-expand attribute that passes through in that
    // slot. Slots that are literal-only in every projection (grouping id, grouping position,
    // constant grouping keys) have no mapping and are re-attached in the new expand as-is.
    val replaceMap = buildReplaceAttributeMap(expand)
    val bottomGroupingKeys = agg.groupingExpressions
      .map(_.toAttribute)
      .flatMap(attr => findReplacement(attr, replaceMap))
      .distinct

    // A keyless finest-grain aggregate would emit one row on empty input, producing spurious
    // grand-total rows where Spark returns none. This rewrite does not support non-atomic keys.
    if (
      bottomGroupingKeys.isEmpty ||
      !bottomGroupingKeys.forall(key => isSupportedGroupingKeyType(key.dataType))
    ) {
      logDebug(s"Lazy expand: unsupported finest-grain grouping keys: $bottomGroupingKeys")
      return None
    }

    val reGroundedPreProject = preProject.map {
      project =>
        val reGrounded =
          ProjectExecTransformer(expandChildOutput ++ preProjectAliases.get, expand.child)
        reGrounded.copyTagsFrom(project)
        reGrounded
    }
    val bottomChild = reGroundedPreProject.getOrElse(expand.child)

    // Flushable, so Velox can abandon the aggregation when the finest grain barely reduces the
    // row count; a regular aggregate here would hash and spill the whole input on
    // high-cardinality keys.
    val bottomAggregate = FlushableHashAggregateExecTransformer(
      requiredChildDistributionExpressions = None,
      groupingExpressions = bottomGroupingKeys,
      aggregateExpressions = agg.aggregateExpressions,
      aggregateAttributes = agg.aggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions = bottomGroupingKeys ++ bufferAttributes,
      child = bottomChild
    )
    bottomAggregate.copyTagsFrom(agg)

    // Native conversion recovers key masks and grouping ids from the Expand projections. It
    // requires one literal grouping-id slot. For multi-column Spark buffers, native conversion
    // sees through the standard extraction ProjectRel and restores the flattened schema afterward.
    val fusionEnabled = VeloxConfig.get.enableVeloxFusedGroupingSetAggregate
    val hasMeasures = agg.aggregateExpressions.nonEmpty
    if (fusionEnabled && !hasMeasures) {
      logDebug(
        "Fused grouping-set aggregation: measure-less grouping sets require Velox's distinct " +
          "aggregation path, keeping the merge stage")
    }
    val shapeFusible = fusionEnabled && hasMeasures && {
      val literalSlots = agg.groupingExpressions.map(_.toAttribute).zipWithIndex.filter {
        case (attr, _) => findReplacement(attr, replaceMap).isEmpty
      }
      val fusible = bottomGroupingKeys.length == numKeys - 1 &&
        bottomGroupingKeys.length <= 64 &&
        literalSlots.length == 1 &&
        literalSlots.head._2 == numKeys - 1 &&
        literalSlots.head._1.dataType == LongType
      if (!fusible) {
        logDebug(
          "Fused grouping-set aggregation: plan shape is not fusible, keeping the merge stage")
      }
      fusible
    }

    val newExpandOutput = agg.resultExpressions.map(_.toAttribute)
    val newExpandProjections =
      buildPostExpandProjections(expand.projections, expand.output, newExpandOutput)

    // Spark permits duplicate grouping sets, while the native node requires unique key masks.
    // Derive masks from the rebuilt projections that the native converter reads.
    val fused = shapeFusible && {
      val masks = newExpandProjections.map(_.take(numKeys - 1).map(_.isInstanceOf[Attribute]))
      val distinctMasks = masks.distinct.length == masks.length
      val withinSetLimit = masks.length <= VeloxConfig.get.maxVeloxFusedGroupingSets
      val singleRoot = distinctMasks && withinSetLimit && numLatticeRoots(masks) == 1
      if (!distinctMasks) {
        logDebug(
          "Fused grouping-set aggregation: duplicate grouping sets, keeping the merge stage")
      }
      if (!withinSetLimit) {
        logDebug(
          s"Fused grouping-set aggregation: ${masks.length} grouping sets exceed the configured " +
            s"maximum ${VeloxConfig.get.maxVeloxFusedGroupingSets}, keeping the merge stage")
      }
      if (distinctMasks && withinSetLimit && !singleRoot) {
        logDebug(
          "Fused grouping-set aggregation: grouping sets have multiple derivation roots, " +
            "keeping the merge stage")
      }
      distinctMasks && withinSetLimit && singleRoot
    }

    val expandBottomAggregate =
      if (fused) {
        bottomAggregate.copy(extractionMetricsOwnedByParent = true)
      } else {
        bottomAggregate
      }
    expandBottomAggregate.copyTagsFrom(agg)

    val newExpand =
      ExpandExecTransformer(
        newExpandProjections,
        newExpandOutput,
        expandBottomAggregate,
        fused,
        fused && VeloxConfig.get.enableVeloxFusedGroupingSetAggregateFinestSetBypass)
    newExpand.copyTagsFrom(expand)

    val newPreFilter = preFilter.map {
      filter =>
        val newFilter = filter.copy(child = newExpand)
        newFilter.copyTagsFrom(filter)
        newFilter
    }
    val mergeChild = newPreFilter.getOrElse(newExpand)

    // The tagged Expand replaces the partial-merge stage. The marker is emitted only during the
    // real transform because applicability depends on the child plan. These guards avoid tagging
    // unsupported shapes; the native converter also falls back to a schema-compatible plain Expand
    // if its checks reject the optimization.
    if (fused) {
      val fusedNodes: Seq[SparkPlan] =
        reGroundedPreProject.toSeq ++ Seq(expandBottomAggregate, newExpand) ++ newPreFilter.toSeq
      if (!fusedNodes.forall(passesNativeValidation)) {
        logDebug("Fused grouping-set aggregation: native validation failed; keeping original")
        return None
      }
      logDebug(s"Fused grouping-set aggregation replaced aggregate over expand: $mergeChild")
      return Some(mergeChild)
    }

    // Deliberately Regular: FlushableHashAggregateRule runs next and converts this merge stage
    // to flushable (it walks down from the shuffle and stops here, never reaching the bottom
    // aggregate, which is why the bottom one is emitted flushable directly above).
    val mergeAggregate = RegularHashAggregateExecTransformer(
      requiredChildDistributionExpressions = agg.requiredChildDistributionExpressions,
      groupingExpressions = agg.groupingExpressions,
      aggregateExpressions =
        agg.aggregateExpressions.map(_.copy(mode = PartialMerge, filter = None)),
      aggregateAttributes = agg.aggregateAttributes,
      initialInputBufferOffset = numKeys,
      resultExpressions = agg.resultExpressions,
      child = mergeChild
    )
    mergeAggregate.copyTagsFrom(agg)

    val newNodes: Seq[SparkPlan] =
      reGroundedPreProject.toSeq ++ Seq(bottomAggregate, newExpand) ++
        newPreFilter.toSeq :+ mergeAggregate
    if (!newNodes.forall(passesNativeValidation)) {
      logDebug("Lazy expand: native validation failed for the rewritten plan; keeping original")
      return None
    }
    logDebug(s"Lazy expand rewrote aggregate over expand: $mergeAggregate")
    Some(mergeAggregate)
  }

  // Counts masks that have no strict superset in the selected grouping sets. Candidate sets fit
  // in one machine word because fusion is capped at 64 sets. Intersecting one candidate bitmap per
  // active key makes this O(S * K), rather than comparing every pair of sets.
  private def numLatticeRoots(masks: Seq[Seq[Boolean]]): Int = {
    if (masks.isEmpty) {
      return 0
    }

    val candidatesContainingKey = Array.fill[Long](masks.head.length)(0L)
    masks.zipWithIndex.foreach {
      case (mask, setIndex) =>
        val candidateBit = 1L << setIndex
        mask.zipWithIndex.foreach {
          case (true, keyIndex) =>
            candidatesContainingKey(keyIndex) |= candidateBit
          case _ => ()
        }
    }

    val allCandidates =
      if (masks.length == 64) -1L else (1L << masks.length) - 1L
    masks.zipWithIndex.count {
      case (mask, setIndex) =>
        var candidates = allCandidates & ~(1L << setIndex)
        var keyIndex = 0
        while (keyIndex < mask.length && candidates != 0L) {
          if (mask(keyIndex)) {
            candidates &= candidatesContainingKey(keyIndex)
          }
          keyIndex += 1
        }
        candidates == 0L
    }
  }

  // The new expand must emit typed null literals for excluded grouping keys; restrict to types
  // whose null literals are known to round-trip through the native ExpandRel.
  private def isSupportedGroupingKeyType(dataType: DataType): Boolean = {
    dataType match {
      // Referenced by type name: TimestampNTZType is private[sql] in Spark 3.3.
      case dt if dt.typeName == "timestamp_ntz" => true
      case BooleanType | StringType | DateType | TimestampType | BinaryType =>
        true
      case _: NumericType => true
      case _ => false
    }
  }

  private def resolvableFrom(references: AttributeSet, candidates: Seq[Attribute]): Boolean = {
    references.forall(ref => candidates.exists(_.semanticEquals(ref)))
  }

  private def findReplacement(
      attribute: Attribute,
      replaceMap: Map[Attribute, Attribute]): Option[Attribute] = {
    replaceMap.collectFirst { case (k, v) if k.semanticEquals(attribute) => v }
  }

  private def buildReplaceAttributeMap(expand: ExpandExecTransformer): Map[Attribute, Attribute] = {
    // A slot is a pass-through of exactly one pre-expand attribute. Spark's Expand companion
    // builds grouping-set projections as `attr | Literal(null)` per slot, so every non-literal
    // entry in a slot is the same attribute -- but that is an unstated Spark invariant, and if it
    // were ever violated, taking the first entry would silently make the finest-grain aggregate
    // group by the wrong column. Require agreement instead of assuming it.
    val passThroughBySlot = expand.output.indices.map {
      i =>
        val attrs = expand.projections.collect {
          case projection if projection(i).isInstanceOf[Attribute] =>
            projection(i).asInstanceOf[Attribute]
        }
        attrs.headOption.filter(head => attrs.forall(_.semanticEquals(head)))
    }
    expand.output
      .zip(passThroughBySlot)
      .collect { case (out, Some(passThrough)) => out -> passThrough }
      .toMap
  }

  // Rebuilds the expand projections against the finest-grain aggregate's output: slots that
  // existed in the original expand keep their per-projection expression (pass-through attribute
  // or literal), and aggregation buffer attributes pass through every projection unchanged.
  private def buildPostExpandProjections(
      originalProjections: Seq[Seq[Expression]],
      originalOutput: Seq[Attribute],
      newOutput: Seq[Attribute]): Seq[Seq[Expression]] = {
    originalProjections.map {
      projection =>
        newOutput.map {
          attr =>
            val index = originalOutput.indexWhere(_.semanticEquals(attr))
            if (index != -1) {
              projection(index)
            } else {
              attr
            }
        }
    }
  }

  private def passesNativeValidation(plan: SparkPlan): Boolean = {
    plan match {
      case validatable: ValidatablePlan =>
        try {
          validatable.doValidate().ok()
        } catch {
          case e: Exception =>
            logDebug(s"Lazy expand: validation threw for ${plan.nodeName}: ${e.getMessage}")
            false
        }
      case _ => true
    }
  }
}
