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
 * For aggregation over grouping sets (rollup/cube), Spark expands every input row once per grouping
 * set before the partial aggregation, so the partial aggregate consumes and hashes
 * `input rows * number of grouping sets` rows:
 *
 * partial aggregate <- expand <- child
 *
 * When the number of distinct full-grouping-key combinations is much smaller than the input row
 * count, it is cheaper to aggregate at the finest grain once, expand only the intermediate
 * aggregation states, and merge the expanded states before shuffle:
 *
 * partial-merge aggregate <- expand (over aggregation buffers) <- partial aggregate <- child
 *
 * The pre-shuffle partial-merge stage collapses duplicated coarse-grained groups locally so the
 * rewrite does not increase shuffle volume (see the ClickHouse backend's lazy expand and its
 * high-cardinality regression, GLUTEN-7986, for why this stage is required).
 *
 * Both new aggregates rely on Velox's flushable-aggregation machinery: if the input has too many
 * distinct full-grouping-key combinations, the finest-grain aggregate abandons itself and streams
 * rows through in intermediate format, and the merge stage over non-raw input abandons to an
 * identity pass-through. The rewrite is therefore disabled when flushable partial aggregation is
 * disabled.
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
 * When spark.gluten.sql.columnar.backend.velox.fusedGroupingSetAggregate.enabled is also on and the
 * plan shape allows it, the rewrite stops one node short: the expand is tagged for the backend and
 * the partial-merge stage is dropped, because the native fused operator merges the grouping sets
 * itself. The tagged expand's output attributes are exactly the dropped merge aggregate's output
 * attributes, so the sub-plan above is again untouched.
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
          case a: Average => isFloatingPointType(a.child.dataType)
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
    // grand-total rows where Spark returns none. Non-atomic key types are excluded because the
    // new expand would need typed null literals for them, which is unaudited.
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

    // The fused native operator recovers each grouping set's key mask and grouping-id value from
    // the Expand's projections. That is only possible when every grouping slot but the last maps
    // onto a key of the finest-grain aggregate (no constant grouping keys, no two slots sharing a
    // key) and the last slot is the integral grouping id that the Expand fills in as a literal.
    //
    // SINGLE-COLUMN BUFFERS ONLY. VeloxHashAggregateExecTransformer.getAggRel wraps the
    // AggregateRel in a ProjectRel whenever any Partial-mode function has more than one
    // aggBufferAttribute (extractStructNeeded / applyExtractStruct, which re-packs the flattened
    // buffer columns into a struct). The native fused converter requires
    // `expandRel.input().has_aggregate()` and derives numKeys from
    // `numInputColumns - childAggRel.measures().size()`; with the ProjectRel interposed the input
    // is no longer an AggregateRel (so the native side would degrade to a plain Expand via the
    // fallback net) and the arithmetic would be off by (bufferColumns - measures) even if it did
    // not. Excluding these shapes here keeps the common case from emitting a marker the native side
    // would only discard. This excludes `avg` and `sum` over DecimalType, whose buffers are
    // (sum, count) and (sum, isEmpty).
    //
    // OPEN (design decision, not a defect): this excludes TPC-DS q67, whose rollup aggregates
    // sum(ss_sales_price) over decimal(7,2). Lifting the restriction means teaching the native
    // converter to see through the extract-struct ProjectRel and re-pack the flattened columns,
    // which also requires verifying the accumulator-type agreement between
    // VeloxIntermediateData.getIntermediateTypeNode and Velox's resolveIntermediateType for each
    // affected function. Until then the fused flag is a no-op for those queries and the
    // three-stage rewrite handles them.
    val shapeFusible = VeloxConfig.get.enableVeloxFusedGroupingSetAggregate && {
      val literalSlots = agg.groupingExpressions.map(_.toAttribute).zipWithIndex.filter {
        case (attr, _) => findReplacement(attr, replaceMap).isEmpty
      }
      val singleColumnBuffers =
        agg.aggregateExpressions.forall(_.aggregateFunction.aggBufferAttributes.length == 1)
      val fusible = singleColumnBuffers &&
        bottomGroupingKeys.length == numKeys - 1 &&
        bottomGroupingKeys.length <= 64 &&
        literalSlots.length == 1 &&
        literalSlots.head._2 == numKeys - 1 &&
        (literalSlots.head._1.dataType == LongType || literalSlots.head._1.dataType == IntegerType)
      if (!fusible) {
        logDebug(
          "Fused grouping-set aggregation: plan shape is not fusible, keeping the merge stage")
      }
      fusible
    }

    val newExpandOutput = agg.resultExpressions.map(_.toAttribute)
    val newExpandProjections =
      buildPostExpandProjections(expand.projections, expand.output, newExpandOutput)

    // DUPLICATE GROUPING SETS. GroupingSetAggregationNode rejects two sets with the same key mask
    // outright (VELOX_CHECK "Duplicate grouping-set mask"), because at the same grain each would be
    // a separate lattice root redoing identical work. Spark reaches here with duplicates from e.g.
    // GROUPING SETS ((a), (a)), which is legal SQL and must keep both result rows -- the existing
    // suite has a test for exactly that. Without this check the flag turns that query from a
    // correct answer into a native query failure, and because the marker is deliberately withheld
    // during validation there is no fallback to catch it. Checked here rather than in the shape
    // predicate above because it reads the rebuilt projections the native side actually parses.
    //
    // The mask is the set of key slots holding a field reference; a masked-off key is a null
    // literal. Comparing slot sets rather than child channels is faithful because the native side
    // separately checks the slot -> channel map is a stable permutation across all sets.
    val fused = shapeFusible && {
      val masks = newExpandProjections.map(_.take(numKeys - 1).map(_.isInstanceOf[Attribute]))
      val distinctMasks = masks.distinct.length == masks.length
      if (!distinctMasks) {
        logDebug(
          "Fused grouping-set aggregation: duplicate grouping sets, keeping the merge stage")
      }
      distinctMasks
    }

    val newExpand =
      ExpandExecTransformer(newExpandProjections, newExpandOutput, bottomAggregate, fused)
    newExpand.copyTagsFrom(expand)

    val newPreFilter = preFilter.map {
      filter =>
        val newFilter = filter.copy(child = newExpand)
        newFilter.copyTagsFrom(filter)
        newFilter
    }
    val mergeChild = newPreFilter.getOrElse(newExpand)

    // Fused path: the tagged Expand IS the merge stage. The native operator merges every grouping
    // set itself and emits partial states, so the partial-merge aggregate that the three-stage
    // rewrite puts here would be a second, redundant hash pass. Dropping it is output-compatible
    // because the merge aggregate's output attributes are exactly the Expand's output attributes.
    //
    // Note this validates the Expand as if it were untagged: ExpandExecTransformer only emits the
    // marker on the real transform, never during validation, because the native validator sees the
    // Expand in isolation and the fused node's shape is only decidable together with its child.
    //
    // Because doValidate() does not see the tagged shape, Gluten's ordinary fallback-to-vanilla-
    // Spark mechanism does not cover this path. The guards in this rule -- the single-column-buffer
    // check, the slot/permutation checks in shapeFusible, and the duplicate-mask check below -- are
    // hand-maintained Scala mirrors of the native invariants in
    // SubstraitToVeloxPlanConverter::toGroupingSetAggregation.
    //
    // The native-side fallback net has landed (SubstraitToVeloxPlan.cc, the isRollup branch of
    // toVeloxPlan(ExpandRel)): when the tagged shape trips any native check, the converter now
    // DEGRADES to a plain ExpandNode instead of VELOX_CHECK-failing the task. That degradation is
    // always semantically safe -- an ignored marker costs shuffle volume, not correctness, since
    // the post-shuffle Final-mode merge is associative over the duplicated buffers -- so drift
    // between these mirrors and the native checks is now a shuffle-volume cost, not a failure.
    // The mirrors are kept anyway so the common case avoids emitting a marker the native side will
    // only discard. The flag stays experimental and off by default until the fused operator itself
    // ships in the linked Velox build and the fallback is exercised on a cluster.
    if (fused) {
      val fusedNodes: Seq[SparkPlan] =
        reGroundedPreProject.toSeq ++ Seq(bottomAggregate, newExpand) ++ newPreFilter.toSeq
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
