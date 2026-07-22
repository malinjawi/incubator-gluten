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

import org.apache.gluten.config.VeloxConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan

/**
 * Planning-only coverage for the fused grouping-set aggregation path.
 *
 * The tests assert on the planned tree and never execute the queries, because the fused
 * ExpandExecTransformer lowers to a native operator that a stock Velox build does not provide.
 * Planning is safe: the marker is only attached during the real Substrait transform, never during
 * native validation, so validation still sees an ordinary ExpandRel.
 */
class FusedGroupingSetAggregateSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.memory.offHeap.size", "2g")
      // Without AQE the executedPlan is already the final columnar plan, so the assertions below
      // hold without running the query.
      .set("spark.sql.adaptive.enabled", "false")
      .set(VeloxConfig.VELOX_LAZY_AGGREGATE_EXPAND_ENABLED.key, "true")
  }

  private val rollupQuery =
    "select l_orderkey, l_partkey, sum(l_suppkey), count(l_suppkey) from lineitem " +
      "group by rollup(l_orderkey, l_partkey)"

  private def plannedNodes(query: String): Seq[SparkPlan] = {
    val plan = spark.sql(query).queryExecution.executedPlan
    plan.collect { case p => p }
  }

  private def expands(nodes: Seq[SparkPlan]): Seq[ExpandExecTransformer] = nodes.collect {
    case e: ExpandExecTransformer if e.child.isInstanceOf[FlushableHashAggregateExecTransformer] =>
      e
  }

  test("expand is tagged and the merge stage is dropped when the fused path is on") {
    withSQLConf(VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true") {
      val nodes = plannedNodes(rollupQuery)
      val lazyExpands = expands(nodes)
      assert(lazyExpands.nonEmpty, s"lazy expand did not fire:\n${nodes.head}")
      assert(
        lazyExpands.forall(_.groupingSetAggregation),
        s"expected the expand to carry the fused marker:\n${nodes.head}")
      // The fused operator does the merging, so no aggregate may sit directly above the expand.
      val mergeAbove = nodes.exists {
        case a: HashAggregateExecBaseTransformer => lazyExpands.exists(e => a.child eq e)
        case _ => false
      }
      assert(!mergeAbove, s"expected no merge aggregate above the fused expand:\n${nodes.head}")
    }
  }

  test("expand is untagged and the merge stage is kept when the fused path is off") {
    withSQLConf(VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "false") {
      val nodes = plannedNodes(rollupQuery)
      val lazyExpands = expands(nodes)
      assert(lazyExpands.nonEmpty, s"lazy expand did not fire:\n${nodes.head}")
      assert(
        lazyExpands.forall(!_.groupingSetAggregation),
        s"expected the expand to carry no fused marker:\n${nodes.head}")
      val mergeAbove = nodes.exists {
        case a: HashAggregateExecBaseTransformer => lazyExpands.exists(e => a.child eq e)
        case _ => false
      }
      assert(mergeAbove, s"expected the merge aggregate above the expand:\n${nodes.head}")
    }
  }

  // VeloxHashAggregateExecTransformer.getAggRel interposes a ProjectRel above the AggregateRel
  // whenever a Partial-mode function has more than one aggBufferAttribute. The native fused
  // converter requires the Expand's Substrait input to be an AggregateRel and would hard-fail
  // (VELOX_CHECK, with no fallback because the marker is withheld during validation), so the
  // rewrite must keep the merge stage for these queries.
  private val multiColumnBufferQueries = Seq(
    "avg" ->
      ("select l_orderkey, l_partkey, avg(l_suppkey), count(l_suppkey) from lineitem " +
        "group by rollup(l_orderkey, l_partkey)"),
    "decimal sum" ->
      ("select l_orderkey, l_partkey, sum(cast(l_extendedprice as decimal(12,2))) from lineitem " +
        "group by rollup(l_orderkey, l_partkey)")
  )

  multiColumnBufferQueries.foreach {
    case (label, query) =>
      test(s"fused path is not taken for a multi-column partial buffer ($label)") {
        withSQLConf(VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true") {
          val nodes = plannedNodes(query)
          val lazyExpands = expands(nodes)
          assert(lazyExpands.nonEmpty, s"lazy expand did not fire:\n${nodes.head}")
          assert(
            lazyExpands.forall(!_.groupingSetAggregation),
            s"expected no fused marker for a multi-column partial buffer:\n${nodes.head}")
          val mergeAbove = nodes.exists {
            case a: HashAggregateExecBaseTransformer => lazyExpands.exists(e => a.child eq e)
            case _ => false
          }
          assert(mergeAbove, s"expected the merge aggregate to be kept:\n${nodes.head}")
        }
      }
  }

  test("fused path stays off when the lazy expand rewrite itself is off") {
    withSQLConf(
      VeloxConfig.VELOX_LAZY_AGGREGATE_EXPAND_ENABLED.key -> "false",
      VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true") {
      val nodes = plannedNodes(rollupQuery)
      assert(
        nodes.collect { case e: ExpandExecTransformer if e.groupingSetAggregation => e }.isEmpty,
        s"expected no fused expand when the lazy rewrite is disabled:\n${nodes.head}"
      )
    }
  }

  // Duplicate grouping sets are legal SQL and must keep both result rows. The native
  // GroupingSetAggregationNode rejects two sets with the same key mask, so the rule must not tag
  // the expand for these; the merge stage stays and the ordinary three-stage rewrite handles it.
  test("fused path is not taken for duplicate grouping sets") {
    val duplicateQuery =
      "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
        "group by grouping sets ((l_orderkey, l_partkey), (l_orderkey, l_partkey))"
    withSQLConf(VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true") {
      val nodes = plannedNodes(duplicateQuery)
      val lazyExpands = expands(nodes)
      assert(lazyExpands.nonEmpty, s"lazy expand did not fire:\n${nodes.head}")
      assert(
        lazyExpands.forall(!_.groupingSetAggregation),
        s"expected no fused marker for duplicate grouping sets:\n${nodes.head}")
      val mergeAbove = nodes.exists {
        case a: HashAggregateExecBaseTransformer => lazyExpands.exists(e => a.child eq e)
        case _ => false
      }
      assert(mergeAbove, s"expected the merge aggregate to be kept:\n${nodes.head}")
    }
  }

  // The rewrite invariant: whatever shape the fused path produces, the columnar plan's output
  // attributes (names and types) must equal those of the untouched plan, so nothing above the
  // matched aggregate needs adjusting. Compares the fused plan's output against the plan produced
  // with the lazy-expand rewrite disabled entirely (the original).
  test("fused rewrite preserves the plan output attributes") {
    def outputSchema(fusedOn: Boolean): Seq[(String, String)] = {
      withSQLConf(
        VeloxConfig.VELOX_LAZY_AGGREGATE_EXPAND_ENABLED.key -> fusedOn.toString,
        VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> fusedOn.toString) {
        spark.sql(rollupQuery).queryExecution.executedPlan.output
          .map(a => (a.name, a.dataType.catalogString))
      }
    }
    // Sanity: the fused marker actually fires under the fused config.
    withSQLConf(
      VeloxConfig.VELOX_LAZY_AGGREGATE_EXPAND_ENABLED.key -> "true",
      VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true") {
      assert(
        expands(plannedNodes(rollupQuery)).exists(_.groupingSetAggregation),
        "fused marker did not fire; the preservation check would be vacuous")
    }
    assert(
      outputSchema(fusedOn = true) == outputSchema(fusedOn = false),
      "fused rewrite changed the plan output attributes")
  }
}
