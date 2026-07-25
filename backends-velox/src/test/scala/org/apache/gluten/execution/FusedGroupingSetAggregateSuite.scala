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
import org.apache.gluten.jni.JniLibLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan

/**
 * Planning and Linux execution coverage for the fused grouping-set aggregation path.
 *
 * The execution tests first convert and inspect the real native plan. On macOS they are canceled
 * before execution because this pinned native build crashes on the corresponding stock input paths;
 * detailed native operator correctness is covered by MultiGroupingSetAggregationTest.
 */
class FusedGroupingSetAggregateSuite extends VeloxWholeStageTransformerSuite {
  private val commonLibPathProperty = "gluten.rollup.test.commonLibPath"
  private val isMacOS = System.getProperty("os.name").startsWith("Mac OS X") ||
    System.getProperty("os.name").startsWith("macOS")

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    Option(System.getProperty(commonLibPathProperty))
      .filter(_.nonEmpty)
      .foreach(JniLibLoader.loadFromPath)
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.memory.offHeap.size", "2g")
      // Spark 4 enables ANSI mode by default, which the Velox backend rejects.
      .set("spark.sql.ansi.enabled", "false")
      // Make executedPlan directly inspectable without running the query.
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

  // Spark 3.5's QueryTest.withSQLConf returns Unit, while Spark 4 preserves the
  // block's result. Capture it explicitly so value-producing test helpers compile
  // and behave the same way on both supported Spark lines.
  private def withSQLConfResult[T](pairs: (String, String)*)(block: => T): T = {
    var result = Option.empty[T]
    withSQLConf(pairs: _*) {
      result = Some(block)
    }
    result.getOrElse(fail("withSQLConf did not evaluate its block"))
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

  test(
    "measure-less rollup keeps the merge stage instead of entering native distinct aggregation") {
    val query =
      "select l_orderkey, l_partkey from lineitem group by rollup(l_orderkey, l_partkey)"
    withSQLConf(VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true") {
      val nodes = plannedNodes(query)
      val lazyExpands = expands(nodes)
      assert(lazyExpands.nonEmpty, s"lazy expand did not fire:\n${nodes.head}")
      assert(
        lazyExpands.forall(!_.groupingSetAggregation),
        s"measure-less grouping sets must not carry the fused marker:\n${nodes.head}")
      val mergeAbove = nodes.exists {
        case a: HashAggregateExecBaseTransformer => lazyExpands.exists(e => a.child eq e)
        case _ => false
      }
      assert(mergeAbove, s"expected the distinct merge aggregate above the expand:\n${nodes.head}")
    }
  }

  // Multi-column buffers introduce a ProjectRel between the child AggregateRel and ExpandRel. The
  // native converter must use the packed AggregateRel state and replay that Project afterward.
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
      test(s"fused path accepts a multi-column partial buffer ($label)") {
        withSQLConf(VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true") {
          val nodes = plannedNodes(query)
          val lazyExpands = expands(nodes)
          assert(lazyExpands.nonEmpty, s"lazy expand did not fire:\n${nodes.head}")
          assert(
            lazyExpands.forall(_.groupingSetAggregation),
            s"expected a fused marker for a multi-column partial buffer:\n${nodes.head}")
          val mergeAbove = nodes.exists {
            case a: HashAggregateExecBaseTransformer => lazyExpands.exists(e => a.child eq e)
            case _ => false
          }
          assert(!mergeAbove, s"expected the merge aggregate to be dropped:\n${nodes.head}")
          assertFusedNativePlan(nodes, finestSetBypass = false, expectedGroupingSets = 3)
        }
      }
  }

  test("strict floating-point mode rejects an integral average with a double sum buffer") {
    val rollup =
      "select l_orderkey, l_partkey, avg(l_suppkey) from lineitem " +
        "group by rollup(l_orderkey, l_partkey)"
    val ordinaryGroupBy =
      "select l_orderkey, avg(l_suppkey) from lineitem group by l_orderkey"
    withSQLConf(
      VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true",
      VeloxConfig.FLOATING_POINT_MODE.key -> "strict") {
      val nodes = plannedNodes(rollup)
      assert(
        nodes.collect { case e: ExpandExecTransformer if e.groupingSetAggregation => e }.isEmpty,
        s"strict mode must not reassociate an average's DOUBLE accumulator:\n${nodes.head}"
      )

      val ordinaryNodes = plannedNodes(ordinaryGroupBy)
      assert(
        ordinaryNodes.exists(_.isInstanceOf[RegularHashAggregateExecTransformer]),
        s"strict integral average did not retain regular aggregation:\n${ordinaryNodes.head}"
      )
      assert(
        ordinaryNodes.forall(!_.isInstanceOf[FlushableHashAggregateExecTransformer]),
        s"strict integral average entered flushable aggregation:\n${ordinaryNodes.head}"
      )
    }
  }

  private val q67ShapeQuery =
    """
      |with input as (
      |  select
      |    cast(case when id < 3 then 1 else 2 end as int) as k1,
      |    cast(case when id < 3 then 1 else 2 end as int) as k2,
      |    cast(case when id < 2 then 1 else 2 end as int) as k3,
      |    cast(case when id < 2 then 1 else 2 end as int) as k4,
      |    case when id < 2 then 'R' when id = 2 then 'A' else null end as s1,
      |    case when id < 2 then 'O' else 'F' end as s2,
      |    case when id < 2 then 'AIR' else 'SHIP' end as s3,
      |    case
      |      when id < 2 then 'DELIVER IN PERSON'
      |      when id = 2 then null
      |      else 'TAKE BACK RETURN'
      |    end as s4,
      |    cast(
      |      case id when 0 then 10.25 when 1 then 20.50 when 2 then 30.75 else null end
      |      as decimal(12, 2)) as price,
      |    cast(
      |      case id when 0 then 0.90 when 2 then 0.80 when 3 then 0.75 else null end
      |      as decimal(5, 2)) as discount
      |  from range(0, 5)
      |)
      |select k1, k2, k3, k4, s1, s2, s3, s4,
      |  sum(price * coalesce(discount, cast(1.00 as decimal(5, 2)))) as revenue
      |from input
      |group by rollup(k1, k2, k3, k4, s1, s2, s3, s4)
      |""".stripMargin

  private def assertFusedNativePlan(
      nodes: Seq[SparkPlan],
      finestSetBypass: Boolean,
      expectedGroupingSets: Int): Seq[String] = {
    assert(
      expands(nodes).exists(_.groupingSetAggregation),
      s"decimal rollup did not use the fused route:\n${nodes.head}")
    val nativePlans = nodes.collect {
      case w: WholeStageTransformer => w.nativePlanString()
    }
    val fusedPlans = nativePlans.filter(_.contains("GroupingSetAggregation"))
    assert(
      fusedPlans.nonEmpty,
      s"no native GroupingSetAggregation found:\n${nativePlans.mkString("\n")}")
    assert(
      fusedPlans.forall(!_.contains("Expand")),
      s"fused native stage still contains Expand:\n${fusedPlans.mkString("\n")}")
    val nativeGroupingSetCount =
      "/gid=".r.findAllIn(fusedPlans.mkString("\n")).length
    assert(
      nativeGroupingSetCount == expectedGroupingSets,
      s"expected $expectedGroupingSets native grouping sets, found $nativeGroupingSetCount:\n" +
        fusedPlans.mkString("\n")
    )
    assert(
      fusedPlans.exists(_.contains("bypass: 0")) == finestSetBypass,
      s"unexpected native bypass detail for enabled=$finestSetBypass:\n" +
        fusedPlans.mkString("\n")
    )
    fusedPlans
  }

  private val fusedSmokeQuery =
    "select l_orderkey, l_partkey, sum(cast(l_suppkey as bigint)) from lineitem " +
      "group by rollup(l_orderkey, l_partkey)"

  test("small fused rollup executes through JNI and matches vanilla Spark") {
    Seq(false, true).foreach {
      finestSetBypass =>
        withSQLConf(
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true",
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_MAX_GROUPING_SETS.key -> "4",
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_FINEST_SET_BYPASS_ENABLED.key ->
            finestSetBypass.toString
        ) {
          assertFusedNativePlan(
            plannedNodes(fusedSmokeQuery),
            finestSetBypass,
            expectedGroupingSets = 3)

          if (!isMacOS) {
            runQueryAndCompare(fusedSmokeQuery) {
              df =>
                val nodes = df.queryExecution.executedPlan.collect { case p => p }
                assertFusedNativePlan(nodes, finestSetBypass, expectedGroupingSets = 3)
            }
          }
        }
    }

    if (isMacOS) {
      cancel(
        "macOS stock fused=false Parquet execution SIGSEGVs in " +
          "StringIdMap::release during Parquet/TableScan teardown; " +
          "Linux retains the execution gate")
    }
  }

  test("q67-shaped mixed eight-key decimal rollup executes through the fused native operator") {
    Seq(false, true).foreach {
      finestSetBypass =>
        withSQLConf(
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true",
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_MAX_GROUPING_SETS.key -> "16",
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_FINEST_SET_BYPASS_ENABLED.key ->
            finestSetBypass.toString
        ) {
          assertFusedNativePlan(
            plannedNodes(q67ShapeQuery),
            finestSetBypass,
            expectedGroupingSets = 9)

          if (!isMacOS) {
            runQueryAndCompare(q67ShapeQuery) {
              df =>
                val nodes = df.queryExecution.executedPlan.collect { case p => p }
                assertFusedNativePlan(nodes, finestSetBypass, expectedGroupingSets = 9)
            }
          }
        }
    }

    if (isMacOS) {
      cancel(
        "macOS stock lazy=false/fused=false execution aborts in Folly F14Table::rehashImpl " +
          "(hp.second == srcChunk->tag(srcI)); Linux retains the q67-shaped execution gate")
    }
  }

  test("finest-set bypass is conservative by default and explicitly selectable") {
    Seq(false, true).foreach {
      enabled =>
        withSQLConf(
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true",
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_FINEST_SET_BYPASS_ENABLED.key ->
            enabled.toString
        ) {
          val nodes = plannedNodes(rollupQuery)
          val lazyExpands = expands(nodes)
          assert(lazyExpands.nonEmpty, s"lazy expand did not fire:\n${nodes.head}")
          assert(lazyExpands.forall(_.groupingSetAggregation))
          assert(
            lazyExpands.forall(_.groupingSetFinestSetBypass == enabled),
            s"unexpected finest-set bypass routing for enabled=$enabled:\n${nodes.head}")
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

  // Spark permits duplicate grouping sets; the native node requires unique key masks.
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

  test("configured grouping-set limit controls fused routing") {
    Seq(3 -> true, 2 -> false).foreach {
      case (limit, expectedFused) =>
        withSQLConf(
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true",
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_MAX_GROUPING_SETS.key ->
            limit.toString
        ) {
          val nodes = plannedNodes(rollupQuery)
          val lazyExpands = expands(nodes)
          assert(
            lazyExpands.nonEmpty,
            s"lazy expand did not fire with limit $limit:\n${nodes.head}")
          assert(
            lazyExpands.forall(_.groupingSetAggregation == expectedFused),
            s"unexpected fused marker with grouping-set limit $limit:\n${nodes.head}")
          val mergeAbove = nodes.exists {
            case a: HashAggregateExecBaseTransformer => lazyExpands.exists(e => a.child eq e)
            case _ => false
          }
          assert(
            mergeAbove != expectedFused,
            s"unexpected merge routing with grouping-set limit $limit:\n${nodes.head}")
        }
    }
  }

  test("native rejection of a tagged plan falls back to plain Expand") {
    val queries = Seq(
      "direct aggregate" -> rollupQuery,
      "buffer-extraction project" -> multiColumnBufferQueries.collectFirst {
        case ("avg", query) => query
      }.get)

    queries.foreach {
      case (label, query) =>
        // Build and retain a tagged Spark plan under a permissive limit. Lowering the
        // session limit only for native conversion deliberately exercises the C++ safety
        // net instead of the Scala rule's normal pre-conversion rejection.
        val taggedNodes = withSQLConfResult(
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true",
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_MAX_GROUPING_SETS.key -> "4") {
          plannedNodes(query)
        }
        assert(
          expands(taggedNodes).exists(_.groupingSetAggregation),
          s"$label did not retain a tagged Expand:\n${taggedNodes.head}")

        withSQLConf(
          VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_MAX_GROUPING_SETS.key -> "2") {
          val nativePlans = taggedNodes.collect {
            case w: WholeStageTransformer => w.nativePlanString()
          }
          assert(
            nativePlans.exists(_.contains("Expand")),
            s"$label did not fall back to a native Expand:\n${nativePlans.mkString("\n")}")
          assert(
            nativePlans.forall(!_.contains("GroupingSetAggregation")),
            s"$label retained the rejected fused node:\n${nativePlans.mkString("\n")}")
        }
    }
  }

  test("fused path rejects grouping sets with multiple derivation roots") {
    val antichainQuery =
      "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
        "group by grouping sets ((l_orderkey), (l_partkey))"
    withSQLConf(VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true") {
      val nodes = plannedNodes(antichainQuery)
      val lazyExpands = expands(nodes)
      assert(lazyExpands.nonEmpty, s"lazy expand did not fire:\n${nodes.head}")
      assert(
        lazyExpands.forall(!_.groupingSetAggregation),
        s"expected no fused marker for a multi-root shape:\n${nodes.head}")
      val mergeAbove = nodes.exists {
        case a: HashAggregateExecBaseTransformer => lazyExpands.exists(e => a.child eq e)
        case _ => false
      }
      assert(mergeAbove, s"expected the merge aggregate to be retained:\n${nodes.head}")
    }
  }

  test("small single-root cube remains eligible") {
    val cubeQuery =
      "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
        "group by cube(l_orderkey, l_partkey)"
    withSQLConf(
      VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true",
      VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_MAX_GROUPING_SETS.key -> "4") {
      val nodes = plannedNodes(cubeQuery)
      val lazyExpands = expands(nodes)
      assert(lazyExpands.nonEmpty, s"lazy expand did not fire:\n${nodes.head}")
      assert(
        lazyExpands.forall(_.groupingSetAggregation),
        s"expected the four-set cube to carry the fused marker:\n${nodes.head}")
      val mergeAbove = nodes.exists {
        case a: HashAggregateExecBaseTransformer => lazyExpands.exists(e => a.child eq e)
        case _ => false
      }
      assert(!mergeAbove, s"expected no merge aggregate above the fused cube:\n${nodes.head}")
    }
  }

  // Operators above the rewrite depend on the exact analyzed output contract.
  test("fused rewrite preserves the plan output attributes") {
    def outputSignatures(
        fusedOn: Boolean): (
        Seq[(String, String, Boolean, String, Seq[String], String)],
        Seq[(String, String, Boolean, String, Seq[String], String)]) = {
      withSQLConfResult(
        VeloxConfig.VELOX_LAZY_AGGREGATE_EXPAND_ENABLED.key -> fusedOn.toString,
        VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> fusedOn.toString) {
        val queryExecution = spark.sql(rollupQuery).queryExecution
        def signature(attributes: Seq[org.apache.spark.sql.catalyst.expressions.Attribute]) = {
          attributes.map {
            attribute =>
              (
                attribute.name,
                attribute.dataType.catalogString,
                attribute.nullable,
                attribute.exprId.toString,
                attribute.qualifier,
                attribute.metadata.json)
          }
        }
        (signature(queryExecution.analyzed.output), signature(queryExecution.executedPlan.output))
      }
    }

    // Ensure the contract comparison exercises the fused plan.
    withSQLConf(
      VeloxConfig.VELOX_LAZY_AGGREGATE_EXPAND_ENABLED.key -> "true",
      VeloxConfig.VELOX_FUSED_GROUPING_SET_AGGREGATE_ENABLED.key -> "true") {
      assert(
        expands(plannedNodes(rollupQuery)).exists(_.groupingSetAggregation),
        "fused marker did not fire; the preservation check would be vacuous")
    }
    Seq(false, true).foreach {
      fusedOn =>
        val (analyzedOutput, physicalOutput) = outputSignatures(fusedOn)
        assert(
          physicalOutput == analyzedOutput,
          s"fused=$fusedOn changed name, type, nullability, exprId, qualifier, or metadata:\n" +
            s"analyzed=$analyzedOutput\nphysical=$physicalOutput"
        )
    }
  }
}
