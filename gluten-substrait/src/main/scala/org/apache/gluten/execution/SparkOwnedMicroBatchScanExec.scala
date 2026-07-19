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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.{LeafExecNode, StreamSourceAwareSparkPlan}
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec

/**
 * Row-only wrapper for Spark-owned streaming sources.
 *
 * Spark remains responsible for micro-batch offset planning, replay, checkpointing, and source
 * commit semantics. Gluten can still insert RowToColumnar above this node to run a native stateless
 * fragment without absorbing the streaming source into a Velox TableScan.
 */
case class SparkOwnedMicroBatchScanExec(scan: MicroBatchScanExec)
  extends LeafExecNode
  with StreamSourceAwareSparkPlan {
  override def output: Seq[Attribute] = scan.output

  override def outputPartitioning: Partitioning = scan.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = scan.outputOrdering

  override lazy val metrics = scan.metrics

  override def getStream: Option[SparkDataStream] = scan.getStream

  override def nodeName: String = s"SparkOwned${scan.nodeName}"

  override protected def doExecute(): RDD[InternalRow] = scan.execute()
}
