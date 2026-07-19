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
package org.apache.gluten.execution.streaming

import org.apache.spark.sql.execution.SparkPlan

/**
 * Marker for Gluten-native Structured Streaming stateful physical operators.
 *
 * This marker is intentionally narrower than the native StateStore provider: a provider can back
 * Spark-owned stateful operators, while this marker means the physical operator itself owns native
 * streaming state semantics and has a separately guarded compatibility contract.
 */
trait NativeStreamingStatefulOperatorExec {
  self: SparkPlan =>

  def nativeStatefulOperatorKind: String
}

object NativeStreamingStatefulOperatorExec {
  val Deduplicate: String = "deduplicate"
  val DeduplicateWithinWatermark: String = "deduplicateWithinWatermark"
  val CountAggregation: String = "countAggregation"
  val LongSumAggregation: String = "longSumAggregation"
  val DeduplicateOperatorKinds: Set[String] = Set(Deduplicate, DeduplicateWithinWatermark)
  val AggregationOperatorKinds: Set[String] = Set(CountAggregation, LongSumAggregation)
}
