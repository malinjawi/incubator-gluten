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
package org.apache.spark.sql.gluten

import org.apache.gluten.execution.ColumnarToRowExecBase

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{CommandResultExec, SparkPlan}

import scala.reflect.ClassTag

object LakehouseWriteTestUtils {
  def commandPhysicalPlan(df: DataFrame): SparkPlan = {
    df.queryExecution.executedPlan match {
      case command: CommandResultExec => command.commandPhysicalPlan
      case other => other
    }
  }

  def collectPlanNodes[T <: SparkPlan: ClassTag](plan: SparkPlan): Seq[T] = {
    plan.collect { case node: T => node }
  }

  def countColumnarToRowTransitions(df: DataFrame): Int = {
    collectPlanNodes[ColumnarToRowExecBase](commandPhysicalPlan(df)).size
  }
}
