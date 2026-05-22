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
package org.apache.gluten.extension.columnar.heuristic

import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.validator.Validator

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import java.util.IdentityHashMap

// Add fallback tags when validator returns negative outcome.
case class AddFallbackTags(
    validator: Validator,
    memoizeValidation: Boolean = false,
    shouldValidate: SparkPlan => Boolean = (_: SparkPlan) => true)
  extends Rule[SparkPlan] {
  private val validationCache =
    if (memoizeValidation) new IdentityHashMap[SparkPlan, Validator.OutCome]() else null

  def apply(plan: SparkPlan): SparkPlan = {
    plan.foreachUp {
      case p if shouldValidate(p) && FallbackTags.maybeOffloadable(p) => addFallbackTag(p)
      case _ =>
    }
    plan
  }

  private def addFallbackTag(plan: SparkPlan): Unit = {
    val outcome = validate(plan)
    outcome match {
      case Validator.Failed(reason) =>
        FallbackTags.add(plan, reason)
      case Validator.Passed =>
    }
  }

  private def validate(plan: SparkPlan): Validator.OutCome = {
    if (!memoizeValidation) {
      return validator.validate(plan)
    }
    val cached = validationCache.get(plan)
    if (cached != null) {
      return cached
    }
    val outcome = validator.validate(plan)
    validationCache.put(plan, outcome)
    outcome
  }
}
