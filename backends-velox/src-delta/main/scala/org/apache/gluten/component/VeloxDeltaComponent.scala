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
package org.apache.gluten.component

import org.apache.gluten.backendsapi.velox.VeloxBackend
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.{DeltaPostTransformRules, OffloadDeltaFilter, OffloadDeltaProject, OffloadDeltaScan}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode._
import org.apache.gluten.extension.columnar.validator.Validator
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector
import org.apache.gluten.execution.GlutenPlan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.SparkReflectionUtil

class VeloxDeltaComponent extends Component {
  override def name(): String = "velox-delta"

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil

  override def isRuntimeCompatible: Boolean = {
    SparkReflectionUtil.isClassPresent("io.delta.sql.DeltaSparkSessionExtension")
  }

  override def injectRules(injector: Injector): Unit = {
    val legacy = injector.gluten.legacy
    legacy.injectTransform {
      c =>
        val deltaScan = OffloadDeltaScan()
        val deltaProject = OffloadDeltaProject()
        val deltaFilter = OffloadDeltaFilter()
        val offloadRules = Seq(deltaScan, deltaProject, deltaFilter).map(_.toStrcitRule())
        DeltaTransform(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offloadRules),
          offloadRules,
          plan => deltaScan.isCandidate(plan) || deltaProject.isCandidate(plan) ||
            deltaFilter.isCandidate(plan))
    }
    DeltaPostTransformRules.rules.foreach(r => legacy.injectPostTransform(_ => r))
  }
}

private case class DeltaTransform(
    validator: Validator,
    offloadRules: Seq[OffloadSingleNode],
    isCandidate: SparkPlan => Boolean)
  extends Rule[SparkPlan]
  with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case node if isDeltaOffloadCandidate(node) =>
        validator.validate(node) match {
          case Validator.Passed =>
            offloadRules.foldLeft(node: SparkPlan) {
              case (current, rule) => rule.offloadAndPropagateTag(current)
            }
          case Validator.Failed(reason) =>
            logDebug(s"Validation failed by reason: $reason on query plan: ${node.nodeName}")
            if (FallbackTags.maybeOffloadable(node)) {
              FallbackTags.add(node, reason)
            }
            node
        }
      case node =>
        node
    }
  }

  private def isDeltaOffloadCandidate(plan: SparkPlan): Boolean = {
    !plan.isInstanceOf[GlutenPlan] && isCandidate(plan)
  }
}
