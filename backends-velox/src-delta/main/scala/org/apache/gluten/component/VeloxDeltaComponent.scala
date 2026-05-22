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
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

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
        val scan = OffloadDeltaScan()
        val project = OffloadDeltaProject()
        val filter = OffloadDeltaFilter()
        val shouldValidate: SparkPlan => Boolean =
          plan => scan.isCandidate(plan) || project.isCandidate(plan) || filter.isCandidate(plan)
        val offload = Seq(scan, project, filter).map(_.toStrcitRule())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload,
          validateOncePerNode = true,
          shouldValidate = shouldValidate)
    }
    legacy.injectTransform(_ => DeltaPostTransformRules.nativeDeletionVectorRule)
    DeltaPostTransformRules.rules.foreach(r => legacy.injectPostTransform(_ => r))
  }
}
