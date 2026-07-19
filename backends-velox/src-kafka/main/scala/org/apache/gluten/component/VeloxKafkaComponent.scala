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
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.SparkReflectionUtil

class VeloxKafkaComponent extends Component {
  override def name(): String = "velox-kafka"

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil

  override def injectRules(injector: Injector): Unit = {
    VeloxKafkaComponent.kafkaOffsetLogHandoffRule().foreach {
      rule =>
        injector.spark.injectResolutionRule(_ => rule)
        injector.spark.injectOptimizerRule(_ => rule)
    }
    injector.gluten.legacy.injectTransform {
      c =>
        val conf = new GlutenConfig(c.sqlConf)
        if (
          !conf.enableNativeStreaming || !conf.enableNativeStreamingKafkaSource ||
          !conf.enableNativeStreamingKafkaSourceExecution
        ) {
          VeloxKafkaComponent.NoopRule
        } else {
          VeloxKafkaComponent.kafkaOffloadRule() match {
            case Some(offload) =>
              HeuristicTransform.Simple(
                Validators.newValidator(conf, offload, c.caller.isStreaming()),
                offload)
            case None => VeloxKafkaComponent.NoopRule
          }
        }
    }
  }
}

object VeloxKafkaComponent {
  private val OffloadKafkaScanClassName = "org.apache.gluten.execution.OffloadKafkaScan"
  private val KafkaOffsetLogHandoffRuleClassName =
    "org.apache.gluten.execution.kafka.GlutenKafkaOffsetLogHandoffRule"

  private object NoopRule extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan
  }

  private def kafkaOffloadRule(): Option[Seq[OffloadSingleNode]] = {
    if (!SparkReflectionUtil.isClassPresent(OffloadKafkaScanClassName)) {
      None
    } else {
      val offload = Class
        .forName(OffloadKafkaScanClassName)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[OffloadSingleNode]
      Some(Seq(offload))
    }
  }

  private def kafkaOffsetLogHandoffRule(): Option[Rule[LogicalPlan]] = {
    if (!SparkReflectionUtil.isClassPresent(KafkaOffsetLogHandoffRuleClassName)) {
      None
    } else {
      Some(Class
        .forName(KafkaOffsetLogHandoffRuleClassName)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[Rule[LogicalPlan]])
    }
  }
}
