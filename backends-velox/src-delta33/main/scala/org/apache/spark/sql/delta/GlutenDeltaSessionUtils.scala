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
package org.apache.spark.sql.delta

import org.apache.gluten.config.{GlutenConfig, GlutenCoreConfig}

import org.apache.spark.sql.SparkSession

private[delta] object GlutenDeltaSessionUtils {

  private val GlutenControlPlaneConfKeys = Seq(
    GlutenCoreConfig.GLUTEN_ENABLED.key,
    GlutenConfig.NATIVE_VALIDATION_ENABLED.key
  )

  /**
   * Delta metadata and checksum helper queries read transaction-log JSON and invoke expressions
   * that Gluten intentionally does not optimize well today. Run those helpers with Gluten disabled
   * so the benchmarked native DML and scan path doesn't pay repeated validation and fallback cost.
   */
  def withGlutenDisabled[T](spark: SparkSession)(body: => T): T = {
    val runtimeConf = spark.conf
    val original = GlutenControlPlaneConfKeys.map(key => key -> runtimeConf.getOption(key)).toMap

    try {
      runtimeConf.set(GlutenCoreConfig.GLUTEN_ENABLED.key, "false")
      runtimeConf.set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      body
    } finally {
      original.foreach {
        case (key, Some(value)) => runtimeConf.set(key, value)
        case (key, None) => runtimeConf.unset(key)
      }
    }
  }
}
