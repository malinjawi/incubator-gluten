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
package org.apache.gluten.execution.kafka

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.MicroBatchScanExecTransformer

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.StringType

import org.scalatest.funsuite.AnyFunSuite

class GlutenKafkaSchemaContractSuite extends AnyFunSuite {

  test("native Kafka micro-batch scan accepts Spark Kafka schema projections") {
    MicroBatchScanExecTransformer.validateKafkaReadSchema(kafkaAttributes(includeHeaders = false))
    MicroBatchScanExecTransformer.validateKafkaReadSchema(
      kafkaAttributes(includeHeaders = false).filter(attribute => attribute.name == "value"))
    MicroBatchScanExecTransformer.validateKafkaReadSchema(kafkaAttributes(includeHeaders = true))
  }

  test("native Kafka micro-batch scan rejects unsupported schema shapes") {
    val unsupportedColumnError = intercept[GlutenNotSupportException] {
      MicroBatchScanExecTransformer.validateKafkaReadSchema(
        Seq(AttributeReference("payload", StringType, nullable = true)()))
    }
    assert(unsupportedColumnError.getMessage.contains("unsupported column"))

    val wrongType = kafkaAttributes(includeHeaders = false).map {
      case attribute if attribute.name == "value" =>
        AttributeReference("value", StringType, nullable = attribute.nullable)()
      case attribute => attribute
    }
    val wrongTypeError = intercept[GlutenNotSupportException] {
      MicroBatchScanExecTransformer.validateKafkaReadSchema(wrongType)
    }
    assert(wrongTypeError.getMessage.contains("column 'value'"))

    val duplicateError = intercept[GlutenNotSupportException] {
      MicroBatchScanExecTransformer.validateKafkaReadSchema(
        Seq(kafkaAttribute("value"), kafkaAttribute("value")))
    }
    assert(duplicateError.getMessage.contains("duplicates: value"))

    val reorderedError = intercept[GlutenNotSupportException] {
      MicroBatchScanExecTransformer.validateKafkaReadSchema(
        Seq(kafkaAttribute("value"), kafkaAttribute("key")))
    }
    assert(reorderedError.getMessage.contains("column order"))
  }

  private def kafkaAttributes(includeHeaders: Boolean): Seq[AttributeReference] = {
    val fields = MicroBatchScanExecTransformer.SparkKafkaReadSchema.fields.filter {
      field => includeHeaders || field.name != "headers"
    }
    fields.map {
      field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    }.toSeq
  }

  private def kafkaAttribute(name: String): AttributeReference = {
    kafkaAttributes(includeHeaders = true).find(_.name == name).getOrElse {
      throw new IllegalArgumentException(s"Unknown Kafka field $name")
    }
  }
}
