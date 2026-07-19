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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.substrait.rel.{LocalFilesBuilder, SplitInfo, StreamKafkaSourceNode}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat.KafkaReadFormat

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.util.HashMap

class VeloxKafkaScanValidationSuite extends AnyFunSuite {

  test("Velox split envelope preserves local-file split type and payload") {
    val split = LocalFilesBuilder.makeLocalFiles("iterator:0")

    val encoded = VeloxIteratorApi.encodeSplitInfo(split)

    assert(encoded.take(4).sameElements("GLSP".getBytes(StandardCharsets.US_ASCII)))
    assert(encoded(4) == 1.toByte)
    assert(encoded(5) == SplitInfo.Kind.LOCAL_FILES.id())
    assert(encoded.drop(6).sameElements(split.toProtobuf.toByteArray))
  }

  test("Velox split envelope preserves Kafka split type and payload") {
    val params = new HashMap[String, Object]()
    params.put("bootstrap.servers", "localhost:9092")
    val split = new StreamKafkaSourceNode(
      "topic-a",
      1,
      2L,
      5L,
      1000L,
      true,
      false,
      params)

    val encoded = VeloxIteratorApi.encodeSplitInfo(split)

    assert(encoded.take(4).sameElements("GLSP".getBytes(StandardCharsets.US_ASCII)))
    assert(encoded(4) == 1.toByte)
    assert(encoded(5) == SplitInfo.Kind.STREAM_KAFKA.id())
    assert(encoded.drop(6).sameElements(split.toProtobuf.toByteArray))
  }

  test("Velox Kafka scan validation is guarded by native streaming Kafka execution configs") {
    val disabled = withSQLConf() {
      validateKafkaScan()
    }

    assert(!disabled.ok())
    assert(disabled.reason().contains(GlutenConfig.NATIVE_STREAMING_ENABLED.key))
    assert(disabled.reason().contains(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key))
    assert(
      disabled.reason().contains(GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key))

    val enabledOrUnavailable = withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true"
    ) {
      validateKafkaScan()
    }

    if (VeloxBackendSettings.isVeloxKafkaClientAvailable()) {
      assert(enabledOrUnavailable.ok())
    } else {
      assert(!enabledOrUnavailable.ok())
      assert(enabledOrUnavailable.reason().contains("ENABLE_VELOX_KAFKA_CLIENT=ON"))
    }
  }

  test("Velox Kafka scan validation fails early when native Kafka client is unavailable") {
    val reason = withSQLConf(
      GlutenConfig.NATIVE_STREAMING_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_ENABLED.key -> "true",
      GlutenConfig.NATIVE_STREAMING_KAFKA_SOURCE_EXECUTION_ENABLED.key -> "true"
    ) {
      VeloxBackendSettings.validateKafkaReadFormat(
        GlutenConfig.get,
        nativeKafkaClientAvailable = false)
    }

    assert(reason.exists(_.contains("ENABLE_VELOX_KAFKA_CLIENT=ON")))
  }

  private def validateKafkaScan() = {
    VeloxBackendSettings.validateScanExec(
      KafkaReadFormat,
      kafkaSchema.fields,
      kafkaSchema,
      rootPaths = Seq.empty,
      properties = Map.empty,
      new Configuration(),
      partitionFileFormats = Set(KafkaReadFormat)
    )
  }

  private def withSQLConf[T](confs: (String, String)*)(body: => T): T = {
    val sqlConf = new SQLConf()
    confs.foreach { case (key, value) => sqlConf.setConfString(key, value) }
    SQLConf.withExistingConf(sqlConf)(body)
  }

  private val kafkaSchema = StructType(
    Seq(
      StructField("key", BinaryType),
      StructField("value", BinaryType),
      StructField("topic", StringType),
      StructField("partition", IntegerType),
      StructField("offset", LongType),
      StructField("timestamp", TimestampType),
      StructField("timestampType", IntegerType),
      StructField(
        "headers",
        ArrayType(
          StructType(
            Seq(
              StructField("key", StringType),
              StructField("value", BinaryType)))))
    ))
}
