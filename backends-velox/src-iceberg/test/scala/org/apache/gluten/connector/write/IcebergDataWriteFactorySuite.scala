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
package org.apache.gluten.connector.write

import org.apache.gluten.proto.IcebergNestedField

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import org.apache.iceberg.{PartitionSpec, SortOrder}
import org.scalatest.funsuite.AnyFunSuite

import java.util

class IcebergDataWriteFactorySuite extends AnyFunSuite {

  test("native Iceberg streaming writer requires stable query and epoch operation ids") {
    val factory = makeFactory("query-123")

    assert(factory.operationIdForEpoch(0L) == "query-123-0")
    assert(factory.operationIdForEpoch(17L) == "query-123-17")
    assert(factory.validateWriterIdentity(partitionId = 3, taskId = 99L, epochId = 17L) ==
      "query-123-17")

    val negativeEpochError = intercept[IllegalArgumentException] {
      factory.operationIdForEpoch(-1L)
    }
    assert(negativeEpochError.getMessage.contains("non-negative epoch id"))
  }

  test("native Iceberg streaming writer rejects invalid Spark task identity before native init") {
    val factory = makeFactory("query-456")

    val negativePartitionError = intercept[IllegalArgumentException] {
      factory.validateWriterIdentity(partitionId = -1, taskId = 99L, epochId = 0L)
    }
    assert(negativePartitionError.getMessage.contains("non-negative partition id"))

    val negativeTaskError = intercept[IllegalArgumentException] {
      factory.validateWriterIdentity(partitionId = 0, taskId = -1L, epochId = 0L)
    }
    assert(negativeTaskError.getMessage.contains("non-negative task id"))

    val negativeEpochError = intercept[IllegalArgumentException] {
      factory.validateWriterIdentity(partitionId = 0, taskId = 99L, epochId = -1L)
    }
    assert(negativeEpochError.getMessage.contains("non-negative epoch id"))
  }

  test("native Iceberg streaming writer rejects missing query id before native writer init") {
    val emptyQueryIdError = intercept[IllegalArgumentException] {
      makeFactory("")
    }
    assert(emptyQueryIdError.getMessage.contains("non-empty query id"))

    val nullQueryIdError = intercept[IllegalArgumentException] {
      makeFactory(null)
    }
    assert(nullQueryIdError.getMessage.contains("non-empty query id"))
  }

  private def makeFactory(queryId: String): IcebergDataWriteFactory = {
    IcebergDataWriteFactory(
      schema = StructType(Seq(StructField("id", IntegerType))),
      format = 1,
      directory = "/tmp/native-iceberg-writer",
      codec = "zstd",
      partitionSpec = PartitionSpec.unpartitioned(),
      sortOrder = SortOrder.unsorted(),
      field = IcebergNestedField.newBuilder().build(),
      icebergProperties = new util.HashMap[String, String](),
      queryId = queryId
    )
  }
}
