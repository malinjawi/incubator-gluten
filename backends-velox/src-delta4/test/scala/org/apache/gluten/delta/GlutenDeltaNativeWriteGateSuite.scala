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
package org.apache.gluten.delta

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.delta.GlutenDeltaNativeWriteGate
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.shims.VariantShreddingShims
import org.apache.spark.sql.delta.util.TableParquetVersionOption
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class GlutenDeltaNativeWriteGateSuite extends SharedSparkSession {

  private val plainSchema = new StructType().add("id", LongType).add("s", StringType)
  private val partitionColumn = AttributeReference("part", IntegerType)()

  /** The options Delta 4.4's writeFiles passes for a table with these properties. */
  private def deltaOptions(
      writePartitionColumns: Boolean = true,
      tableProperties: Map[String, String] = Map.empty,
      writerOptions: Map[String, String] = Map.empty): Map[String, String] = {
    val filtered = writerOptions + ("writePartitionColumns" -> writePartitionColumns.toString)
    filtered ++
      VariantShreddingShims.getVariantInferShreddingSchemaOptions(false) ++
      TableParquetVersionOption.getWriterOptions(spark, filtered, tableProperties)
  }

  private def unsupported(
      options: Map[String, String] = deltaOptions(),
      schema: StructType = plainSchema,
      partitioned: Boolean = false,
      configuration: Map[String, String] = Map.empty): Option[String] =
    GlutenDeltaNativeWriteGate.unsupportedWrite(
      spark.sessionState.conf,
      options,
      schema,
      if (partitioned) Seq(partitionColumn) else Seq.empty,
      Metadata(configuration = configuration))

  test("Delta's default options for a plain table are written natively") {
    assert(unsupported().isEmpty)
    for (codec <- Seq("uncompressed", "snappy", "gzip", "lz4", "zstd")) {
      assert(unsupported(deltaOptions(writerOptions = Map("compression" -> codec))).isEmpty, codec)
    }
  }

  test("codecs the Velox writer does not map fall back, from the option or the session") {
    assert(unsupported(deltaOptions(writerOptions = Map("compression" -> "lz4_raw"))).isDefined)
    assert(unsupported(deltaOptions(writerOptions = Map("compression" -> "brotli"))).isDefined)
    withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "lz4_raw") {
      assert(unsupported().isDefined)
    }
  }

  test("maxRecordsPerFile falls back for unpartitioned writes, from the option or the session") {
    val limited = deltaOptions(writerOptions = Map("maxRecordsPerFile" -> "10"))
    assert(unsupported(limited).isDefined)
    withSQLConf(SQLConf.MAX_RECORDS_PER_FILE.key -> "10") {
      assert(unsupported().isDefined)
    }
    // The partitioned native writer splits batches by the limit.
    val partitionedLimited = deltaOptions(
      writePartitionColumns = false,
      writerOptions = Map("maxRecordsPerFile" -> "10"))
    assert(unsupported(partitionedLimited, partitioned = true).isEmpty)
  }

  test("partition columns in data files fall back") {
    assert(unsupported(partitioned = true).isDefined)
    assert(unsupported(deltaOptions(writePartitionColumns = false), partitioned = true).isEmpty)
    withSQLConf(SQLConf.MAX_CONCURRENT_OUTPUT_FILE_WRITERS.key -> "2") {
      assert(
        unsupported(deltaOptions(writePartitionColumns = false), partitioned = true).isDefined)
    }
  }

  test("timestamps fall back unless both the session and the option ask for microseconds") {
    val withTimestamp = plainSchema.add("ts", TimestampType)
    // Spark's default is INT96.
    assert(unsupported(schema = withTimestamp).isDefined)
    assert(
      unsupported(
        deltaOptions(writerOptions =
          Map(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS")),
        withTimestamp).isDefined)
    withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
      assert(unsupported(schema = withTimestamp).isEmpty)
      assert(unsupported(schema = new StructType().add("a", ArrayType(TimestampType))).isEmpty)
      assert(
        unsupported(
          deltaOptions(writerOptions = Map(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96")),
          withTimestamp).isDefined)
    }
    withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MILLIS") {
      assert(unsupported(schema = new StructType().add("a", ArrayType(TimestampType))).isDefined)
    }
  }

  test("a datetime rebase mode other than CORRECTED falls back for dates and timestamps") {
    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "LEGACY") {
      assert(unsupported(schema = plainSchema.add("d", DateType)).isDefined)
      assert(unsupported().isEmpty)
    }
  }

  test("the legacy Parquet layout falls back for arrays and maps") {
    withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "true") {
      assert(unsupported(schema = plainSchema.add("a", ArrayType(IntegerType))).isDefined)
      assert(unsupported(schema = plainSchema.add("m", MapType(StringType, IntegerType))).isDefined)
      assert(unsupported().isEmpty)
    }
  }

  test("Parquet v2 falls back") {
    assert(
      unsupported(
        deltaOptions(tableProperties = Map("delta.parquet.format.version" -> "2.12.0"))).isDefined)
    assert(
      unsupported(deltaOptions(writerOptions = Map("parquet.writer.version" -> "v2"))).isDefined)
  }

  test("variant shredding and unknown options fall back") {
    assert(
      unsupported(
        deltaOptions() ++ VariantShreddingShims.getVariantInferShreddingSchemaOptions(true))
        .isDefined == VariantShreddingShims.getVariantInferShreddingSchemaOptions(true).nonEmpty)
    assert(unsupported(deltaOptions() + ("someNewDeltaOption" -> "x")).isDefined)
  }

  test("column mapping and Iceberg compatibility fall back") {
    for (mode <- Seq("name", "id")) {
      assert(unsupported(configuration = Map("delta.columnMapping.mode" -> mode)).isDefined, mode)
    }
    assert(
      unsupported(configuration =
        Map("delta.columnMapping.mode" -> "name", "delta.enableIcebergCompatV2" -> "true"))
        .isDefined)
  }

  test("VOID and TIMESTAMP_NTZ columns fall back, at any depth") {
    assert(GlutenDeltaNativeWriteGate.unsupportedSchema(plainSchema).isEmpty)
    for (
      dataType <- Seq(
        NullType,
        new StructType().add("v", NullType),
        ArrayType(NullType),
        TimestampNTZType,
        new StructType().add("t", TimestampNTZType),
        MapType(StringType, TimestampNTZType)
      )
    ) {
      val schema = plainSchema.add("c", dataType)
      assert(GlutenDeltaNativeWriteGate.unsupportedSchema(schema).isDefined, dataType)
      assert(unsupported(schema = schema).isDefined, dataType)
    }
  }
}
