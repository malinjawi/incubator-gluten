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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarStreamingDataWriterFactory, LanceColumnarBatchDataWriterFactory}

import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution
import org.apache.spark.sql.connector.write.{BatchWrite, RequiresDistributionAndOrdering, Write}
import org.apache.spark.sql.types.StructType

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.lance.spark.write.{LanceBatchWrite, SparkWrite}

import scala.collection.JavaConverters._

/**
 * A Lance batch write offloaded to Velox. Executors export their Velox batches as an Arrow C stream
 * that lance-spark writes into uncommitted fragments; lance-spark's [[LanceBatchWrite]] then
 * commits them on the driver, exactly as for its own row writer.
 */
trait LanceWriteExec extends ColumnarV2TableWriteExec {

  /**
   * One [[LanceBatchWrite]] per node. `SparkWrite.toBatch` builds a new one on every call and each
   * pins the table version, so validation, the writer factory and the commit must share it.
   */
  @transient override lazy val batchWrite: BatchWrite = write.toBatch

  private def lanceBatchWrite: LanceBatchWrite = batchWrite.asInstanceOf[LanceBatchWrite]

  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory =
    LanceColumnarBatchDataWriterFactory(
      lanceBatchWrite.getFragmentWriteSpec,
      lanceBatchWrite.getCommitSchema.toJson)

  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory =
    throw new UnsupportedOperationException("Streaming writes to Lance are not offloaded")

  override def doValidateInternal(): ValidationResult = {
    if (!LanceWriteExec.supportsWrite(write)) {
      return ValidationResult.failed(s"Not a Lance write: ${write.getClass.getName}")
    }
    write match {
      case r: RequiresDistributionAndOrdering
          if !r.requiredDistribution().isInstanceOf[UnspecifiedDistribution] =>
        return ValidationResult.failed("Lance table requires a write distribution (sharded)")
      case _ =>
    }
    BackendsApiManager.getValidatorApiInstance.doSchemaValidate(query.schema) match {
      case Some(reason) => return ValidationResult.failed(reason)
      case None =>
    }
    LanceWriteExec.unsupportedColumn(lanceBatchWrite.getCommitSchema) match {
      case Some(reason) => ValidationResult.failed(reason)
      case None => ValidationResult.succeeded
    }
  }
}

object LanceWriteExec {
  private val BlobKey = "lance-encoding:blob"

  def supportsWrite(write: Write): Boolean = write.isInstanceOf[SparkWrite]

  /**
   * The first column whose committed Arrow type Velox cannot export with the same layout, if any.
   * Velox exports plain Utf8, Binary and List, microsecond timestamps and 128-bit decimals.
   */
  def unsupportedColumn(schema: Schema): Option[String] =
    schema.getFields.asScala.iterator.flatMap(unsupported(_, "")).toSeq.headOption

  private def unsupported(field: Field, prefix: String): Option[String] = {
    val name = prefix + field.getName
    val metadata = Option(field.getMetadata).map(_.asScala).getOrElse(Map.empty[String, String])
    if (metadata.get(BlobKey).exists(_.equalsIgnoreCase("true"))) {
      return Some(s"Lance blob column $name is not offloaded")
    }
    def children: Option[String] =
      field.getChildren.asScala.iterator.flatMap(unsupported(_, s"$name.")).toSeq.headOption
    field.getType match {
      case _: ArrowType.Bool | _: ArrowType.Utf8 | _: ArrowType.Binary => None
      case i: ArrowType.Int if i.getIsSigned => None
      case f: ArrowType.FloatingPoint if f.getPrecision != FloatingPointPrecision.HALF => None
      case d: ArrowType.Decimal if d.getBitWidth == 128 => None
      case d: ArrowType.Date if d.getUnit == DateUnit.DAY => None
      case t: ArrowType.Timestamp if t.getUnit == TimeUnit.MICROSECOND => None
      case _: ArrowType.List | _: ArrowType.Struct | _: ArrowType.Map => children
      case other =>
        Some(s"Column $name has Arrow type $other, which Velox cannot export for Lance")
    }
  }
}
