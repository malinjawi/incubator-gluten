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
package org.apache.spark.sql.execution.streaming.operators.stateful

import java.nio.ByteBuffer

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.{GlutenPlan, VeloxColumnarToRowExec}
import org.apache.gluten.execution.streaming.NativeStreamingStatefulOperatorExec
import org.apache.gluten.execution.streaming.state.{VeloxNativeStateStoreOperations, VeloxNativeStateStoreProvider}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Sum}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.{Append, Complete, Update}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan, UnaryExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.NextIterator

import org.apache.hadoop.conf.Configuration

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

case class VeloxNativeStreamingDeduplicateExec(
    keyExpressions: Seq[Attribute],
    child: SparkPlan,
    stateInfo: Option[StatefulOperatorStateInfo],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    withinWatermark: Boolean)
  extends BaseStreamingDeduplicateExec
  with NativeStreamingStatefulOperatorExec {

  override val nativeStatefulOperatorKind: String =
    if (withinWatermark) {
      NativeStreamingStatefulOperatorExec.DeduplicateWithinWatermark
    } else {
      NativeStreamingStatefulOperatorExec.Deduplicate
    }

  override protected val schemaForValueRow: StructType =
    if (withinWatermark) {
      StructType(Array(StructField("expiresAtMicros", LongType, nullable = false)))
    } else {
      StructType(Array(StructField("__dummy__", NullType)))
    }

  override protected val extraOptionOnStateStore: Map[String, String] =
    if (withinWatermark) {
      Map.empty
    } else {
      Map(StateStoreConf.FORMAT_VALIDATION_CHECK_VALUE_CONFIG -> "false")
    }

  override def shortName: String =
    if (withinWatermark) "dedupeWithinWatermark" else "dedupe"

  override protected def doExecute(): RDD[InternalRow] = {
    metrics

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      schemaForValueRow,
      NoPrefixKeyStateEncoderSpec(keyExpressions.toStructType),
      session.sessionState,
      Some(session.streams.stateStoreCoordinator),
      extraOptions = extraOptionOnStateStore
    ) {
      (store, iter) =>
        val rows = ArrayBuffer.empty[UnsafeRow]
        val keys = ArrayBuffer.empty[Array[Byte]]
        val eventTimes = if (withinWatermark) Some(ArrayBuffer.empty[Long]) else None
        val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
        val eventTimeOrdinal = if (withinWatermark) {
          Some(nativeEventTimeOrdinal())
        } else {
          None
        }

        iter.foreach {
          input =>
            val row = input.asInstanceOf[UnsafeRow].copy()
            val key = getKey(row).asInstanceOf[UnsafeRow].copy()
            rows += row
            keys += key.getBytes
            eventTimes.foreach(_ += row.getLong(eventTimeOrdinal.get))
        }

        val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
        val updatesStartTimeNs = System.nanoTime()
        val nativeResult = if (withinWatermark) {
          VeloxNativeStateStoreOperations.deduplicateWithinWatermark(
            store,
            keys.toArray,
            eventTimes.get.toArray,
            eventTimeWatermarkForLateEvents.map(DateTimeUtils.millisToMicros),
            DateTimeUtils.millisToMicros(nativeDelayThresholdMs()),
            eventTimeWatermarkForEviction.map(DateTimeUtils.millisToMicros)
          )
        } else {
          VeloxNativeStateStoreOperations.deduplicate(store, keys.toArray)
        }
        allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime() - updatesStartTimeNs)

        val result = NativeDedupeResult(nativeResult)
        longMetric("numOutputRows") += result.numOutputRows
        longMetric("numUpdatedStateRows") += result.numUpdatedStateRows
        longMetric("numDroppedDuplicateRows") += result.numDroppedDuplicateRows
        longMetric("numRowsDroppedByWatermark") += result.numRowsDroppedByWatermark
        longMetric("numRemovedStateRows") += result.numRemovedStateRows

        val outputRows = result.outputIndices.iterator.map(rows(_): InternalRow)

        new Iterator[InternalRow] {
          private var committed = false

          override def hasNext: Boolean = {
            val hasMoreRows = outputRows.hasNext
            if (!hasMoreRows) {
              commitOnce()
            }
            hasMoreRows
          }

          override def next(): InternalRow = {
            if (!hasNext) {
              Iterator.empty.next()
            } else {
              outputRows.next()
            }
          }

          private def commitOnce(): Unit = {
            if (!committed) {
              committed = true
              longMetric("commitTimeMs") += timeTakenMs(store.commit())
              setStoreMetrics(store)
              setOperatorMetrics()
            }
          }
        }
    }
  }

  override protected def initializeReusedDupInfoRow(): Option[UnsafeRow] = None

  override protected def putDupInfoIntoState(
      store: StateStore,
      data: UnsafeRow,
      key: UnsafeRow,
      reusedDupInfoRow: Option[UnsafeRow]): Unit = {
    throw new UnsupportedOperationException("Velox native dedupe uses JNI batch state updates")
  }

  override protected def evictDupInfoFromState(store: StateStore): Unit = {
    throw new UnsupportedOperationException("Velox native dedupe uses JNI batch state eviction")
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): VeloxNativeStreamingDeduplicateExec = {
    copy(child = newChild)
  }

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration,
      batchId: Long,
      stateSchemaVersion: Int): List[StateSchemaValidationResult] = {
    val newStateSchema = List(
      StateStoreColFamilySchema(
        StateStore.DEFAULT_COL_FAMILY_NAME,
        keySchemaId = 0,
        keyExpressions.toStructType,
        valueSchemaId = 0,
        schemaForValueRow))
    List(
      StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(
        getStateInfo,
        hadoopConf,
        newStateSchema,
        session.sessionState,
        stateSchemaVersion,
        extraOptions = extraOptionOnStateStore))
  }

  private def nativeEventTimeOrdinal(): Int = {
    val eventTimeCol = WatermarkSupport
      .findEventTimeColumn(child.output, allowMultipleEventTimeColumns = false)
      .getOrElse {
        throw new IllegalStateException(
          "Velox native deduplicate-with-watermark requires one event-time column")
      }
    child.output.indexOf(eventTimeCol)
  }

  private def nativeDelayThresholdMs(): Long = {
    val eventTimeCol = WatermarkSupport
      .findEventTimeColumn(child.output, allowMultipleEventTimeColumns = false)
      .getOrElse {
        throw new IllegalStateException(
          "Velox native deduplicate-with-watermark requires one event-time column")
      }
    eventTimeCol.metadata.getLong(EventTimeWatermark.delayKey)
  }

}

case class VeloxNativeStreamingCountExec(
    stateKeyExpressions: Seq[Attribute],
    inputKeyExpressions: Seq[Expression],
    inputCountAttribute: Option[Attribute],
    outputAttributes: Seq[Attribute],
    stateInfo: Option[StatefulOperatorStateInfo],
    outputMode: Option[OutputMode],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    consumeColumnarInput: Boolean,
    child: SparkPlan)
  extends UnaryExecNode
  with GlutenPlan
  with StateStoreWriter
  with WatermarkSupport
  with NativeStreamingStatefulOperatorExec {

  override val nativeStatefulOperatorKind: String =
    NativeStreamingStatefulOperatorExec.CountAggregation

  override def keyExpressions: Seq[Attribute] = stateKeyExpressions

  override def output: Seq[Attribute] = outputAttributes

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def batchType(): Convention.BatchType = Convention.BatchType.None

  override def rowType(): Convention.RowType = Convention.RowType.VanillaRowType

  def directColumnarInputEnabled: Boolean = consumeColumnarInput && child.supportsColumnar

  def directTypedInt64StateInputEnabled: Boolean = {
    directColumnarInputEnabled &&
    (outputMode.contains(Complete) || outputMode.contains(Update)) &&
    stateKeyExpressions.length == 1 &&
    stateKeyExpressions.head.dataType == LongType
  }

  def directTypedInt64StateUpdateRowsEnabled: Boolean =
    directTypedInt64StateInputEnabled && outputMode.contains(Update)

  // Typed single-int64-key count path returning a single primitive long[] instead of per-key
  // byte[][] for Update-mode output. Gated off by default.
  def directTypedInt64PrimitiveUpdateRowsEnabled: Boolean =
    directTypedInt64StateUpdateRowsEnabled &&
      GlutenConfig.get.enableNativeStreamingStatefulTypedPrimitiveOutput

  override def requiredChildConvention(): Seq[ConventionReq] = {
    if (directColumnarInputEnabled) {
      Seq(
        ConventionReq.ofBatch(
          ConventionReq.BatchType.Is(BackendsApiManager.getSettings.primaryBatchType)))
    } else {
      Seq(ConventionReq.vanillaRow)
    }
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (keyExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      StatefulOperatorPartitioning.getCompatibleDistribution(
        keyExpressions,
        getStateInfo,
        conf) :: Nil
    }
  }

  override def shortName: String = "stateStoreSave"

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration,
      batchId: Long,
      stateSchemaVersion: Int): List[StateSchemaValidationResult] = {
    val newStateSchema = List(
      StateStoreColFamilySchema(
        StateStore.DEFAULT_COL_FAMILY_NAME,
        keySchemaId = 0,
        keyExpressions.toStructType,
        valueSchemaId = 0,
        VeloxNativeStreamingCountExec.CountValueSchema))
    List(
      StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(
        getStateInfo,
        hadoopConf,
        newStateSchema,
        session.sessionState,
        stateSchemaVersion))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics
    require(
      outputMode.exists(mode => mode == Complete || mode == Update || mode == Append),
      s"Velox native count aggregation supports only Complete, Update, or Append output mode, " +
        s"found $outputMode"
    )
    require(
      !outputMode.contains(Append) ||
        (watermarkExpressionForLateEvents.isDefined &&
          watermarkExpressionForEviction.isDefined &&
          keyExpressions.exists(_.metadata.contains(EventTimeWatermark.delayKey))),
      "Velox native append count aggregation requires late-data and key-eviction watermarks"
    )

    val inputCountOrdinal = inputCountAttribute.map {
      countAttribute =>
        val ordinal = child.output.indexWhere(_.exprId == countAttribute.exprId)
        require(
          ordinal >= 0,
          s"Velox native count aggregation cannot find input count attribute $countAttribute")
        ordinal
    }

    columnarCountKeyOrdinals(inputCountOrdinal) match {
      case Some(keyOrdinals) =>
        return doExecuteColumnarCount(keyOrdinals)
      case None =>
    }

    VeloxNativeStreamingStatefulInput
      .executeRows(child, consumeColumnarInput)
      .mapPartitionsWithStateStore(
        getStateInfo,
        keyExpressions.toStructType,
        VeloxNativeStreamingCountExec.CountValueSchema,
        NoPrefixKeyStateEncoderSpec(keyExpressions.toStructType),
        session.sessionState,
        Some(session.streams.stateStoreCoordinator)
      ) {
        (store, iter) =>
          val keys = ArrayBuffer.empty[Array[Byte]]
          val deltas = ArrayBuffer.empty[Long]
          val updatedKeys = LinkedHashMap.empty[Seq[Byte], UnsafeRow]
          val getKey = GenerateUnsafeProjection.generate(inputKeyExpressions, child.output)
          val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
          val updatesStartTimeNs = System.nanoTime()
          val updateMode = outputMode.contains(Update)
          val appendMode = outputMode.contains(Append)
          val inputRows =
            if (appendMode) {
              applyRemovingRowsOlderThanWatermark(iter, watermarkPredicateForDataForLateEvents.get)
            } else {
              iter
            }

          inputRows.foreach {
            input =>
              val row = input.asInstanceOf[UnsafeRow]
              val key = getKey(row).asInstanceOf[UnsafeRow].copy()
              val delta = inputCountOrdinal.map(ordinal => row.getLong(ordinal)).getOrElse(1L)
              if (delta != 0L) {
                val keyBytes = key.getBytes
                keys += keyBytes
                inputCountOrdinal.foreach(_ => deltas += delta)
                if (updateMode) {
                  updatedKeys.getOrElseUpdate(keyBytes.toIndexedSeq, key)
                }
              }
          }

          val nativeResult = NativeCountResult(
            inputCountOrdinal match {
              case Some(_) =>
                VeloxNativeStateStoreOperations.incrementCount(store, keys.toArray, deltas.toArray)
              case None =>
                VeloxNativeStateStoreOperations.incrementCount(store, keys.toArray)
            })
          allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime() - updatesStartTimeNs)
          longMetric("numUpdatedStateRows") += nativeResult.numUpdatedStateRows

          val rangeIter = if (updateMode) None else Some(store.iterator())
          val removalStartTimeNs = if (appendMode) System.nanoTime() else 0L
          val updateIter =
            if (updateMode) {
              updatedKeys.valuesIterator.map {
                key =>
                  val value = store.get(key)
                  require(value != null, s"Velox native count aggregation missing updated key $key")
                  new JoinedRow(key.copy(), value.copy()): InternalRow
              }
            } else {
              Iterator.empty
            }
          new NextIterator[InternalRow] {
            override protected def getNext(): InternalRow = {
              if (updateMode && updateIter.hasNext) {
                longMetric("numOutputRows") += 1
                updateIter.next()
              } else if (appendMode) {
                var removedRow: InternalRow = null
                while (rangeIter.exists(_.hasNext) && removedRow == null) {
                  val pair = rangeIter.get.next()
                  if (watermarkPredicateForKeysForEviction.get.eval(pair.key)) {
                    store.remove(pair.key)
                    longMetric("numRemovedStateRows") += 1
                    removedRow = new JoinedRow(pair.key.copy(), pair.value.copy())
                  }
                }
                if (removedRow != null) {
                  longMetric("numOutputRows") += 1
                  removedRow
                } else {
                  finished = true
                  null
                }
              } else if (!updateMode && rangeIter.exists(_.hasNext)) {
                val pair = rangeIter.get.next()
                longMetric("numOutputRows") += 1
                new JoinedRow(pair.key.copy(), pair.value.copy())
              } else {
                finished = true
                null
              }
            }

            override protected def close(): Unit = {
              rangeIter.foreach(_.close())
              if (appendMode) {
                longMetric("allRemovalsTimeMs") +=
                  NANOSECONDS.toMillis(System.nanoTime() - removalStartTimeNs)
              }
              longMetric("commitTimeMs") += timeTakenMs(store.commit())
              setStoreMetrics(store)
              setOperatorMetrics()
            }
          }
      }
  }

  private def columnarCountKeyOrdinals(inputCountOrdinal: Option[Int]): Option[Array[Int]] = {
    if (
      !consumeColumnarInput ||
      !child.supportsColumnar ||
      inputCountOrdinal.isDefined ||
      !(outputMode.contains(Complete) || outputMode.contains(Update))
    ) {
      return None
    }

    val ordinals = ArrayBuffer.empty[Int]
    inputKeyExpressions.foreach {
      case attr: Attribute =>
        val ordinal = child.output.indexWhere(_.exprId == attr.exprId)
        require(
          ordinal >= 0,
          s"Velox native columnar count aggregation cannot find input key attribute $attr")
        ordinals += ordinal
      case _ =>
        return None
    }
    Some(ordinals.toArray)
  }

  private def doExecuteColumnarCount(keyOrdinals: Array[Int]): RDD[InternalRow] = {
    child.executeColumnar().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      VeloxNativeStreamingCountExec.CountValueSchema,
      NoPrefixKeyStateEncoderSpec(keyExpressions.toStructType),
      session.sessionState,
      Some(session.streams.stateStoreCoordinator)
    ) {
      (store, batches: Iterator[ColumnarBatch]) =>
        val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
        val updatesStartTimeNs = System.nanoTime()
        var updatedRows = 0L
        val updateMode = outputMode.contains(Update)
        val typedInt64StateInput = directTypedInt64StateInputEnabled
        val typedInt64PrimitiveUpdateRows = directTypedInt64PrimitiveUpdateRowsEnabled
        val typedInt64UpdateRows =
          directTypedInt64StateUpdateRowsEnabled && !typedInt64PrimitiveUpdateRows
        val updatedKeyValues = LinkedHashMap.empty[ByteBuffer, (Array[Byte], Array[Byte])]
        val updatedOutputRows = LinkedHashMap.empty[ByteBuffer, Array[Byte]]
        val updatedTypedRows = LinkedHashMap.empty[Long, Long]
        var typedNullKeySeen = false
        var typedNullKeySum = 0L

        batches.foreach {
          batch =>
            val nativeResult =
              if (typedInt64PrimitiveUpdateRows) {
                val raw = VeloxNativeStateStoreOperations
                  .incrementCountSingleInt64KeyFromColumnarBatchTypedRows(
                    store,
                    batch,
                    keyOrdinals.head)
                require(
                  raw.length >= NativeTypedInt64UpdateResult.HeaderSize,
                  s"Invalid native typed-primitive count result length ${raw.length}")
                val numNonNull = raw(5).toInt
                val hasNullKey = raw(6) != 0L
                val nullSum = raw(7)
                require(
                  raw.length ==
                    NativeTypedInt64UpdateResult.HeaderSize + numNonNull.toLong * 2,
                  s"Native typed-primitive count result length ${raw.length} != header + " +
                    s"${numNonNull} pairs")
                var i = 0
                while (i < numNonNull) {
                  val base = NativeTypedInt64UpdateResult.HeaderSize + i * 2
                  updatedTypedRows(raw(base)) = raw(base + 1)
                  i += 1
                }
                if (hasNullKey) {
                  typedNullKeySeen = true
                  typedNullKeySum = nullSum
                }
                NativeCountResult(raw.take(NativeTypedInt64UpdateResult.MetricCount))
              } else if (typedInt64UpdateRows) {
                val (rawResult, updatedKeyBytes, updatedRowBytes) =
                  VeloxNativeStateStoreOperations
                    .incrementCountSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
                      store,
                      batch,
                      keyOrdinals.head,
                      VeloxNativeStreamingCountExec.CountValueSchema.length)
                updatedKeyBytes.zip(updatedRowBytes).foreach {
                  case (keyBytes, rowBytes) =>
                    updatedOutputRows(ByteBuffer.wrap(keyBytes).asReadOnlyBuffer()) = rowBytes
                }
                NativeCountResult(rawResult)
              } else if (typedInt64StateInput) {
                NativeCountResult(
                  VeloxNativeStateStoreOperations.incrementCountSingleInt64KeyFromColumnarBatch(
                    store,
                    batch,
                    keyOrdinals.head))
              } else if (updateMode) {
                val (rawResult, updatedKeyBytes, updatedValueBytes) =
                  VeloxNativeStateStoreOperations.incrementCountFromColumnarBatchWithKeyValues(
                    store,
                    batch,
                    keyOrdinals)
                updatedKeyBytes.zip(updatedValueBytes).foreach {
                  case (keyBytes, valueBytes) =>
                    updatedKeyValues(ByteBuffer.wrap(keyBytes).asReadOnlyBuffer()) =
                      keyBytes -> valueBytes
                }
                NativeCountResult(rawResult)
              } else {
                NativeCountResult(
                  VeloxNativeStateStoreOperations.incrementCountFromColumnarBatch(
                    store,
                    batch,
                    keyOrdinals))
              }
            updatedRows += nativeResult.numUpdatedStateRows
        }

        allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime() - updatesStartTimeNs)
        longMetric("numUpdatedStateRows") += updatedRows

        val rangeIter = if (updateMode) None else Some(store.iterator())
        val updateIter =
          if (typedInt64PrimitiveUpdateRows) {
            val proj = UnsafeProjection.create(Array[DataType](LongType, LongType))
            val scratch = new GenericInternalRow(2)
            val nonNullIter = updatedTypedRows.iterator.map {
              case (key, count) =>
                scratch.setLong(0, key)
                scratch.setLong(1, count)
                proj(scratch).copy(): InternalRow
            }
            val nullIter =
              if (typedNullKeySeen) {
                Iterator.single {
                  scratch.setNullAt(0)
                  scratch.setLong(1, typedNullKeySum)
                  proj(scratch).copy(): InternalRow
                }
              } else {
                Iterator.empty
              }
            nonNullIter ++ nullIter
          } else if (typedInt64UpdateRows) {
            updatedOutputRows.valuesIterator.map {
              rowBytes => unsafeRowFromOutputBytes(rowBytes): InternalRow
            }
          } else if (updateMode) {
            updatedKeyValues.valuesIterator.map {
              case (keyBytes, valueBytes) =>
                new JoinedRow(
                  unsafeRowFromKeyBytes(keyBytes),
                  unsafeRowFromValueBytes(valueBytes, VeloxNativeStreamingCountExec.CountValueSchema.length)
                ): InternalRow
            }
          } else {
            Iterator.empty
          }
        new NextIterator[InternalRow] {
          override protected def getNext(): InternalRow = {
            if (updateMode && updateIter.hasNext) {
              longMetric("numOutputRows") += 1
              updateIter.next()
            } else if (!updateMode && rangeIter.exists(_.hasNext)) {
              val pair = rangeIter.get.next()
              longMetric("numOutputRows") += 1
              new JoinedRow(pair.key.copy(), pair.value.copy())
            } else {
              finished = true
              null
            }
          }

          override protected def close(): Unit = {
            rangeIter.foreach(_.close())
            longMetric("commitTimeMs") += timeTakenMs(store.commit())
            setStoreMetrics(store)
            setOperatorMetrics()
          }
        }
    }
  }

  private def unsafeRowFromKeyBytes(bytes: Array[Byte]): UnsafeRow = {
    val row = new UnsafeRow(stateKeyExpressions.length)
    row.pointTo(bytes, bytes.length)
    row
  }

  private def unsafeRowFromValueBytes(bytes: Array[Byte], fields: Int): UnsafeRow = {
    val row = new UnsafeRow(fields)
    row.pointTo(bytes, bytes.length)
    row
  }

  private def unsafeRowFromOutputBytes(bytes: Array[Byte]): UnsafeRow = {
    val row = new UnsafeRow(outputAttributes.length)
    row.pointTo(bytes, bytes.length)
    row
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): VeloxNativeStreamingCountExec = {
    copy(child = newChild)
  }

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    (outputMode.contains(Append) || outputMode.contains(Update)) &&
    eventTimeWatermarkForEviction.exists(newInputWatermark > _)
  }
}

object VeloxNativeStreamingCountExec {
  val CountValueSchema: StructType =
    StructType(Array(StructField("count", LongType, nullable = false)))
}

case class VeloxNativeStreamingLongSumExec(
    stateKeyExpressions: Seq[Attribute],
    inputKeyExpressions: Seq[Expression],
    inputSumAttribute: Attribute,
    outputAttributes: Seq[Attribute],
    stateInfo: Option[StatefulOperatorStateInfo],
    outputMode: Option[OutputMode],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    consumeColumnarInput: Boolean,
    child: SparkPlan)
  extends UnaryExecNode
  with GlutenPlan
  with StateStoreWriter
  with WatermarkSupport
  with NativeStreamingStatefulOperatorExec {

  override val nativeStatefulOperatorKind: String =
    NativeStreamingStatefulOperatorExec.LongSumAggregation

  override def keyExpressions: Seq[Attribute] = stateKeyExpressions

  override def output: Seq[Attribute] = outputAttributes

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def batchType(): Convention.BatchType = Convention.BatchType.None

  override def rowType(): Convention.RowType = Convention.RowType.VanillaRowType

  def directColumnarInputEnabled: Boolean = consumeColumnarInput && child.supportsColumnar

  def directFixedWidthUpdateRowsEnabled: Boolean = {
    directColumnarInputEnabled &&
    outputMode.contains(Update) &&
    stateKeyExpressions.forall(
      key => VeloxNativeStreamingLongSumExec.isFixedWidthUpdateKeyType(key.dataType))
  }

  def directTypedInt64StateUpdateRowsEnabled: Boolean = {
    directFixedWidthUpdateRowsEnabled &&
    stateKeyExpressions.length == 1 &&
    stateKeyExpressions.head.dataType == LongType
  }

  // When the typed single-int64-key path is engaged AND the typed-primitive-output gate is on, the
  // native op returns the changed Update-mode groups as a single primitive long[] (int64 key + int64
  // sum) rather than a per-key byte[][] of UnsafeRow blobs. Off by default.
  def directTypedInt64PrimitiveUpdateRowsEnabled: Boolean = {
    directTypedInt64StateUpdateRowsEnabled &&
    GlutenConfig.get.enableNativeStreamingStatefulTypedPrimitiveOutput
  }

  override def requiredChildConvention(): Seq[ConventionReq] = {
    if (directColumnarInputEnabled) {
      Seq(
        ConventionReq.ofBatch(
          ConventionReq.BatchType.Is(BackendsApiManager.getSettings.primaryBatchType)))
    } else {
      Seq(ConventionReq.vanillaRow)
    }
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (keyExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      StatefulOperatorPartitioning.getCompatibleDistribution(
        keyExpressions,
        getStateInfo,
        conf) :: Nil
    }
  }

  override def shortName: String = "stateStoreSave"

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration,
      batchId: Long,
      stateSchemaVersion: Int): List[StateSchemaValidationResult] = {
    val newStateSchema = List(
      StateStoreColFamilySchema(
        StateStore.DEFAULT_COL_FAMILY_NAME,
        keySchemaId = 0,
        keyExpressions.toStructType,
        valueSchemaId = 0,
        VeloxNativeStreamingLongSumExec.valueSchema(outputAttributes, stateKeyExpressions)
      ))
    List(
      StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(
        getStateInfo,
        hadoopConf,
        newStateSchema,
        session.sessionState,
        stateSchemaVersion))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics
    require(
      outputMode.exists(mode => mode == Complete || mode == Update),
      s"Velox native long sum aggregation supports only Complete or Update output mode, " +
        s"found $outputMode"
    )
    val inputSumOrdinal = child.output.indexWhere(_.exprId == inputSumAttribute.exprId)
    require(
      inputSumOrdinal >= 0,
      s"Velox native long sum aggregation cannot find input sum attribute $inputSumAttribute")

    columnarLongSumInputOrdinals(inputSumOrdinal) match {
      case Some((keyOrdinals, valueOrdinal)) =>
        return doExecuteColumnarLongSum(keyOrdinals, valueOrdinal)
      case None =>
    }

    VeloxNativeStreamingStatefulInput
      .executeRows(child, consumeColumnarInput)
      .mapPartitionsWithStateStore(
        getStateInfo,
        keyExpressions.toStructType,
        VeloxNativeStreamingLongSumExec.valueSchema(outputAttributes, stateKeyExpressions),
        NoPrefixKeyStateEncoderSpec(keyExpressions.toStructType),
        session.sessionState,
        Some(session.streams.stateStoreCoordinator)
      ) {
        (store, iter) =>
          val keys = ArrayBuffer.empty[Array[Byte]]
          val deltas = ArrayBuffer.empty[Long]
          val updatedKeys = LinkedHashMap.empty[Seq[Byte], UnsafeRow]
          val getKey = GenerateUnsafeProjection.generate(inputKeyExpressions, child.output)
          val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
          val updatesStartTimeNs = System.nanoTime()
          val updateMode = outputMode.contains(Update)

          iter.foreach {
            input =>
              val row = input.asInstanceOf[UnsafeRow]
              // A null summed value contributes nothing (Spark SUM ignores nulls), but the group
              // key is still observed so the group is emitted with its existing/zero running sum.
              val delta = if (row.isNullAt(inputSumOrdinal)) 0L else row.getLong(inputSumOrdinal)
              val key = getKey(row).asInstanceOf[UnsafeRow].copy()
              val keyBytes = key.getBytes
              keys += keyBytes
              deltas += delta
              if (updateMode) {
                updatedKeys.getOrElseUpdate(keyBytes.toIndexedSeq, key)
              }
          }

          val nativeResult = NativeCountResult(
            VeloxNativeStateStoreOperations.incrementLongSum(store, keys.toArray, deltas.toArray))
          allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime() - updatesStartTimeNs)
          longMetric("numUpdatedStateRows") += nativeResult.numUpdatedStateRows

          val rangeIter = if (updateMode) None else Some(store.iterator())
          val updateIter =
            if (updateMode) {
              updatedKeys.valuesIterator.map {
                key =>
                  val value = store.get(key)
                  require(
                    value != null,
                    s"Velox native long sum aggregation missing updated key $key")
                  new JoinedRow(key.copy(), value.copy()): InternalRow
              }
            } else {
              Iterator.empty
            }
          new NextIterator[InternalRow] {
            override protected def getNext(): InternalRow = {
              if (updateMode && updateIter.hasNext) {
                longMetric("numOutputRows") += 1
                updateIter.next()
              } else if (!updateMode && rangeIter.exists(_.hasNext)) {
                val pair = rangeIter.get.next()
                longMetric("numOutputRows") += 1
                new JoinedRow(pair.key.copy(), pair.value.copy())
              } else {
                finished = true
                null
              }
            }

            override protected def close(): Unit = {
              rangeIter.foreach(_.close())
              longMetric("commitTimeMs") += timeTakenMs(store.commit())
              setStoreMetrics(store)
              setOperatorMetrics()
            }
          }
      }
  }

  private def columnarLongSumInputOrdinals(inputSumOrdinal: Int): Option[(Array[Int], Int)] = {
    if (
      !consumeColumnarInput ||
      !child.supportsColumnar ||
      !(outputMode.contains(Complete) || outputMode.contains(Update)) ||
      inputKeyExpressions.isEmpty ||
      child.output(inputSumOrdinal).dataType != LongType
    ) {
      return None
    }

    val keyOrdinals = ArrayBuffer.empty[Int]
    inputKeyExpressions.foreach {
      case attr: Attribute =>
        val ordinal = child.output.indexWhere(_.exprId == attr.exprId)
        require(
          ordinal >= 0,
          s"Velox native columnar long sum aggregation cannot find input key attribute $attr")
        keyOrdinals += ordinal
      case _ =>
        return None
    }
    Some((keyOrdinals.toArray, inputSumOrdinal))
  }

  private def doExecuteColumnarLongSum(
      keyOrdinals: Array[Int],
      valueOrdinal: Int): RDD[InternalRow] = {
    val valueSchema = VeloxNativeStreamingLongSumExec.valueSchema(outputAttributes, stateKeyExpressions)
    child.executeColumnar().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      valueSchema,
      NoPrefixKeyStateEncoderSpec(keyExpressions.toStructType),
      session.sessionState,
      Some(session.streams.stateStoreCoordinator)
    ) {
      (store, batches: Iterator[ColumnarBatch]) =>
        val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
        val updatesStartTimeNs = System.nanoTime()
        var updatedRows = 0L
        val updateMode = outputMode.contains(Update)
        val fixedWidthUpdateRows = directFixedWidthUpdateRowsEnabled
        val typedInt64PrimitiveUpdateRows = directTypedInt64PrimitiveUpdateRowsEnabled
        val typedInt64UpdateRows =
          directTypedInt64StateUpdateRowsEnabled && !typedInt64PrimitiveUpdateRows
        val updatedKeyValues = LinkedHashMap.empty[ByteBuffer, (Array[Byte], Array[Byte])]
        val updatedOutputRows = LinkedHashMap.empty[ByteBuffer, Array[Byte]]
        // Primitive-output accumulators: distinct non-null group -> running sum (insertion-ordered),
        // plus the optional single SQL NULL group. No per-key byte[] / ByteBuffer is allocated.
        val updatedTypedRows = LinkedHashMap.empty[Long, Long]
        var typedNullKeySeen = false
        var typedNullKeySum = 0L

        batches.foreach {
          batch =>
            val nativeResult =
              if (typedInt64PrimitiveUpdateRows) {
                val raw = VeloxNativeStateStoreOperations
                  .incrementLongSumSingleInt64KeyFromColumnarBatchTypedRows(
                    store,
                    batch,
                    keyOrdinals.head,
                    valueOrdinal)
                require(
                  raw.length >= NativeTypedInt64UpdateResult.HeaderSize,
                  s"Invalid native typed-primitive long sum result length ${raw.length}")
                val numNonNull = raw(5).toInt
                val hasNullKey = raw(6) != 0L
                val nullSum = raw(7)
                require(
                  raw.length ==
                    NativeTypedInt64UpdateResult.HeaderSize + numNonNull.toLong * 2,
                  s"Native typed-primitive long sum result length ${raw.length} != header + " +
                    s"${numNonNull} pairs")
                var i = 0
                while (i < numNonNull) {
                  val base = NativeTypedInt64UpdateResult.HeaderSize + i * 2
                  updatedTypedRows(raw(base)) = raw(base + 1)
                  i += 1
                }
                if (hasNullKey) {
                  typedNullKeySeen = true
                  typedNullKeySum = nullSum
                }
                NativeCountResult(raw.take(NativeTypedInt64UpdateResult.MetricCount))
              } else if (typedInt64UpdateRows) {
                val (rawResult, updatedKeyBytes, updatedRowBytes) =
                  VeloxNativeStateStoreOperations
                    .incrementLongSumSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
                      store,
                      batch,
                      keyOrdinals.head,
                      valueOrdinal,
                      valueSchema.length)
                updatedKeyBytes.zip(updatedRowBytes).foreach {
                  case (keyBytes, rowBytes) =>
                    updatedOutputRows(ByteBuffer.wrap(keyBytes).asReadOnlyBuffer()) = rowBytes
                }
                NativeCountResult(rawResult)
              } else if (fixedWidthUpdateRows) {
                val (rawResult, updatedKeyBytes, updatedRowBytes) =
                  VeloxNativeStateStoreOperations
                    .incrementLongSumFromColumnarBatchWithFixedWidthRows(
                      store,
                      batch,
                      keyOrdinals,
                      valueOrdinal,
                      valueSchema.length)
                updatedKeyBytes.zip(updatedRowBytes).foreach {
                  case (keyBytes, rowBytes) =>
                    updatedOutputRows(ByteBuffer.wrap(keyBytes).asReadOnlyBuffer()) = rowBytes
                }
                NativeCountResult(rawResult)
              } else if (updateMode) {
                val (rawResult, updatedKeyBytes, updatedValueBytes) =
                  VeloxNativeStateStoreOperations.incrementLongSumFromColumnarBatchWithKeyValues(
                    store,
                    batch,
                    keyOrdinals,
                    valueOrdinal)
                updatedKeyBytes.zip(updatedValueBytes).foreach {
                  case (keyBytes, valueBytes) =>
                    updatedKeyValues(ByteBuffer.wrap(keyBytes).asReadOnlyBuffer()) =
                      keyBytes -> valueBytes
                }
                NativeCountResult(rawResult)
              } else {
                NativeCountResult(
                  VeloxNativeStateStoreOperations.incrementLongSumFromColumnarBatch(
                    store,
                    batch,
                    keyOrdinals,
                    valueOrdinal))
              }
            updatedRows += nativeResult.numUpdatedStateRows
        }

        allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime() - updatesStartTimeNs)
        longMetric("numUpdatedStateRows") += updatedRows

        val rangeIter = if (updateMode) None else Some(store.iterator())
        val updateIter =
          if (typedInt64PrimitiveUpdateRows) {
            // Build each [key:Long, sum:Long] output UnsafeRow straight from the primitive longs via
            // a reused UnsafeProjection -- no per-key byte[] decode. The single null group (if any)
            // is emitted last with a null key field.
            val proj = UnsafeProjection.create(Array[DataType](LongType, LongType))
            val scratch = new GenericInternalRow(2)
            val nonNullIter = updatedTypedRows.iterator.map {
              case (key, sum) =>
                scratch.setLong(0, key)
                scratch.setLong(1, sum)
                proj(scratch).copy(): InternalRow
            }
            val nullIter =
              if (typedNullKeySeen) {
                Iterator.single {
                  scratch.setNullAt(0)
                  scratch.setLong(1, typedNullKeySum)
                  proj(scratch).copy(): InternalRow
                }
              } else {
                Iterator.empty
              }
            nonNullIter ++ nullIter
          } else if (fixedWidthUpdateRows) {
            updatedOutputRows.valuesIterator.map {
              rowBytes => unsafeRowFromOutputBytes(rowBytes): InternalRow
            }
          } else if (updateMode) {
            updatedKeyValues.valuesIterator.map {
              case (keyBytes, valueBytes) =>
                new JoinedRow(
                  unsafeRowFromKeyBytes(keyBytes),
                  unsafeRowFromValueBytes(valueBytes, valueSchema.length)): InternalRow
            }
          } else {
            Iterator.empty
          }
        new NextIterator[InternalRow] {
          override protected def getNext(): InternalRow = {
            if (updateMode && updateIter.hasNext) {
              longMetric("numOutputRows") += 1
              updateIter.next()
            } else if (!updateMode && rangeIter.exists(_.hasNext)) {
              val pair = rangeIter.get.next()
              longMetric("numOutputRows") += 1
              new JoinedRow(pair.key.copy(), pair.value.copy())
            } else {
              finished = true
              null
            }
          }

          override protected def close(): Unit = {
            rangeIter.foreach(_.close())
            longMetric("commitTimeMs") += timeTakenMs(store.commit())
            setStoreMetrics(store)
            setOperatorMetrics()
          }
        }
    }
  }

  private def unsafeRowFromKeyBytes(bytes: Array[Byte]): UnsafeRow = {
    val row = new UnsafeRow(stateKeyExpressions.length)
    row.pointTo(bytes, bytes.length)
    row
  }

  private def unsafeRowFromValueBytes(bytes: Array[Byte], fields: Int): UnsafeRow = {
    val row = new UnsafeRow(fields)
    row.pointTo(bytes, bytes.length)
    row
  }

  private def unsafeRowFromOutputBytes(bytes: Array[Byte]): UnsafeRow = {
    val row = new UnsafeRow(outputAttributes.length)
    row.pointTo(bytes, bytes.length)
    row
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): VeloxNativeStreamingLongSumExec = {
    copy(child = newChild)
  }

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    false
  }
}

object VeloxNativeStreamingLongSumExec {
  def isFixedWidthUpdateKeyType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          DateType | TimestampType | TimestampNTZType =>
        true
      case _ => false
    }
  }

  def valueSchema(
      outputAttributes: Seq[Attribute],
      stateKeyExpressions: Seq[Attribute]): StructType = {
    val schema = outputAttributes.drop(stateKeyExpressions.length).toStructType
    require(
      schema.length == 1 && schema.head.dataType == LongType,
      s"Velox native long sum aggregation requires exactly one long value field, got $schema")
    schema
  }
}

private object VeloxNativeStreamingStatefulInput {
  def executeRows(child: SparkPlan, consumeColumnarInput: Boolean): RDD[InternalRow] = {
    if (!consumeColumnarInput || !child.supportsColumnar) {
      return child.execute()
    }

    child.executeColumnar().mapPartitions {
      batches => VeloxColumnarToRowExec.toRowIterator(batches)
    }
  }
}

class VeloxNativeStreamingStatefulOperatorRule
  extends org.apache.spark.sql.catalyst.rules.Rule[
    SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val deduplicateEnabled = VeloxNativeStreamingStatefulOperatorRule.deduplicateEnabled
    val aggregationEnabled = VeloxNativeStreamingStatefulOperatorRule.aggregationEnabled
    if (
      !deduplicateEnabled &&
      !aggregationEnabled
    ) {
      return plan
    }

    plan.transformUp {
      case save: StateStoreSaveExec if aggregationEnabled =>
        VeloxNativeStreamingStatefulOperatorRule.tryRewriteAggregation(save).getOrElse(save)

      case dedupe: StreamingDeduplicateExec
          if deduplicateEnabled &&
            dedupe.stateInfo.isDefined &&
            WatermarkSupport
              .findEventTimeColumn(dedupe.child.output, allowMultipleEventTimeColumns = false)
              .isEmpty =>
        VeloxNativeStreamingDeduplicateExec(
          dedupe.keyExpressions,
          dedupe.child,
          dedupe.stateInfo,
          dedupe.eventTimeWatermarkForLateEvents,
          dedupe.eventTimeWatermarkForEviction,
          withinWatermark = false
        )

      case dedupe: StreamingDeduplicateWithinWatermarkExec
          if deduplicateEnabled && dedupe.stateInfo.isDefined =>
        VeloxNativeStreamingDeduplicateExec(
          dedupe.keyExpressions,
          dedupe.child,
          dedupe.stateInfo,
          dedupe.eventTimeWatermarkForLateEvents,
          dedupe.eventTimeWatermarkForEviction,
          withinWatermark = true
        )
    }
  }
}

object VeloxNativeStreamingStatefulOperatorRule {
  private def deduplicateEnabled: Boolean = {
    val glutenConf = GlutenConfig.get
    val stateStoreProvider = SQLConf.get.getConfString(SQLConf.STATE_STORE_PROVIDER_CLASS.key)
    glutenConf.enableNativeStreaming &&
    glutenConf.enableNativeStreamingStatefulDeduplicate &&
    glutenConf.enableNativeStreamingStateStore &&
    VeloxConfig.get.enableVeloxNativeStreamingStateStore &&
    stateStoreProvider == classOf[VeloxNativeStateStoreProvider].getName
  }

  private def aggregationEnabled: Boolean = {
    val glutenConf = GlutenConfig.get
    val stateStoreProvider = SQLConf.get.getConfString(SQLConf.STATE_STORE_PROVIDER_CLASS.key)
    glutenConf.enableNativeStreaming &&
    glutenConf.enableNativeStreamingStatefulAggregation &&
    glutenConf.enableNativeStreamingStateStore &&
    VeloxConfig.get.enableVeloxNativeStreamingStateStore &&
    stateStoreProvider == classOf[VeloxNativeStateStoreProvider].getName
  }

  private def tryRewriteAggregation(save: StateStoreSaveExec): Option[SparkPlan] = {
    tryRewriteRawCountAggregation(save)
      .orElse(tryRewriteCountAggregation(save))
      .orElse(tryRewriteRawLongSumAggregation(save))
      .orElse(tryRewriteLongSumAggregation(save))
  }

  private def tryRewriteRawCountAggregation(
      save: StateStoreSaveExec): Option[VeloxNativeStreamingCountExec] = {
    if (
      save.stateInfo.isEmpty ||
      !isSupportedRawAggregationOutputMode(save) ||
      save.keyExpressions.isEmpty
    ) {
      return None
    }

    stripTransparentWrappers(save.child) match {
      case stateMergeAgg: HashAggregateExec if isCountOnly(stateMergeAgg) =>
        stripTransparentWrappers(stateMergeAgg.child) match {
          case restore: StateStoreRestoreExec =>
            stripTransparentWrappers(restore.child) match {
              case batchMergeAgg: HashAggregateExec =>
                rawCountInputPlan(batchMergeAgg, save).map {
                  rawInput =>
                    VeloxNativeStreamingCountExec(
                      save.keyExpressions,
                      batchMergeAgg.groupingExpressions,
                      inputCountAttribute = None,
                      save.output,
                      save.stateInfo,
                      save.outputMode,
                      save.eventTimeWatermarkForLateEvents,
                      save.eventTimeWatermarkForEviction,
                      consumeColumnarInput = true,
                      rawInput
                    )
                }
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
  }

  private def tryRewriteCountAggregation(
      save: StateStoreSaveExec): Option[VeloxNativeStreamingCountExec] = {
    if (
      save.stateInfo.isEmpty ||
      !isSupportedOutputMode(save) ||
      save.keyExpressions.isEmpty
    ) {
      return None
    }

    stripTransparentWrappers(save.child) match {
      case stateMergeAgg: HashAggregateExec if isCountOnly(stateMergeAgg) =>
        stripTransparentWrappers(stateMergeAgg.child) match {
          case restore: StateStoreRestoreExec =>
            stripTransparentWrappers(restore.child) match {
              case batchMergeAgg: HashAggregateExec
                  if isSupportedPartialCount(batchMergeAgg, save) =>
                Some(
                  VeloxNativeStreamingCountExec(
                    save.keyExpressions,
                    batchMergeAgg.groupingExpressions,
                    Some(batchMergeAgg.output.last),
                    save.output,
                    save.stateInfo,
                    save.outputMode,
                    save.eventTimeWatermarkForLateEvents,
                    save.eventTimeWatermarkForEviction,
                    consumeColumnarInput = false,
                    batchMergeAgg
                  ))
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
  }

  private def tryRewriteRawLongSumAggregation(
      save: StateStoreSaveExec): Option[VeloxNativeStreamingLongSumExec] = {
    if (
      save.stateInfo.isEmpty ||
      !isSupportedRawAggregationOutputMode(save) ||
      save.keyExpressions.isEmpty
    ) {
      return None
    }

    stripTransparentWrappers(save.child) match {
      case stateMergeAgg: HashAggregateExec if isLongSumOnly(stateMergeAgg) =>
        stripTransparentWrappers(stateMergeAgg.child) match {
          case restore: StateStoreRestoreExec =>
            stripTransparentWrappers(restore.child) match {
              case batchMergeAgg: HashAggregateExec =>
                rawLongSumInputPlan(batchMergeAgg, save).map {
                  case (rawInput, sumAttribute) =>
                    VeloxNativeStreamingLongSumExec(
                      save.keyExpressions,
                      batchMergeAgg.groupingExpressions,
                      sumAttribute,
                      save.output,
                      save.stateInfo,
                      save.outputMode,
                      save.eventTimeWatermarkForLateEvents,
                      save.eventTimeWatermarkForEviction,
                      consumeColumnarInput = true,
                      rawInput
                    )
                }
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
  }

  private def tryRewriteLongSumAggregation(
      save: StateStoreSaveExec): Option[VeloxNativeStreamingLongSumExec] = {
    if (
      save.stateInfo.isEmpty ||
      !isSupportedLongSumOutputMode(save) ||
      save.keyExpressions.isEmpty
    ) {
      return None
    }

    stripTransparentWrappers(save.child) match {
      case stateMergeAgg: HashAggregateExec if isLongSumOnly(stateMergeAgg) =>
        stripTransparentWrappers(stateMergeAgg.child) match {
          case restore: StateStoreRestoreExec =>
            stripTransparentWrappers(restore.child) match {
              case batchMergeAgg: HashAggregateExec
                  if isSupportedPartialLongSum(batchMergeAgg, save) =>
                Some(
                  VeloxNativeStreamingLongSumExec(
                    save.keyExpressions,
                    batchMergeAgg.groupingExpressions,
                    batchMergeAgg.output.last,
                    save.output,
                    save.stateInfo,
                    save.outputMode,
                    save.eventTimeWatermarkForLateEvents,
                    save.eventTimeWatermarkForEviction,
                    consumeColumnarInput = false,
                    batchMergeAgg
                  ))
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
  }

  private def rawCountInputPlan(
      batchMergeAgg: HashAggregateExec,
      save: StateStoreSaveExec): Option[SparkPlan] = {
    rawPartialAggregate(batchMergeAgg.child) match {
      case Some(localPartialAgg) if isSupportedPartialCount(localPartialAgg, save) =>
        Some(localPartialAgg.child)
      case _ => None
    }
  }

  private def rawLongSumInputPlan(
      batchMergeAgg: HashAggregateExec,
      save: StateStoreSaveExec): Option[(SparkPlan, Attribute)] = {
    rawPartialAggregate(batchMergeAgg.child) match {
      case Some(localPartialAgg) if isSupportedPartialLongSum(localPartialAgg, save) =>
        rawLongSumAttribute(localPartialAgg)
          .filter(
            sumAttribute =>
              localPartialAgg.child.output.exists(_.exprId == sumAttribute.exprId))
          // Strip a transition wrapper (e.g. VeloxColumnarToRow) so the native long-sum reads
          // the columnar child directly instead of through a row bridge.
          .map(sumAttribute => (stripTransparentWrappers(localPartialAgg.child), sumAttribute))
      case _ => None
    }
  }

  private def rawPartialAggregate(plan: SparkPlan): Option[HashAggregateExec] = {
    stripTransparentWrappers(plan) match {
      case exchange: ShuffleExchangeExec =>
        stripTransparentWrappers(exchange.child) match {
          case localPartialAgg: HashAggregateExec => Some(localPartialAgg)
          case _ => None
        }
      case localPartialAgg: HashAggregateExec => Some(localPartialAgg)
      case _ => None
    }
  }

  private def stripTransparentWrappers(plan: SparkPlan): SparkPlan = plan match {
    case input: InputAdapter => stripTransparentWrappers(input.child)
    case wholeStage: WholeStageCodegenExec => stripTransparentWrappers(wholeStage.child)
    // A VeloxColumnarToRow only exists to bridge a native columnar child into a row consumer.
    // The native streaming operator consumes the columnar child directly (consumeColumnarInput),
    // so this row bridge is transparent for plan matching and can be stripped to expose the
    // underlying native columnar plan (e.g. ProjectExecTransformer).
    case c2r: VeloxColumnarToRowExec => stripTransparentWrappers(c2r.child)
    case other => other
  }

  private def isSupportedOutputMode(save: StateStoreSaveExec): Boolean = {
    save.outputMode match {
      // Spark may carry placeholder watermark values on plain aggregations. Decline native count
      // only when Spark has derived an actual watermark predicate that native count cannot
      // evaluate yet.
      case Some(Complete) | Some(Update) => !hasWatermarkPredicate(save)
      case Some(Append) => isSupportedAppendWatermarkCount(save)
      case _ => false
    }
  }

  private def isSupportedAppendWatermarkCount(save: StateStoreSaveExec): Boolean = {
    save.watermarkPredicateForDataForLateEvents.isDefined &&
    save.watermarkPredicateForKeysForEviction.isDefined &&
    save.keyExpressions.exists(isWindowWatermarkKey)
  }

  private def isSupportedLongSumOutputMode(save: StateStoreSaveExec): Boolean = {
    save.outputMode match {
      case Some(Complete) | Some(Update) => !hasWatermarkPredicate(save)
      case _ => false
    }
  }

  private def isSupportedRawAggregationOutputMode(save: StateStoreSaveExec): Boolean = {
    save.outputMode match {
      case Some(Complete) | Some(Update) => !hasWatermarkPredicate(save)
      case _ => false
    }
  }

  private def isWindowWatermarkKey(attribute: Attribute): Boolean = {
    attribute.metadata.contains(EventTimeWatermark.delayKey) &&
    (attribute.dataType match {
      case StructType(Array(start, end)) =>
        start.name == "start" &&
        start.dataType == TimestampType &&
        end.name == "end" &&
        end.dataType == TimestampType
      case _ => false
    })
  }

  private def hasWatermarkPredicate(save: StateStoreSaveExec): Boolean = {
    save.keyExpressions.exists(_.metadata.contains(EventTimeWatermark.delayKey)) ||
    save.watermarkPredicateForDataForLateEvents.isDefined ||
    save.watermarkPredicateForDataForEviction.isDefined ||
    save.watermarkPredicateForKeysForLateEvents.isDefined ||
    save.watermarkPredicateForKeysForEviction.isDefined
  }

  private def isSupportedPartialCount(
      partialAgg: HashAggregateExec,
      save: StateStoreSaveExec): Boolean = {
    isCountOnly(partialAgg) &&
    partialAgg.groupingExpressions.nonEmpty &&
    partialAgg.groupingExpressions.length == save.keyExpressions.length &&
    partialAgg.output.length == save.keyExpressions.length + 1 &&
    partialAgg.output.last.dataType == LongType &&
    save.output.length == save.keyExpressions.length + 1 &&
    save.output.last.dataType == LongType
  }

  private def isSupportedPartialLongSum(
      partialAgg: HashAggregateExec,
      save: StateStoreSaveExec): Boolean = {
    isLongSumValueSupported(partialAgg) &&
    partialAgg.groupingExpressions.nonEmpty &&
    partialAgg.groupingExpressions.length == save.keyExpressions.length &&
    partialAgg.output.length == save.keyExpressions.length + 1 &&
    partialAgg.output.last.dataType == LongType &&
    save.output.length == save.keyExpressions.length + 1 &&
    save.output.last.dataType == LongType
  }

  private def isCountOnly(agg: HashAggregateExec): Boolean = {
    agg.aggregateExpressions.nonEmpty &&
    agg.aggregateExpressions.forall(_.aggregateFunction.isInstanceOf[Count])
  }

  private def isLongSumOnly(agg: HashAggregateExec): Boolean = {
    agg.aggregateExpressions.nonEmpty &&
    agg.aggregateExpressions.forall {
      expression =>
        expression.aggregateFunction match {
          case sum: Sum => sum.dataType == LongType
          case _ => false
        }
    }
  }

  // Whether a nullable summed value column is acceptable for the native long-sum operator.
  // A null summed value contributes nothing (matching Spark SUM semantics). When false, the
  // operator only engages for non-nullable value columns, preserving the original strict
  // contract (the native columnar fast-path rejects actual null values at runtime).
  private def allowNullableLongSumValue: Boolean =
    GlutenConfig.get.enableNativeStreamingStatefulLongSumNullableValue

  private def isSupportedLongSumValueChild(child: Expression): Boolean =
    allowNullableLongSumValue || !child.nullable

  private def isLongSumValueSupported(agg: HashAggregateExec): Boolean = {
    agg.aggregateExpressions.nonEmpty &&
    agg.aggregateExpressions.forall {
      expression =>
        expression.aggregateFunction match {
          case sum: Sum => sum.dataType == LongType && isSupportedLongSumValueChild(sum.child)
          case _ => false
        }
    }
  }

  private def rawLongSumAttribute(agg: HashAggregateExec): Option[Attribute] = {
    if (agg.aggregateExpressions.length != 1) {
      return None
    }
    agg.aggregateExpressions.head.aggregateFunction match {
      case sum: Sum if sum.dataType == LongType && isSupportedLongSumValueChild(sum.child) =>
        sum.child match {
          case attr: Attribute => Some(attr)
          case _ => None
        }
      case _ => None
    }
  }
}

private object NativeDedupeResult {
  private val MetricCount = 8

  def apply(raw: Array[Long]): NativeDedupeResult = {
    require(raw.nonEmpty, "Velox native deduplicate returned an empty result")
    val outputCount = raw(0).toInt
    require(outputCount >= 0, s"Invalid native deduplicate output count: ${raw(0)}")
    require(
      raw.length == 1 + outputCount + MetricCount,
      s"Invalid native deduplicate result length ${raw.length} for $outputCount outputs")

    val outputIndices = raw.slice(1, 1 + outputCount).map {
      index =>
        require(
          index >= 0 && index <= Int.MaxValue,
          s"Invalid native deduplicate output row index: $index")
        index.toInt
    }
    val metricStart = 1 + outputCount
    NativeDedupeResult(
      outputIndices,
      numInputRows = raw(metricStart),
      numOutputRows = raw(metricStart + 1),
      numUpdatedStateRows = raw(metricStart + 2),
      numDroppedDuplicateRows = raw(metricStart + 3),
      numRowsDroppedByWatermark = raw(metricStart + 4),
      numRemovedStateRows = raw(metricStart + 5),
      numTotalStateRows = raw(metricStart + 6),
      stateMemoryBytes = raw(metricStart + 7)
    )
  }
}

private case class NativeDedupeResult(
    outputIndices: Array[Int],
    numInputRows: Long,
    numOutputRows: Long,
    numUpdatedStateRows: Long,
    numDroppedDuplicateRows: Long,
    numRowsDroppedByWatermark: Long,
    numRemovedStateRows: Long,
    numTotalStateRows: Long,
    stateMemoryBytes: Long)

private object NativeCountResult {
  private val MetricCount = 5

  def apply(raw: Array[Long]): NativeCountResult = {
    require(
      raw.length == MetricCount,
      s"Invalid native count aggregation result length ${raw.length}")
    NativeCountResult(
      numInputRows = raw(0),
      numOutputRows = raw(1),
      numUpdatedStateRows = raw(2),
      numTotalStateRows = raw(3),
      stateMemoryBytes = raw(4)
    )
  }
}

private case class NativeCountResult(
    numInputRows: Long,
    numOutputRows: Long,
    numUpdatedStateRows: Long,
    numTotalStateRows: Long,
    stateMemoryBytes: Long)

// Layout constants for the primitive-typed Update result long[] returned by the
// nativeIncrement*SingleInt64KeyFromColumnarBatchTypedRows JNI methods:
//   [0..4]  the 5 NativeCountResult metrics
//   [5]     numNonNullKeys
//   [6]     hasNullKey (0/1)
//   [7]     nullKeySum
//   [8..]   (key, sum) pairs, one per distinct non-null group
private object NativeTypedInt64UpdateResult {
  val MetricCount = 5
  val HeaderSize = 8
}
