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
package org.apache.spark.sql.streaming

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.streaming.state.{ExperimentalVeloxNativeStateStoreProvider, VeloxNativeStateStoreJniWrapper, VeloxNativeStateStoreOperations, VeloxNativeStateStoreProvider}

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import org.apache.hadoop.conf.Configuration

import java.nio.file.{Files, Path => NioPath, Paths}
import java.util.Properties

import scala.jdk.CollectionConverters._
import scala.util.Try

class GlutenVeloxNativeStateStoreProviderSuite extends GlutenSQLTestsTrait {

  test("production Velox native StateStore provider requires global native state gate") {
    withStateStoreSqlConf(VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true") {
      val error = intercept[IllegalStateException] {
        initProductionProvider()
      }
      assert(error.getMessage.contains(GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key))
    }
  }

  test("production Velox native StateStore provider requires backend native state gate") {
    withStateStoreSqlConf(GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true") {
      val error = intercept[IllegalStateException] {
        initProductionProvider()
      }
      assert(
        error.getMessage.contains(VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key))
    }
  }

  test("production Velox native StateStore provider rejects unsupported state shapes at init") {
    withProductionNativeStateEnabled() {
      val columnFamilyError = intercept[UnsupportedOperationException] {
        initProductionProvider(useColumnFamilies = true)
      }
      assert(columnFamilyError.getMessage.contains("column families"))

      val multipleValuesError = intercept[UnsupportedOperationException] {
        initProductionProvider(useMultipleValuesPerKey = true)
      }
      assert(multipleValuesError.getMessage.contains("multiple values per key"))

      val prefixScanKeySchema = keySchema.add("suffix", IntegerType, nullable = false)
      val prefixScanError = intercept[UnsupportedOperationException] {
        initProductionProvider(
          providerKeySchema = prefixScanKeySchema,
          keyStateEncoderSpec = Some(PrefixKeyScanStateEncoderSpec(prefixScanKeySchema, 1)))
      }
      assert(prefixScanError.getMessage.contains("key state encoder PrefixKeyScanStateEncoderSpec"))

      val joinStoreError = intercept[UnsupportedOperationException] {
        initProductionProvider(stateStoreName = "left-keyWithIndexToValue")
      }
      assert(joinStoreError.getMessage.contains("stream-stream join state store"))
    }
  }

  test("production Velox native StateStore provider initializes before native store access") {
    withProductionNativeStateEnabled() {
      val provider = initProductionProvider()
      provider.close()
    }
  }

  test("production Velox native StateStore provider commits and reopens versions through JNI") {
    withProductionProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        store0.put(intRow(2), intRow(20))

        assert(store0.commit() == 1)
        assert(store0.hasCommitted)

        val store1 = provider.getStore(1)
        assert(readInt(store1.get(intRow(1))) == 10)
        assert(readInt(store1.get(intRow(2))) == 20)
        val metrics = store1.metrics
        assert(metrics.numKeys == 2)
        assert(provider.supportedCustomMetrics.map(_.name).toSet ==
          Set("veloxNativeStateStoreNumKeys", "veloxNativeStateStoreMemoryBytes"))
        assert(customMetricValues(metrics) ==
          Map(
            "veloxNativeStateStoreNumKeys" -> metrics.numKeys,
            "veloxNativeStateStoreMemoryBytes" -> metrics.memoryUsedBytes))
        assert(store1.iterator().map(pair => readInt(pair.key) -> readInt(pair.value)).toSet ==
          Set(1 -> 10, 2 -> 20))
        store1.release()
    }
  }

  test("production Velox native StateStore provider replays removes through JNI") {
    withProductionProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        assert(store0.commit() == 1)

        val store1 = provider.getStore(1)
        store1.remove(intRow(1))
        store1.put(intRow(2), intRow(20))
        assert(store1.commit() == 2)

        val store2 = provider.getStore(2)
        assert(store2.get(intRow(1)) == null)
        assert(readInt(store2.get(intRow(2))) == 20)
        assert(store2.metrics.numKeys == 1)
        store2.release()
    }
  }

  test("production Velox native StateStore provider abort discards uncommitted native writes") {
    withProductionProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        assert(store0.commit() == 1)

        val store1 = provider.getStore(1)
        store1.put(intRow(1), intRow(99))
        store1.put(intRow(3), intRow(30))
        store1.abort()

        val replayed = provider.getStore(1)
        assert(readInt(replayed.get(intRow(1))) == 10)
        assert(replayed.get(intRow(3)) == null)
        replayed.release()
    }
  }

  test("production Velox native StateStore provider handles same-version retry semantics") {
    withProductionProvider {
      provider =>
        val firstAttempt = provider.getStore(0)
        firstAttempt.put(intRow(1), intRow(10))
        assert(firstAttempt.commit() == 1)

        val idempotentRetry = provider.getStore(0)
        idempotentRetry.put(intRow(1), intRow(10))
        assert(idempotentRetry.commit() == 1)

        val reopened = provider.getStore(1)
        assert(readInt(reopened.get(intRow(1))) == 10)
        assert(reopened.metrics.numKeys == 1)
        reopened.release()

        val divergentRetry = provider.getStore(0)
        divergentRetry.put(intRow(1), intRow(11))
        val retryError = intercept[IllegalStateException] {
          divergentRetry.commit()
        }
        assert(retryError.getMessage.contains("already exists with different bytes"))
    }
  }

  test("production Velox native StateStore provider rejects invalid versions and empty snapshots") {
    withProductionNativeStateEnabled() {
      val checkpointRoot =
        Files.createTempDirectory("gluten-production-native-state-invalid-version").toString
      val provider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
      try {
        val negativeVersionError = intercept[RuntimeException] {
          provider.getStore(-1, None)
        }
        assert(negativeVersionError.getMessage.contains("non-negative version"))

        Files.write(
          nativeCheckpointDir(checkpointRoot).resolve("00000000000000000000.snapshot"),
          Array.emptyByteArray)
        val emptySnapshotError = intercept[RuntimeException] {
          provider.getStore(0)
        }
        assert(emptySnapshotError.getMessage.contains("empty snapshot"))
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native StateStore provider cleans temp files across retry failures") {
    withProductionNativeStateEnabled() {
      val checkpointRoot =
        Files.createTempDirectory("gluten-production-native-state-temp-cleanup").toString
      val provider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
      try {
        assertNoNativeTempFiles(checkpointRoot)
        assertNativeSnapshotFiles(checkpointRoot, Set.empty)

        val aborted = provider.getStore(0)
        aborted.put(intRow(1), intRow(99))
        aborted.abort()
        assertNoNativeTempFiles(checkpointRoot)
        assertNativeSnapshotFiles(checkpointRoot, Set.empty)

        val firstAttempt = provider.getStore(0)
        firstAttempt.put(intRow(1), intRow(10))
        assert(firstAttempt.commit() == 1)
        assertNoNativeTempFiles(checkpointRoot)
        assertNativeSnapshotFiles(checkpointRoot, Set("00000000000000000001.snapshot"))

        val idempotentRetry = provider.getStore(0)
        idempotentRetry.put(intRow(1), intRow(10))
        assert(idempotentRetry.commit() == 1)
        assertNoNativeTempFiles(checkpointRoot)
        assertNativeSnapshotFiles(checkpointRoot, Set("00000000000000000001.snapshot"))

        val divergentRetry = provider.getStore(0)
        divergentRetry.put(intRow(1), intRow(11))
        val retryError = intercept[IllegalStateException] {
          divergentRetry.commit()
        }
        assert(retryError.getMessage.contains("already exists with different bytes"))
        assertNoNativeTempFiles(checkpointRoot)
        assertNativeSnapshotFiles(checkpointRoot, Set("00000000000000000001.snapshot"))

        val reopened = provider.getStore(1)
        assert(readInt(reopened.get(intRow(1))) == 10)
        reopened.release()
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native StateStore provider maintenance prunes old snapshots") {
    withProductionNativeStateEnabled() {
      val checkpointRoot =
        Files.createTempDirectory("gluten-production-native-state-maintenance").toString
      val provider = initProductionProvider(
        checkpointRoot = Some(checkpointRoot),
        storeConf = retentionStateStoreConf(
          minVersionsToRetain = 2,
          minVersionsToDelete = 1L,
          maxVersionsToDeletePerMaintenance = 10)
      )
      try {
        for (version <- 0L until 6L) {
          val store = provider.getStore(version)
          store.put(intRow(version.toInt), intRow(version.toInt))
          assert(store.commit() == version + 1L)
        }
        assertNativeSnapshotFiles(
          checkpointRoot,
          (1L to 6L).map(version => f"$version%020d.snapshot").toSet)

        val pinnedStore = provider.getStore(4)
        try {
          provider.doMaintenance()
          assertNativeSnapshotFiles(
            checkpointRoot,
            Set(
              "00000000000000000004.snapshot",
              "00000000000000000005.snapshot",
              "00000000000000000006.snapshot"))
          assert(readInt(pinnedStore.get(intRow(3))) == 3)
        } finally {
          pinnedStore.release()
        }

        provider.doMaintenance()
        assertNativeSnapshotFiles(
          checkpointRoot,
          Set("00000000000000000005.snapshot", "00000000000000000006.snapshot"))

        val latest = provider.getStore(6)
        assert(readInt(latest.get(intRow(5))) == 5)
        latest.release()
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native StateStore provider retries cleanly after failed aborted batch") {
    withProductionProvider {
      provider =>
        val failedAttempt = provider.getStore(0)
        failedAttempt.put(intRow(1), intRow(99))
        failedAttempt.abort()

        val missingVersionError = intercept[IllegalStateException] {
          provider.getStore(1)
        }
        assert(missingVersionError.getMessage.contains("Missing Velox native state snapshot"))

        val retriedAttempt = provider.getStore(0)
        retriedAttempt.put(intRow(2), intRow(20))
        assert(retriedAttempt.commit() == 1)

        val replayed = provider.getStore(1)
        assert(replayed.get(intRow(1)) == null)
        assert(readInt(replayed.get(intRow(2))) == 20)
        assert(replayed.metrics.numKeys == 1)
        replayed.release()
    }
  }

  test("production Velox native count aggregation JNI bridge updates UnsafeRow long state") {
    withProductionNativeStateEnabled() {
      val provider = initProductionProvider(providerValueSchema = countValueSchema)
      try {
        val store0 = provider.getStore(0)
        val first = nativeCountMetrics(
          VeloxNativeStateStoreOperations.incrementCount(
            store0,
            Array(intRow(1).getBytes, intRow(1).getBytes, intRow(2).getBytes)))
        assert(first("numInputRows") == 3L)
        assert(first("numOutputRows") == 2L)
        assert(first("numUpdatedStateRows") == 3L)
        assert(first("numTotalStateRows") == 2L)
        assert(readLong(store0.get(intRow(1))) == 2L)
        assert(readLong(store0.get(intRow(2))) == 1L)
        assert(store0.commit() == 1)

        val store1 = provider.getStore(1)
        val replayed = nativeCountMetrics(
          VeloxNativeStateStoreOperations.incrementCount(
            store1,
            Array(intRow(1).getBytes, intRow(3).getBytes)))
        assert(replayed("numInputRows") == 2L)
        assert(replayed("numOutputRows") == 2L)
        assert(replayed("numUpdatedStateRows") == 2L)
        assert(replayed("numTotalStateRows") == 3L)
        assert(readLong(store1.get(intRow(1))) == 3L)
        assert(readLong(store1.get(intRow(2))) == 1L)
        assert(readLong(store1.get(intRow(3))) == 1L)
        assert(store1.commit() == 2)

        val reopened = provider.getStore(2)
        assert(readLong(reopened.get(intRow(1))) == 3L)
        assert(readLong(reopened.get(intRow(2))) == 1L)
        assert(readLong(reopened.get(intRow(3))) == 1L)
        reopened.release()
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native count aggregation JNI bridge adds partial deltas") {
    withProductionNativeStateEnabled() {
      val provider = initProductionProvider(providerValueSchema = countValueSchema)
      try {
        val store0 = provider.getStore(0)
        val first = nativeCountMetrics(
          VeloxNativeStateStoreOperations.incrementCount(
            store0,
            Array(intRow(1).getBytes, intRow(2).getBytes, intRow(1).getBytes),
            Array(10L, 20L, 5L)))
        assert(first("numInputRows") == 3L)
        assert(first("numOutputRows") == 2L)
        assert(first("numUpdatedStateRows") == 3L)
        assert(first("numTotalStateRows") == 2L)
        assert(readLong(store0.get(intRow(1))) == 15L)
        assert(readLong(store0.get(intRow(2))) == 20L)
        assert(store0.commit() == 1)

        val reopened = provider.getStore(1)
        assert(readLong(reopened.get(intRow(1))) == 15L)
        assert(readLong(reopened.get(intRow(2))) == 20L)
        reopened.release()
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native count aggregation JNI bridge evicts explicit keys") {
    withProductionNativeStateEnabled() {
      val provider = initProductionProvider(providerValueSchema = countValueSchema)
      try {
        val store0 = provider.getStore(0)
        nativeCountMetrics(
          VeloxNativeStateStoreOperations.incrementCount(
            store0,
            Array(intRow(1).getBytes, intRow(2).getBytes, intRow(3).getBytes),
            Array(10L, 20L, 30L)))
        val evicted = nativeCountEvictionMetrics(
          VeloxNativeStateStoreOperations.evictCountKeys(
            store0,
            Array(intRow(2).getBytes, intRow(4).getBytes, intRow(2).getBytes)))
        assert(evicted("numInputRows") == 3L)
        assert(evicted("numRemovedStateRows") == 1L)
        assert(evicted("numTotalStateRows") == 2L)
        assert(readLong(store0.get(intRow(1))) == 10L)
        assert(store0.get(intRow(2)) == null)
        assert(readLong(store0.get(intRow(3))) == 30L)
        assert(store0.commit() == 1)

        val reopened = provider.getStore(1)
        assert(readLong(reopened.get(intRow(1))) == 10L)
        assert(reopened.get(intRow(2)) == null)
        assert(readLong(reopened.get(intRow(3))) == 30L)
        reopened.release()
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native deduplicate JNI bridge emits first seen rows") {
    withProductionNativeStateEnabled() {
      var handle = 0L
      var replayedHandle = 0L
      try {
        handle = VeloxNativeStateStoreJniWrapper.nativeOpenStore(0, null)
        val first = nativeDedupeResult(
          VeloxNativeStateStoreJniWrapper.nativeDeduplicate(
            handle,
            Array(intRow(1).getBytes, intRow(1).getBytes, intRow(2).getBytes)))

        assert(first.outputIndices == Seq(0L, 2L))
        assert(first.metric("numInputRows") == 3L)
        assert(first.metric("numOutputRows") == 2L)
        assert(first.metric("numUpdatedStateRows") == 2L)
        assert(first.metric("numDroppedDuplicateRows") == 1L)
        assert(first.metric("numTotalStateRows") == 2L)

        val snapshot = VeloxNativeStateStoreJniWrapper.nativeCommit(handle)
        VeloxNativeStateStoreJniWrapper.nativeCloseStore(handle)
        handle = 0L

        replayedHandle = VeloxNativeStateStoreJniWrapper.nativeOpenStore(1, snapshot)
        val replayed = nativeDedupeResult(
          VeloxNativeStateStoreJniWrapper.nativeDeduplicate(
            replayedHandle,
            Array(intRow(1).getBytes, intRow(3).getBytes, intRow(2).getBytes)))

        assert(replayed.outputIndices == Seq(1L))
        assert(replayed.metric("numDroppedDuplicateRows") == 2L)
        assert(replayed.metric("numTotalStateRows") == 3L)
      } finally {
        if (handle != 0L) {
          VeloxNativeStateStoreJniWrapper.nativeCloseStore(handle)
        }
        if (replayedHandle != 0L) {
          VeloxNativeStateStoreJniWrapper.nativeCloseStore(replayedHandle)
        }
      }
    }
  }

  test("production Velox native deduplicate-with-watermark JNI bridge matches Spark ordering") {
    withProductionNativeStateEnabled() {
      var handle = 0L
      var replayedHandle = 0L
      var afterEvictionHandle = 0L
      try {
        handle = VeloxNativeStateStoreJniWrapper.nativeOpenStore(0, null)
        val first = nativeDedupeResult(
          VeloxNativeStateStoreJniWrapper.nativeDeduplicateWithinWatermark(
            handle,
            Array(intRow(1).getBytes, intRow(1).getBytes, intRow(2).getBytes),
            Array(1000L, 2000L, 3000L),
            false,
            0L,
            10000L,
            false,
            0L))

        assert(first.outputIndices == Seq(0L, 2L))
        assert(first.metric("numDroppedDuplicateRows") == 1L)
        assert(first.metric("numRowsDroppedByWatermark") == 0L)
        assert(first.metric("numTotalStateRows") == 2L)

        val snapshot = VeloxNativeStateStoreJniWrapper.nativeCommit(handle)
        VeloxNativeStateStoreJniWrapper.nativeCloseStore(handle)
        handle = 0L

        replayedHandle = VeloxNativeStateStoreJniWrapper.nativeOpenStore(1, snapshot)
        val duplicateThenEvict = nativeDedupeResult(
          VeloxNativeStateStoreJniWrapper.nativeDeduplicateWithinWatermark(
            replayedHandle,
            Array(intRow(1).getBytes, intRow(4).getBytes),
            Array(20000L, 500L),
            true,
            900L,
            10000L,
            true,
            12000L))

        assert(duplicateThenEvict.outputIndices.isEmpty)
        assert(duplicateThenEvict.metric("numDroppedDuplicateRows") == 1L)
        assert(duplicateThenEvict.metric("numRowsDroppedByWatermark") == 1L)
        assert(duplicateThenEvict.metric("numRemovedStateRows") == 1L)
        assert(duplicateThenEvict.metric("numTotalStateRows") == 1L)

        val afterEvictionSnapshot = VeloxNativeStateStoreJniWrapper.nativeCommit(replayedHandle)
        VeloxNativeStateStoreJniWrapper.nativeCloseStore(replayedHandle)
        replayedHandle = 0L

        afterEvictionHandle =
          VeloxNativeStateStoreJniWrapper.nativeOpenStore(2, afterEvictionSnapshot)
        val emittedAgain = nativeDedupeResult(
          VeloxNativeStateStoreJniWrapper.nativeDeduplicateWithinWatermark(
            afterEvictionHandle,
            Array(intRow(1).getBytes, intRow(2).getBytes),
            Array(25000L, 26000L),
            true,
            900L,
            10000L,
            false,
            0L))

        assert(emittedAgain.outputIndices == Seq(0L))
        assert(emittedAgain.metric("numDroppedDuplicateRows") == 1L)
        assert(emittedAgain.metric("numTotalStateRows") == 2L)
      } finally {
        if (handle != 0L) {
          VeloxNativeStateStoreJniWrapper.nativeCloseStore(handle)
        }
        if (replayedHandle != 0L) {
          VeloxNativeStateStoreJniWrapper.nativeCloseStore(replayedHandle)
        }
        if (afterEvictionHandle != 0L) {
          VeloxNativeStateStoreJniWrapper.nativeCloseStore(afterEvictionHandle)
        }
      }
    }
  }

  test(
    "production Velox native StateStore provider survives repeated commit abort reopen lifecycle") {
    withProductionProvider {
      provider =>
        var expected = Map.empty[Int, Int]
        for (version <- 0L until 60L) {
          val aborted = provider.getStore(version)
          aborted.put(intRow(1000 + version.toInt), intRow(-1))
          aborted.abort()

          val store = provider.getStore(version)
          val key = (version % 17).toInt
          store.put(intRow(key), intRow(version.toInt))
          expected += key -> version.toInt

          if (version % 5 == 0) {
            val removedKey = ((version + 7) % 17).toInt
            store.remove(intRow(removedKey))
            expected -= removedKey
          }

          assert(store.commit() == version + 1)

          val reopened = provider.getStore(version + 1)
          assert(reopened.metrics.numKeys == expected.size)
          expected.foreach {
            case (expectedKey, expectedValue) =>
              assert(readInt(reopened.get(intRow(expectedKey))) == expectedValue)
          }
          assert(reopened.iterator().map(pair => readInt(pair.key) -> readInt(pair.value)).toSet ==
            expected.toSet)
          reopened.release()
        }
    }
  }

  test("production Velox native StateStore provider reopens fresh providers over checkpoint") {
    withProductionNativeStateEnabled() {
      val checkpointRoot =
        Files.createTempDirectory("gluten-production-native-state-fresh-provider").toString
      val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
      val baselineIterators = VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles()
      var expected = Map.empty[Int, Int]

      for (version <- 0L until 40L) {
        val abortedProvider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
        try {
          val aborted = abortedProvider.getStore(version)
          aborted.put(intRow(10000 + version.toInt), intRow(-1))
          aborted.abort()
        } finally {
          abortedProvider.close()
        }
        assertNativeHandleCounts(baselineStores, baselineIterators)
        assertNoNativeTempFiles(checkpointRoot)

        val provider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
        try {
          val store = provider.getStore(version)
          for (offset <- 0 until 4) {
            val key = ((version * 4) + offset).toInt % 23
            val value = (version.toInt * 1000) + offset
            store.put(intRow(key), intRow(value))
            expected += key -> value
          }
          if (version % 6 == 0) {
            val removedKey = ((version + 11) % 23).toInt
            store.remove(intRow(removedKey))
            expected -= removedKey
          }
          assert(store.commit() == version + 1)
        } finally {
          provider.close()
        }
        assertNativeHandleCounts(baselineStores, baselineIterators)
        assertNoNativeTempFiles(checkpointRoot)

        val reopenedProvider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
        try {
          val reopened = reopenedProvider.getStore(version + 1)
          assert(reopened.metrics.numKeys == expected.size)
          assert(reopened.iterator().map(pair => readInt(pair.key) -> readInt(pair.value)).toSet ==
            expected.toSet)
          reopened.release()
        } finally {
          reopenedProvider.close()
        }
        assertNativeHandleCounts(baselineStores, baselineIterators)
        assertNoNativeTempFiles(checkpointRoot)
      }
    }
  }

  test("production Velox native StateStore provider closes store handles consistently") {
    withProductionProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        store0.release()

        assertClosedStore(store0.get(intRow(1)))
        assertClosedStore(store0.put(intRow(2), intRow(20)))
        assertClosedStore(store0.remove(intRow(1)))
        assertClosedStore(store0.iterator().hasNext)
        assertClosedStore(store0.commit())
        assertClosedStore(store0.removeColFamilyIfExists(StateStore.DEFAULT_COL_FAMILY_NAME))
        assertClosedStore(store0.createColFamilyIfAbsent(
          StateStore.DEFAULT_COL_FAMILY_NAME,
          keySchema,
          valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema)))
        store0.abort()

        val store1 = provider.getStore(0)
        store1.put(intRow(1), intRow(10))
        assert(store1.commit() == 1)
        assert(store1.hasCommitted)
        assert(store1.metrics.numKeys == 1)
        assertClosedStore(store1.put(intRow(2), intRow(20)))
        store1.abort()

        provider.close()
        val providerClosedError = intercept[IllegalStateException] {
          provider.getStore(1)
        }
        assert(providerClosedError.getMessage.contains("has been closed"))
    }
  }

  test("production Velox native StateStore provider close releases outstanding stores") {
    withProductionNativeStateEnabled() {
      val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
      val baselineIterators = VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles()
      val provider = initProductionProvider()
      var store: StateStore = null
      try {
        store = provider.getStore(0)
        store.put(intRow(1), intRow(10))
        val iterator = store.iterator()
        assert(iterator.hasNext)
        assertNativeHandleCounts(baselineStores + 1, baselineIterators + 1)
        provider.close()
        assertNativeHandleCounts(baselineStores, baselineIterators)
        assertClosedStore(store.get(intRow(1)))
        assertClosedIterator(iterator.hasNext)
        iterator.close()
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native StateStore provider releases stores on task completion") {
    withProductionNativeStateEnabled() {
      val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
      val baselineIterators = VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles()
      val provider = initProductionProvider()
      val context = taskContext()
      var store: StateStore = null
      try {
        TaskContext.withTaskContext(context) {
          store = provider.getStore(0)
          store.put(intRow(1), intRow(10))
          assertNativeHandleCounts(baselineStores + 1, baselineIterators)
        }

        assertNativeHandleCounts(baselineStores + 1, baselineIterators)
        context.markTaskCompleted(None)
        assertNativeHandleCounts(baselineStores, baselineIterators)
        assertClosedStore(store.get(intRow(1)))
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native StateStore provider closes outstanding iterator handles") {
    withProductionProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        store0.put(intRow(2), intRow(20))
        assert(store0.commit() == 1)

        val releasedStore = provider.getStore(1)
        val releasedIterator = releasedStore.iterator()
        assert(releasedIterator.hasNext)
        releasedStore.release()
        assertClosedIterator(releasedIterator.hasNext)
        releasedIterator.close()

        val committedStore = provider.getStore(1)
        val committedIterator = committedStore.iterator()
        assert(committedIterator.hasNext)
        committedStore.put(intRow(3), intRow(30))
        assert(committedStore.commit() == 2)
        assertClosedIterator(committedIterator.hasNext)
        committedIterator.close()
    }
  }

  test("production Velox native StateStore provider does not leak native handles") {
    withProductionProvider {
      provider =>
        val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
        val baselineIterators = VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles()
        var expected = Map.empty[Int, Int]

        val iterations = positiveIntTestParam(
          "gluten.native.streaming.state.provider.handleLeakIterations",
          "GLUTEN_NATIVE_STREAMING_STATE_PROVIDER_HANDLE_LEAK_ITERATIONS",
          20)

        for (version <- 0L until iterations.toLong) {
          assertNativeHandleCounts(baselineStores, baselineIterators)

          val aborted = provider.getStore(version)
          aborted.put(intRow(1000 + version.toInt), intRow(-1))
          val abortedIterator = aborted.iterator()
          assert(VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles() == baselineStores + 1)
          assert(
            VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles() ==
              baselineIterators + 1)
          aborted.abort()
          assertNativeHandleCounts(baselineStores, baselineIterators)
          abortedIterator.close()

          val store = provider.getStore(version)
          val key = (version % 7).toInt
          store.put(intRow(key), intRow(version.toInt))
          expected += key -> version.toInt

          val explicitlyClosedIterator = store.iterator()
          explicitlyClosedIterator.close()
          assert(VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles() == baselineStores + 1)
          assert(
            VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles() == baselineIterators)

          val leakedIterator = store.iterator()
          assert(
            VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles() ==
              baselineIterators + 1)
          assert(store.commit() == version + 1)
          assertNativeHandleCounts(baselineStores, baselineIterators)
          leakedIterator.close()

          val reopened = provider.getStore(version + 1)
          assert(reopened.metrics.numKeys == expected.size)
          expected.foreach {
            case (expectedKey, expectedValue) =>
              assert(readInt(reopened.get(intRow(expectedKey))) == expectedValue)
          }
          reopened.release()
          assertNativeHandleCounts(baselineStores, baselineIterators)
        }
    }
  }

  test("production Velox native StateStore provider validates checkpoint metadata") {
    withProductionNativeStateEnabled() {
      val checkpointRoot =
        Files.createTempDirectory("gluten-production-native-state-metadata").toString
      val provider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
      provider.close()

      val reopened = initProductionProvider(checkpointRoot = Some(checkpointRoot))
      reopened.close()

      val changedValueSchema = new StructType().add("value", LongType, nullable = false)
      val error = intercept[IllegalStateException] {
        initProductionProvider(
          checkpointRoot = Some(checkpointRoot),
          providerValueSchema = changedValueSchema)
      }
      assert(error.getMessage.contains("metadata mismatch"))
      assert(error.getMessage.contains("valueSchemaJson"))
    }
  }

  test("production Velox native StateStore provider fails unsupported state shapes clearly") {
    withProductionProvider {
      provider =>
        val store = provider.getStore(0)
        assertProductionUnsupported(store.putList(intRow(1), Array(intRow(1))))
        assertProductionUnsupported(store.merge(intRow(1), intRow(1)))
        assertProductionUnsupported(store.prefixScan(intRow(1)))
        assertProductionUnsupported(store.valuesIterator(intRow(1)).toSeq)
        assertProductionUnsupported(store.createColFamilyIfAbsent(
          "secondary",
          keySchema,
          valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema)))
        store.abort()
    }
  }

  test("production Velox native StateStore resident reuse commits and reopens versions") {
    withResidentReuseProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        store0.put(intRow(2), intRow(20))
        assert(store0.commit() == 1)

        // The next batch opens the version we just committed: the resident map
        // is reused, so a native store handle stays alive between batches.
        assert(VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles() >= 1L)

        val store1 = provider.getStore(1)
        assert(readInt(store1.get(intRow(1))) == 10)
        assert(readInt(store1.get(intRow(2))) == 20)
        store1.put(intRow(3), intRow(30))
        assert(store1.commit() == 2)

        val store2 = provider.getStore(2)
        assert(readInt(store2.get(intRow(1))) == 10)
        assert(readInt(store2.get(intRow(2))) == 20)
        assert(readInt(store2.get(intRow(3))) == 30)
        assert(store2.metrics.numKeys == 3)
        store2.release()
    }
  }

  test("production Velox native StateStore resident reuse rolls back aborted batch") {
    withResidentReuseProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        assert(store0.commit() == 1)

        // Reuse the resident map, mutate it, then abort: the rollback must undo
        // the uncommitted writes in place without re-reading the snapshot.
        val store1 = provider.getStore(1)
        store1.put(intRow(1), intRow(99))
        store1.put(intRow(3), intRow(30))
        store1.remove(intRow(1))
        store1.abort()

        val replayed = provider.getStore(1)
        assert(readInt(replayed.get(intRow(1))) == 10)
        assert(replayed.get(intRow(3)) == null)
        assert(replayed.metrics.numKeys == 1)
        replayed.release()
    }
  }

  test("production Velox native StateStore resident reuse replays removes") {
    withResidentReuseProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        store0.put(intRow(2), intRow(20))
        assert(store0.commit() == 1)

        val store1 = provider.getStore(1)
        store1.remove(intRow(1))
        assert(store1.commit() == 2)

        val store2 = provider.getStore(2)
        assert(store2.get(intRow(1)) == null)
        assert(readInt(store2.get(intRow(2))) == 20)
        assert(store2.metrics.numKeys == 1)
        store2.release()
    }
  }

  test("production Velox native StateStore resident reuse falls back on version mismatch") {
    withProductionNativeStateEnabled(residentReuse = true) {
      val checkpointRoot =
        Files.createTempDirectory("gluten-production-native-state-reuse-coldstart").toString
      val provider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
      try {
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        assert(store0.commit() == 1)

        val store1 = provider.getStore(1)
        store1.put(intRow(2), intRow(20))
        assert(store1.commit() == 2)

        // A non-latest version is not resident, so it cold-starts from disk.
        val store0Again = provider.getStore(0)
        assert(store0Again.get(intRow(2)) == null)
        assert(store0Again.metrics.numKeys == 0)
        store0Again.release()
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native StateStore resident reuse releases handle on provider close") {
    withProductionNativeStateEnabled(residentReuse = true) {
      val baselineStores = VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles()
      val provider = initProductionProvider()
      try {
        val store = provider.getStore(0)
        store.put(intRow(1), intRow(10))
        assert(store.commit() == 1)
        // The committed map stays resident, so one extra handle survives.
        assert(VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles() == baselineStores + 1)
      } finally {
        provider.close()
      }
      // Provider close must free the resident handle.
      assert(VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles() == baselineStores)
    }
  }

  for (residentReuse <- Seq(false, true)) {
    val mode = if (residentReuse) "resident-reuse" else "cold"

    test(s"production Velox native StateStore delta checkpoint ($mode) commits and reopens") {
      withDeltaCheckpointProvider(residentReuse) {
        provider =>
          val store0 = provider.getStore(0)
          store0.put(intRow(1), intRow(10))
          store0.put(intRow(2), intRow(20))
          assert(store0.commit() == 1)

          val store1 = provider.getStore(1)
          assert(readInt(store1.get(intRow(1))) == 10)
          assert(readInt(store1.get(intRow(2))) == 20)
          store1.put(intRow(3), intRow(30))
          assert(store1.commit() == 2)

          val store2 = provider.getStore(2)
          assert(readInt(store2.get(intRow(1))) == 10)
          assert(readInt(store2.get(intRow(2))) == 20)
          assert(readInt(store2.get(intRow(3))) == 30)
          assert(store2.metrics.numKeys == 3)
          store2.release()
      }
    }

    test(s"production Velox native StateStore delta checkpoint ($mode) replays removes") {
      withDeltaCheckpointProvider(residentReuse) {
        provider =>
          val store0 = provider.getStore(0)
          store0.put(intRow(1), intRow(10))
          store0.put(intRow(2), intRow(20))
          assert(store0.commit() == 1)

          val store1 = provider.getStore(1)
          store1.remove(intRow(1))
          assert(store1.commit() == 2)

          val store2 = provider.getStore(2)
          assert(store2.get(intRow(1)) == null)
          assert(readInt(store2.get(intRow(2))) == 20)
          assert(store2.metrics.numKeys == 1)
          store2.release()
      }
    }

    test(
      s"production Velox native StateStore delta checkpoint ($mode) abort discards uncommitted") {
      withDeltaCheckpointProvider(residentReuse) {
        provider =>
          val store0 = provider.getStore(0)
          store0.put(intRow(1), intRow(10))
          assert(store0.commit() == 1)

          val store1 = provider.getStore(1)
          store1.put(intRow(1), intRow(99))
          store1.put(intRow(3), intRow(30))
          store1.abort()

          val replayed = provider.getStore(1)
          assert(readInt(replayed.get(intRow(1))) == 10)
          assert(replayed.get(intRow(3)) == null)
          replayed.release()
      }
    }
  }

  test("production Velox native StateStore delta checkpoint writes deltas with periodic snapshots") {
    withProductionNativeStateEnabled(deltaCheckpoint = true) {
      val checkpointRoot =
        Files.createTempDirectory("gluten-production-native-state-delta-layout").toString
      // minDeltasForSnapshot defaults to 10, so version 1 (the first base) and version 10 get full
      // snapshots; versions 2..9 and 11 are delta-only.
      val provider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
      try {
        for (version <- 0L until 11L) {
          val store = provider.getStore(version)
          store.put(intRow(version.toInt), intRow(version.toInt))
          assert(store.commit() == version + 1L)
        }
        assertNativeSnapshotFiles(
          checkpointRoot,
          Set("00000000000000000001.snapshot", "00000000000000000010.snapshot"))
        assertNativeDeltaFiles(
          checkpointRoot,
          (1L to 11L).map(version => f"$version%020d.delta").toSet)

        // Reconstruct the latest version from snapshot@10 + delta@11 and verify all keys survive.
        val reopened = provider.getStore(11)
        assert(reopened.metrics.numKeys == 11)
        (0 until 11).foreach(key => assert(readInt(reopened.get(intRow(key))) == key))
        reopened.release()
      } finally {
        provider.close()
      }
    }
  }

  test("production Velox native StateStore delta checkpoint reopens fresh providers (restart)") {
    withProductionNativeStateEnabled(deltaCheckpoint = true) {
      val checkpointRoot =
        Files.createTempDirectory("gluten-production-native-state-delta-restart").toString
      var expected = Map.empty[Int, Int]
      for (version <- 0L until 15L) {
        // A brand-new provider per version simulates an executor restart: every getStore must
        // cold-start by replaying the delta chain onto the latest snapshot.
        val provider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
        try {
          val store = provider.getStore(version)
          val key = (version % 7).toInt
          val value = version.toInt
          store.put(intRow(key), intRow(value))
          expected += key -> value
          if (version % 4 == 0) {
            val removedKey = ((version + 3) % 7).toInt
            store.remove(intRow(removedKey))
            expected -= removedKey
          }
          assert(store.commit() == version + 1L)
        } finally {
          provider.close()
        }

        val reopenedProvider = initProductionProvider(checkpointRoot = Some(checkpointRoot))
        try {
          val reopened = reopenedProvider.getStore(version + 1L)
          assert(reopened.metrics.numKeys == expected.size)
          expected.foreach {
            case (expectedKey, expectedValue) =>
              assert(readInt(reopened.get(intRow(expectedKey))) == expectedValue)
          }
          assert(reopened.iterator().map(pair => readInt(pair.key) -> readInt(pair.value)).toSet ==
            expected.toSet)
          reopened.release()
        } finally {
          reopenedProvider.close()
        }
      }
    }
  }

  test("production Velox native StateStore delta checkpoint maintenance prunes superseded files") {
    withProductionNativeStateEnabled(deltaCheckpoint = true) {
      val checkpointRoot =
        Files.createTempDirectory("gluten-production-native-state-delta-maintenance").toString
      val provider = initProductionProvider(
        checkpointRoot = Some(checkpointRoot),
        storeConf = retentionStateStoreConf(
          minVersionsToRetain = 2,
          minVersionsToDelete = 1L,
          maxVersionsToDeletePerMaintenance = 100)
      )
      try {
        for (version <- 0L until 12L) {
          val store = provider.getStore(version)
          store.put(intRow(version.toInt), intRow(version.toInt))
          assert(store.commit() == version + 1L)
        }
        // snapshot@1 and snapshot@10 exist before maintenance; retaining the last 2 versions keeps
        // snapshot@10 as the base for versions 11/12, so everything below version 10 is prunable.
        provider.doMaintenance()
        assertNativeSnapshotFiles(checkpointRoot, Set("00000000000000000010.snapshot"))
        assert(
          nativeCheckpointFiles(checkpointRoot)
            .map(_.getFileName.toString)
            .filter(_.endsWith(".delta"))
            .forall(_ >= "00000000000000000010.delta"))

        val latest = provider.getStore(12)
        assert(readInt(latest.get(intRow(11))) == 11)
        latest.release()
      } finally {
        provider.close()
      }
    }
  }

  test("experimental Velox native StateStore provider commits and reopens versions") {
    withProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        store0.put(intRow(2), intRow(20))

        assert(store0.commit() == 1)
        assert(store0.hasCommitted)

        val store1 = provider.getStore(1)
        assert(readInt(store1.get(intRow(1))) == 10)
        assert(readInt(store1.get(intRow(2))) == 20)
        assert(store1.metrics.numKeys == 2)
        assert(store1.iterator().map(pair => readInt(pair.key) -> readInt(pair.value)).toSet ==
          Set(1 -> 10, 2 -> 20))
        store1.release()
    }
  }

  test("experimental Velox native StateStore provider replays removes") {
    withProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        assert(store0.commit() == 1)

        val store1 = provider.getStore(1)
        store1.remove(intRow(1))
        store1.put(intRow(2), intRow(20))
        assert(store1.commit() == 2)

        val store2 = provider.getStore(2)
        assert(store2.get(intRow(1)) == null)
        assert(readInt(store2.get(intRow(2))) == 20)
        assert(store2.metrics.numKeys == 1)
        store2.release()
    }
  }

  test("experimental Velox native StateStore provider abort discards uncommitted writes") {
    withProvider {
      provider =>
        val store0 = provider.getStore(0)
        store0.put(intRow(1), intRow(10))
        assert(store0.commit() == 1)

        val store1 = provider.getStore(1)
        store1.put(intRow(1), intRow(99))
        store1.put(intRow(3), intRow(30))
        store1.abort()

        val replayed = provider.getStore(1)
        assert(readInt(replayed.get(intRow(1))) == 10)
        assert(replayed.get(intRow(3)) == null)
        replayed.release()
    }
  }

  test("experimental Velox native StateStore provider fails unsupported state shapes clearly") {
    withProvider {
      provider =>
        val store = provider.getStore(0)
        assertUnsupported(store.putList(intRow(1), Array(intRow(1))))
        assertUnsupported(store.merge(intRow(1), intRow(1)))
        assertUnsupported(store.prefixScan(intRow(1)))
        assertUnsupported(store.valuesIterator(intRow(1)).toSeq)
        assertUnsupported(store.createColFamilyIfAbsent(
          "secondary",
          keySchema,
          valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema)))
        store.abort()
    }
  }

  test("experimental Velox native StateStore provider fails column families at init") {
    val provider = new ExperimentalVeloxNativeStateStoreProvider()
    val error = intercept[UnsupportedOperationException] {
      provider.init(
        StateStoreId(Files.createTempDirectory("gluten-native-state").toString, 0L, 0),
        keySchema,
        valueSchema,
        NoPrefixKeyStateEncoderSpec(keySchema),
        useColumnFamilies = true,
        StateStoreConf.empty,
        new Configuration(),
        useMultipleValuesPerKey = false,
        None
      )
    }
    assert(error.getMessage.contains("column families"))
  }

  private def withProvider(testBody: ExperimentalVeloxNativeStateStoreProvider => Unit): Unit = {
    val checkpointRoot = Files.createTempDirectory("gluten-native-state").toString
    val provider = new ExperimentalVeloxNativeStateStoreProvider()
    provider.init(
      StateStoreId(checkpointRoot, operatorId = 0L, partitionId = 0),
      keySchema,
      valueSchema,
      NoPrefixKeyStateEncoderSpec(keySchema),
      useColumnFamilies = false,
      StateStoreConf.empty,
      new Configuration(),
      useMultipleValuesPerKey = false,
      None
    )
    try {
      testBody(provider)
    } finally {
      provider.close()
    }
  }

  private def withProductionNativeStateEnabled(
      residentReuse: Boolean = false,
      deltaCheckpoint: Boolean = false)(
      testBody: => Unit): Unit = {
    withStateStoreSqlConf(
      GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key -> "true",
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_RESIDENT_REUSE_ENABLED.key ->
        residentReuse.toString,
      VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_DELTA_CHECKPOINT_ENABLED.key ->
        deltaCheckpoint.toString) {
      testBody
    }
  }

  private def withProductionProvider(testBody: VeloxNativeStateStoreProvider => Unit): Unit = {
    withProductionNativeStateEnabled() {
      val provider = initProductionProvider()
      try {
        testBody(provider)
      } finally {
        provider.close()
      }
    }
  }

  private def withResidentReuseProvider(
      testBody: VeloxNativeStateStoreProvider => Unit): Unit = {
    withProductionNativeStateEnabled(residentReuse = true) {
      val provider = initProductionProvider()
      try {
        testBody(provider)
      } finally {
        provider.close()
      }
    }
  }

  private def withDeltaCheckpointProvider(residentReuse: Boolean)(
      testBody: VeloxNativeStateStoreProvider => Unit): Unit = {
    withProductionNativeStateEnabled(residentReuse = residentReuse, deltaCheckpoint = true) {
      val provider = initProductionProvider()
      try {
        testBody(provider)
      } finally {
        provider.close()
      }
    }
  }

  private def withStateStoreSqlConf(confs: (String, String)*)(testBody: => Unit): Unit = {
    withSQLConf(confs: _*) {
      testBody
    }
  }

  private def initProductionProvider(
      checkpointRoot: Option[String] = None,
      providerKeySchema: StructType = keySchema,
      providerValueSchema: StructType = valueSchema,
      keyStateEncoderSpec: Option[KeyStateEncoderSpec] = None,
      useColumnFamilies: Boolean = false,
      useMultipleValuesPerKey: Boolean = false,
      stateStoreName: String = StateStoreId.DEFAULT_STORE_NAME,
      storeConf: StateStoreConf = StateStoreConf.empty): VeloxNativeStateStoreProvider = {
    val provider = new VeloxNativeStateStoreProvider()
    provider.init(
      StateStoreId(
        checkpointRoot.getOrElse(
          Files.createTempDirectory("gluten-production-native-state").toString),
        operatorId = 0L,
        partitionId = 0,
        storeName = stateStoreName),
      providerKeySchema,
      providerValueSchema,
      keyStateEncoderSpec.getOrElse(NoPrefixKeyStateEncoderSpec(providerKeySchema)),
      useColumnFamilies,
      storeConf,
      new Configuration(),
      useMultipleValuesPerKey,
      None
    )
    provider
  }

  private def keySchema: StructType = new StructType().add("key", IntegerType, nullable = false)

  private def valueSchema: StructType = new StructType().add("value", IntegerType, nullable = false)

  private def countValueSchema: StructType =
    new StructType().add("count", LongType, nullable = false)

  private def retentionStateStoreConf(
      minVersionsToRetain: Int,
      minVersionsToDelete: Long,
      maxVersionsToDeletePerMaintenance: Int): StateStoreConf = {
    val retain = minVersionsToRetain
    val delete = minVersionsToDelete
    val maxDelete = maxVersionsToDeletePerMaintenance
    new StateStoreConf(new SQLConf) {
      override val minVersionsToRetain: Int = retain
      override val minVersionsToDelete: Long = delete
      override val maxVersionsToDeletePerMaintenance: Int = maxDelete
    }
  }

  private def intRow(value: Int): UnsafeRow = {
    val row = new UnsafeRow(1)
    val bytes = new Array[Byte](16)
    row.pointTo(bytes, bytes.length)
    row.setInt(0, value)
    row.copy()
  }

  private def readInt(row: UnsafeRow): Int = {
    assert(row != null)
    row.getInt(0)
  }

  private def readLong(row: UnsafeRow): Long = {
    assert(row != null)
    row.getLong(0)
  }

  private def assertUnsupported(f: => Any): Unit = {
    val error = intercept[UnsupportedOperationException](f)
    assert(error.getMessage.contains("Experimental Velox native StateStore POC"))
  }

  private def assertProductionUnsupported(f: => Any): Unit = {
    val error = intercept[UnsupportedOperationException](f)
    assert(
      error.getMessage.contains("Velox native StateStore supports only default single-value state"))
  }

  private case class NativeDedupeResult(outputIndices: Seq[Long], metrics: Map[String, Long]) {
    def metric(name: String): Long = metrics(name)
  }

  private def nativeCountMetrics(raw: Array[Long]): Map[String, Long] = {
    val metricNames = Seq(
      "numInputRows",
      "numOutputRows",
      "numUpdatedStateRows",
      "numTotalStateRows",
      "stateMemoryBytes"
    )
    assert(raw.length == metricNames.size)
    metricNames.zip(raw).toMap
  }

  private def nativeCountEvictionMetrics(raw: Array[Long]): Map[String, Long] = {
    val metricNames = Seq(
      "numInputRows",
      "numRemovedStateRows",
      "numTotalStateRows",
      "stateMemoryBytes"
    )
    assert(raw.length == metricNames.size)
    metricNames.zip(raw).toMap
  }

  private def nativeDedupeResult(raw: Array[Long]): NativeDedupeResult = {
    assert(raw.nonEmpty)
    val outputCount = raw(0).toInt
    val metricNames = Seq(
      "numInputRows",
      "numOutputRows",
      "numUpdatedStateRows",
      "numDroppedDuplicateRows",
      "numRowsDroppedByWatermark",
      "numRemovedStateRows",
      "numTotalStateRows",
      "stateMemoryBytes"
    )
    assert(raw.length == 1 + outputCount + metricNames.size)
    val outputIndices = raw.slice(1, 1 + outputCount).toSeq
    val metrics = metricNames.zip(raw.drop(1 + outputCount)).toMap
    NativeDedupeResult(outputIndices, metrics)
  }

  private def taskContext(): TaskContextImpl = {
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      taskAttemptId = 0L,
      attemptNumber = 0,
      numPartitions = 1,
      taskMemoryManager = null,
      localProperties = new Properties(),
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpus = 1,
      resources = Map.empty
    )
  }

  private def assertClosedStore(f: => Any): Unit = {
    val error = intercept[IllegalStateException](f)
    assert(error.getMessage.contains("already closed"))
  }

  private def assertClosedIterator(f: => Any): Unit = {
    val error = intercept[RuntimeException](f)
    assert(error.getMessage.contains("Unknown Velox native StateStore handle"))
  }

  private def assertNativeHandleCounts(expectedStores: Long, expectedIterators: Long): Unit = {
    assert(VeloxNativeStateStoreJniWrapper.nativeActiveStoreHandles() == expectedStores)
    assert(VeloxNativeStateStoreJniWrapper.nativeActiveIteratorHandles() == expectedIterators)
  }

  private def positiveIntTestParam(property: String, env: String, defaultValue: Int): Int = {
    sys.props.get(property).orElse(sys.env.get(env)).map {
      value =>
        Try(value.toInt)
          .filter(_ > 0)
          .getOrElse(fail(s"$property/$env must be a positive integer, got '$value'"))
    }.getOrElse(defaultValue)
  }

  private def customMetricValues(metrics: StateStoreMetrics): Map[String, Long] = {
    metrics.customMetrics.map { case (metric, value) => metric.name -> value }
  }

  private def assertNoNativeTempFiles(checkpointRoot: String): Unit = {
    val tempFiles = nativeCheckpointFiles(checkpointRoot)
      .filter(_.getFileName.toString.endsWith(".tmp"))
    assert(tempFiles.isEmpty, s"Unexpected Velox native StateStore temp files: $tempFiles")
  }

  private def assertNativeSnapshotFiles(checkpointRoot: String, expected: Set[String]): Unit = {
    val snapshots = nativeCheckpointFiles(checkpointRoot)
      .map(_.getFileName.toString)
      .filter(_.endsWith(".snapshot"))
      .toSet
    assert(snapshots == expected)
  }

  private def assertNativeDeltaFiles(checkpointRoot: String, expected: Set[String]): Unit = {
    val deltas = nativeCheckpointFiles(checkpointRoot)
      .map(_.getFileName.toString)
      .filter(_.endsWith(".delta"))
      .toSet
    assert(deltas == expected)
  }

  private def nativeCheckpointFiles(checkpointRoot: String): Seq[NioPath] = {
    val stream = Files.walk(Paths.get(checkpointRoot))
    try {
      stream.iterator().asScala.filter(Files.isRegularFile(_)).toSeq
    } finally {
      stream.close()
    }
  }

  private def nativeCheckpointDir(checkpointRoot: String): NioPath = {
    nativeCheckpointFiles(checkpointRoot)
      .find(_.getFileName.toString == "_metadata.properties")
      .map(_.getParent)
      .getOrElse(fail(s"Could not find Velox native StateStore metadata under $checkpointRoot"))
  }
}
