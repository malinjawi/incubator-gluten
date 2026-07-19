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
package org.apache.gluten.execution.streaming.state;

public final class VeloxNativeStateStoreJniWrapper {
  private VeloxNativeStateStoreJniWrapper() {}

  public static native long nativeOpenStore(long version, byte[] snapshot);

  public static native byte[] nativeGet(long storeHandle, byte[] key);

  public static native void nativePut(long storeHandle, byte[] key, byte[] value);

  public static native void nativeRemove(long storeHandle, byte[] key);

  public static native byte[] nativeCommit(long storeHandle);

  /**
   * Serializes only the keys mutated since the store was opened or since the dirty set was last
   * cleared (changed key/value pairs plus tombstones for removed keys). Used to write per-version
   * delta checkpoints.
   */
  public static native byte[] nativeCommitDelta(long storeHandle);

  /** Applies a delta produced by {@link #nativeCommitDelta} onto a resident store map. */
  public static native void nativeApplyDelta(long storeHandle, byte[] delta);

  /** Clears the dirty key set after a delta or snapshot has been durably written. */
  public static native void nativeClearDirty(long storeHandle);

  /**
   * Reverts every key mutated since the store was opened or last cleared back to its pre-batch
   * value, discarding an uncommitted batch on a resident store in O(keys mutated this batch).
   */
  public static native void nativeRollback(long storeHandle);

  /** Advances the logical version of a resident store so it can be reused for the next batch. */
  public static native void nativeSetVersion(long storeHandle, long version);

  public static native void nativeCloseStore(long storeHandle);

  public static native long nativeIterator(long storeHandle);

  public static native boolean nativeIteratorHasNext(long iteratorHandle);

  public static native byte[][] nativeIteratorNext(long iteratorHandle);

  public static native void nativeCloseIterator(long iteratorHandle);

  public static native long[] nativeMetrics(long storeHandle);

  /**
   * Increments one Spark-compatible UnsafeRow long count value per key occurrence and returns
   * [numInputRows, numOutputRows, numUpdatedStateRows, numTotalStateRows, stateMemoryBytes].
   */
  public static native long[] nativeIncrementCount(long storeHandle, byte[][] keys);

  /**
   * Adds one Spark-compatible UnsafeRow long count delta per key and returns [numInputRows,
   * numOutputRows, numUpdatedStateRows, numTotalStateRows, stateMemoryBytes].
   */
  public static native long[] nativeIncrementCountWithDeltas(
      long storeHandle, byte[][] keys, long[] deltas);

  /**
   * Derives Spark-compatible UnsafeRow grouping keys from a Velox columnar batch and increments one
   * count per input row. Returns [numInputRows, numOutputRows, numUpdatedStateRows,
   * numTotalStateRows, stateMemoryBytes].
   */
  public static native long[] nativeIncrementCountFromColumnarBatch(
      long storeHandle, long batchHandle, int[] keyOrdinals);

  /**
   * Same update as nativeIncrementCountFromColumnarBatch, but also returns the distinct changed
   * Spark-compatible UnsafeRow grouping keys for Update-mode output. Result layout is [long[]
   * metrics, byte[][] updatedKeys].
   */
  public static native Object[] nativeIncrementCountFromColumnarBatchWithKeys(
      long storeHandle, long batchHandle, int[] keyOrdinals);

  /**
   * Same update as nativeIncrementCountFromColumnarBatchWithKeys, but returns final changed
   * key/value pairs so Spark Update mode can emit rows without one StateStore get per key. Result
   * layout is [long[] metrics, byte[][] updatedKeys, byte[][] updatedValues].
   */
  public static native Object[] nativeIncrementCountFromColumnarBatchWithKeyValues(
      long storeHandle, long batchHandle, int[] keyOrdinals);

  /**
   * Same count update contract as nativeIncrementCountFromColumnarBatchWithKeyValues, but uses a
   * typed native path for the narrow Spark-compatible shape with one BIGINT grouping key. Result
   * layout is [long[] metrics, byte[][] updatedKeys, byte[][] updatedRows].
   */
  public static native Object[] nativeIncrementCountSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
      long storeHandle, long batchHandle, int keyOrdinal, int valueFieldCount);

  /**
   * Same update as nativeIncrementCountFromColumnarBatch, but uses a typed native path for the
   * narrow Spark-compatible shape with one BIGINT grouping key and returns metrics only.
   */
  public static native long[] nativeIncrementCountSingleInt64KeyFromColumnarBatch(
      long storeHandle, long batchHandle, int keyOrdinal);

  /**
   * Adds one Spark-compatible UnsafeRow long sum delta per key and returns [numInputRows,
   * numOutputRows, numUpdatedStateRows, numTotalStateRows, stateMemoryBytes].
   */
  public static native long[] nativeIncrementLongSumWithDeltas(
      long storeHandle, byte[][] keys, long[] deltas);

  /**
   * Derives Spark-compatible UnsafeRow grouping keys and non-null long sum deltas from a Velox
   * columnar batch. Returns [numInputRows, numOutputRows, numUpdatedStateRows, numTotalStateRows,
   * stateMemoryBytes].
   */
  public static native long[] nativeIncrementLongSumFromColumnarBatch(
      long storeHandle, long batchHandle, int[] keyOrdinals, int valueOrdinal);

  /**
   * Same update as nativeIncrementLongSumFromColumnarBatch, but also returns the distinct changed
   * Spark-compatible UnsafeRow grouping keys for Update-mode output. Result layout is [long[]
   * metrics, byte[][] updatedKeys].
   */
  public static native Object[] nativeIncrementLongSumFromColumnarBatchWithKeys(
      long storeHandle, long batchHandle, int[] keyOrdinals, int valueOrdinal);

  /**
   * Same update as nativeIncrementLongSumFromColumnarBatchWithKeys, but returns final changed
   * key/value pairs so Spark Update mode can emit rows without one StateStore get per key. Result
   * layout is [long[] metrics, byte[][] updatedKeys, byte[][] updatedValues].
   */
  public static native Object[] nativeIncrementLongSumFromColumnarBatchWithKeyValues(
      long storeHandle, long batchHandle, int[] keyOrdinals, int valueOrdinal);

  /**
   * Same update as nativeIncrementLongSumFromColumnarBatchWithKeyValues, but returns
   * Spark-compatible joined UnsafeRows for fixed-width Update-mode output. Result layout is [long[]
   * metrics, byte[][] updatedKeys, byte[][] updatedRows].
   */
  public static native Object[] nativeIncrementLongSumFromColumnarBatchWithFixedWidthRows(
      long storeHandle, long batchHandle, int[] keyOrdinals, int valueOrdinal, int valueFieldCount);

  /**
   * Same output contract as nativeIncrementLongSumFromColumnarBatchWithFixedWidthRows, but uses a
   * typed native path for the narrow Spark-compatible shape with one BIGINT grouping key and one
   * non-null BIGINT sum value. Spark StateStore versioning/checkpoint bytes remain authoritative.
   */
  public static native Object[]
      nativeIncrementLongSumSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
          long storeHandle,
          long batchHandle,
          int keyOrdinal,
          int valueOrdinal,
          int valueFieldCount);

  /**
   * Same typed long-sum update as
   * nativeIncrementLongSumSingleInt64KeyFromColumnarBatchWithFixedWidthRows, but returns the changed
   * groups as a single primitive {@code long[]} instead of a per-key {@code byte[][]}. Layout:
   * [numInputRows, numOutputRows, numUpdatedStateRows, numTotalStateRows, stateMemoryBytes,
   * numNonNullKeys, hasNullKey(0/1), nullKeySum, key_0, sum_0, key_1, sum_1, ...]. The resident
   * state, checkpoint bytes, and metrics are identical to the byte[]-returning path.
   */
  public static native long[] nativeIncrementLongSumSingleInt64KeyFromColumnarBatchTypedRows(
      long storeHandle, long batchHandle, int keyOrdinal, int valueOrdinal);

  /**
   * Same typed count update contract as
   * nativeIncrementLongSumSingleInt64KeyFromColumnarBatchTypedRows (unit deltas, count value).
   * Layout matches that method.
   */
  public static native long[] nativeIncrementCountSingleInt64KeyFromColumnarBatchTypedRows(
      long storeHandle, long batchHandle, int keyOrdinal);

  /**
   * Removes count aggregation state for explicit Spark UnsafeRow keys and returns [numInputRows,
   * numRemovedStateRows, numTotalStateRows, stateMemoryBytes].
   */
  public static native long[] nativeEvictCountKeys(long storeHandle, byte[][] keys);

  /**
   * Returns [outputCount, output row indices..., numInputRows, numOutputRows, numUpdatedStateRows,
   * numDroppedDuplicateRows, numRowsDroppedByWatermark, numRemovedStateRows, numTotalStateRows,
   * stateMemoryBytes].
   */
  public static native long[] nativeDeduplicate(long storeHandle, byte[][] keys);

  /**
   * Returns [outputCount, output row indices..., numInputRows, numOutputRows, numUpdatedStateRows,
   * numDroppedDuplicateRows, numRowsDroppedByWatermark, numRemovedStateRows, numTotalStateRows,
   * stateMemoryBytes].
   */
  public static native long[] nativeDeduplicateWithinWatermark(
      long storeHandle,
      byte[][] keys,
      long[] eventTimeMicros,
      boolean hasLateWatermark,
      long lateWatermarkMicros,
      long delayThresholdMicros,
      boolean hasEvictionWatermark,
      long evictionWatermarkMicros);

  public static native long nativeActiveStoreHandles();

  public static native long nativeActiveIteratorHandles();
}
