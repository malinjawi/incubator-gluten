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

#pragma once

#include "state/VeloxNativeStateStore.h"
#include "velox/vector/ComplexVector.h"

#include <cstdint>
#include <vector>

namespace gluten {

struct NativeStreamingCountMetrics {
  int64_t numInputRows{0};
  int64_t numOutputRows{0};
  int64_t numUpdatedStateRows{0};
  int64_t numTotalStateRows{0};
  int64_t stateMemoryBytes{0};
};

struct NativeStreamingCountEvictionMetrics {
  int64_t numInputRows{0};
  int64_t numRemovedStateRows{0};
  int64_t numTotalStateRows{0};
  int64_t stateMemoryBytes{0};
};

struct NativeStreamingCountUpdateResult {
  NativeStreamingCountMetrics metrics;
  std::vector<StateBytes> updatedKeys;
  std::vector<StateBytes> updatedValues;
};

// Primitive-typed update result for the single-non-null-BIGINT-key + non-null-BIGINT-value shape.
// Instead of per-key UnsafeRow byte blobs, the distinct changed groups are returned as two parallel
// primitive vectors (keys[i] -> sums[i]), plus an optional running sum for the single SQL NULL key.
// This lets the JNI boundary hand the JVM a single jlongArray rather than a per-key byte[][], which
// removes the dominant per-key marshaling cost on the Update output path.
struct NativeStreamingTypedInt64UpdateResult {
  NativeStreamingCountMetrics metrics;
  std::vector<int64_t> keys;
  std::vector<int64_t> sums;
  bool hasNullKey{false};
  int64_t nullKeySum{0};
};

class VeloxNativeStreamingAggregation {
 public:
  explicit VeloxNativeStreamingAggregation(VeloxNativeStateStore& store);

  NativeStreamingCountMetrics incrementCount(const std::vector<StateBytes>& keys);

  NativeStreamingCountMetrics incrementCount(
      const std::vector<StateBytes>& keys,
      const std::vector<int64_t>& deltas);

  NativeStreamingCountMetrics incrementCount(
      const facebook::velox::RowVectorPtr& input,
      const std::vector<int32_t>& keyOrdinals);

  NativeStreamingCountMetrics incrementCountSingleInt64Key(
      const facebook::velox::RowVectorPtr& input,
      int32_t keyOrdinal);

  NativeStreamingCountUpdateResult incrementCountAndReturnKeys(
      const facebook::velox::RowVectorPtr& input,
      const std::vector<int32_t>& keyOrdinals);

  NativeStreamingCountUpdateResult incrementCountSingleInt64KeyAndReturnKeys(
      const facebook::velox::RowVectorPtr& input,
      int32_t keyOrdinal);

  NativeStreamingCountMetrics incrementLongSum(
      const std::vector<StateBytes>& keys,
      const std::vector<int64_t>& deltas);

  NativeStreamingCountMetrics incrementLongSum(
      const facebook::velox::RowVectorPtr& input,
      const std::vector<int32_t>& keyOrdinals,
      int32_t valueOrdinal);

  NativeStreamingCountUpdateResult incrementLongSumAndReturnKeys(
      const facebook::velox::RowVectorPtr& input,
      const std::vector<int32_t>& keyOrdinals,
      int32_t valueOrdinal);

  NativeStreamingCountUpdateResult incrementLongSumSingleInt64KeyAndReturnKeys(
      const facebook::velox::RowVectorPtr& input,
      int32_t keyOrdinal,
      int32_t valueOrdinal);

  // Same typed update as incrementLongSumSingleInt64KeyAndReturnKeys, but returns the distinct
  // changed groups as primitive int64 keys/sums (no per-key UnsafeRow allocation), so the JNI
  // boundary can return a single jlongArray. The resident state map, checkpoint/delta bytes, and
  // metrics are byte-for-byte identical to the byte[]-returning path.
  NativeStreamingTypedInt64UpdateResult incrementLongSumSingleInt64KeyAndReturnTypedRows(
      const facebook::velox::RowVectorPtr& input,
      int32_t keyOrdinal,
      int32_t valueOrdinal);

  // Primitive-typed analogue of incrementCountSingleInt64KeyAndReturnKeys.
  NativeStreamingTypedInt64UpdateResult incrementCountSingleInt64KeyAndReturnTypedRows(
      const facebook::velox::RowVectorPtr& input,
      int32_t keyOrdinal);

  NativeStreamingCountEvictionMetrics evictKeys(const std::vector<StateBytes>& keys);

 private:
  VeloxNativeStateStore& store_;
};

} // namespace gluten
