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

#include "state/VeloxNativeStreamingDeduplicate.h"

#include "utils/Exception.h"

#include <cstring>
#include <limits>

namespace gluten {
namespace {

StateBytes emptyDeduplicateValue() {
  return {};
}

void appendInt64(StateBytes& bytes, int64_t value) {
  uint64_t raw = 0;
  static_assert(sizeof(raw) == sizeof(value));
  std::memcpy(&raw, &value, sizeof(value));

  for (auto offset = 0; offset < 64; offset += 8) {
    bytes.push_back(static_cast<uint8_t>((raw >> offset) & 0xff));
  }
}

int64_t readInt64(const StateBytes& bytes, size_t start) {
  GLUTEN_CHECK(
      bytes.size() >= start + sizeof(int64_t),
      "Corrupt native streaming deduplicate state value: truncated int64");

  uint64_t raw = 0;
  for (size_t i = 0; i < sizeof(int64_t); ++i) {
    raw |= static_cast<uint64_t>(bytes[start + i]) << (i * 8);
  }

  int64_t value = 0;
  std::memcpy(&value, &raw, sizeof(value));
  return value;
}

StateBytes encodeExpiresAtMicros(int64_t value) {
  StateBytes bytes;
  bytes.reserve(sizeof(int64_t) * 2);
  appendInt64(bytes, 0);
  appendInt64(bytes, value);
  return bytes;
}

int64_t decodeExpiresAtMicros(const StateBytes& bytes) {
  if (bytes.size() == sizeof(int64_t)) {
    return readInt64(bytes, 0);
  }

  GLUTEN_CHECK(
      bytes.size() == sizeof(int64_t) * 2,
      "Corrupt native streaming deduplicate state value: expected Spark UnsafeRow long value");
  GLUTEN_CHECK(
      readInt64(bytes, 0) == 0,
      "Corrupt native streaming deduplicate state value: expiresAtMicros is null");
  return readInt64(bytes, sizeof(int64_t));
}

int64_t checkedExpiresAtMicros(int64_t eventTimeMicros, int64_t delayThresholdMicros) {
  GLUTEN_CHECK(
      delayThresholdMicros >= 0,
      "Native streaming deduplicate watermark delay must be non-negative");
  GLUTEN_CHECK(
      eventTimeMicros <= std::numeric_limits<int64_t>::max() - delayThresholdMicros,
      "Native streaming deduplicate expiresAtMicros overflow");
  return eventTimeMicros + delayThresholdMicros;
}

int64_t evictExpiredState(VeloxNativeStateStore& store, int64_t watermarkMicros) {
  int64_t removedRows = 0;
  auto iterator = store.iterator();
  while (iterator->hasNext()) {
    auto row = iterator->next();
    const auto expiresAtMicros = decodeExpiresAtMicros(row.second);
    if (watermarkMicros >= expiresAtMicros) {
      store.remove(row.first);
      ++removedRows;
    }
  }
  return removedRows;
}

} // namespace

VeloxNativeStreamingDeduplicate::VeloxNativeStreamingDeduplicate(VeloxNativeStateStore& store)
    : store_(store) {}

NativeStreamingDeduplicateResult VeloxNativeStreamingDeduplicate::deduplicate(
    const std::vector<StateBytes>& keys) {
  NativeStreamingDeduplicateResult result;
  auto& metrics = result.metrics;
  metrics.numInputRows = static_cast<int64_t>(keys.size());

  for (size_t index = 0; index < keys.size(); ++index) {
    if (store_.get(keys[index]).has_value()) {
      ++metrics.numDroppedDuplicateRows;
      continue;
    }

    store_.put(keys[index], emptyDeduplicateValue());
    result.outputRowIndices.push_back(index);
    ++metrics.numOutputRows;
    ++metrics.numUpdatedStateRows;
  }

  refreshStateMetrics(metrics);
  return result;
}

NativeStreamingDeduplicateResult VeloxNativeStreamingDeduplicate::deduplicateWithinWatermark(
    const std::vector<NativeStreamingDeduplicateWatermarkInput>& rows,
    std::optional<int64_t> eventTimeWatermarkForLateEventsMicros,
    int64_t delayThresholdMicros,
    std::optional<int64_t> eventTimeWatermarkForEvictionMicros) {
  GLUTEN_CHECK(
      delayThresholdMicros >= 0,
      "Native streaming deduplicate watermark delay must be non-negative");

  NativeStreamingDeduplicateResult result;
  auto& metrics = result.metrics;
  metrics.numInputRows = static_cast<int64_t>(rows.size());

  for (size_t index = 0; index < rows.size(); ++index) {
    const auto& row = rows[index];
    if (eventTimeWatermarkForLateEventsMicros.has_value() &&
        row.eventTimeMicros <= eventTimeWatermarkForLateEventsMicros.value()) {
      ++metrics.numRowsDroppedByWatermark;
      continue;
    }

    if (store_.get(row.key).has_value()) {
      ++metrics.numDroppedDuplicateRows;
      continue;
    }

    const auto expiresAtMicros =
        checkedExpiresAtMicros(row.eventTimeMicros, delayThresholdMicros);
    store_.put(row.key, encodeExpiresAtMicros(expiresAtMicros));
    result.outputRowIndices.push_back(index);
    ++metrics.numOutputRows;
    ++metrics.numUpdatedStateRows;
  }

  if (eventTimeWatermarkForEvictionMicros.has_value()) {
    metrics.numRemovedStateRows =
        evictExpiredState(store_, eventTimeWatermarkForEvictionMicros.value());
  }

  refreshStateMetrics(metrics);
  return result;
}

void VeloxNativeStreamingDeduplicate::refreshStateMetrics(
    NativeStreamingDeduplicateMetrics& metrics) const {
  const auto stateMetrics = store_.metrics();
  metrics.numTotalStateRows = stateMetrics.numKeys;
  metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
}

} // namespace gluten
