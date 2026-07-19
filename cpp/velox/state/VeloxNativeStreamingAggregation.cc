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

#include "state/VeloxNativeStreamingAggregation.h"

#include "utils/Exception.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/DecodedVector.h"

#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace gluten {
namespace {

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
      "Corrupt native streaming aggregation state value: truncated int64");

  uint64_t raw = 0;
  for (size_t i = 0; i < sizeof(int64_t); ++i) {
    raw |= static_cast<uint64_t>(bytes[start + i]) << (i * 8);
  }

  int64_t value = 0;
  std::memcpy(&value, &raw, sizeof(value));
  return value;
}

StateBytes encodeUnsafeRowLong(int64_t value) {
  StateBytes bytes;
  bytes.reserve(sizeof(int64_t) * 2);
  appendInt64(bytes, 0);
  appendInt64(bytes, value);
  return bytes;
}

int64_t decodeUnsafeRowLong(const StateBytes& bytes) {
  GLUTEN_CHECK(
      bytes.size() == sizeof(int64_t) * 2,
      "Corrupt native streaming aggregation state value: expected Spark UnsafeRow long value");
  GLUTEN_CHECK(
      readInt64(bytes, 0) == 0,
      "Corrupt native streaming aggregation state value: count is null");
  return readInt64(bytes, sizeof(int64_t));
}

int64_t addChecked(int64_t value, int64_t delta) {
  GLUTEN_CHECK(
      delta >= 0,
      "Native streaming aggregation count delta must be non-negative");
  GLUTEN_CHECK(
      value <= std::numeric_limits<int64_t>::max() - delta,
      "Native streaming aggregation count overflow");
  return value + delta;
}

int64_t addCheckedSigned(int64_t value, int64_t delta) {
  if (delta > 0) {
    GLUTEN_CHECK(
        value <= std::numeric_limits<int64_t>::max() - delta,
        "Native streaming aggregation long sum overflow");
  } else if (delta < 0) {
    GLUTEN_CHECK(
        value >= std::numeric_limits<int64_t>::min() - delta,
        "Native streaming aggregation long sum underflow");
  }
  return value + delta;
}

struct BatchDelta {
  int64_t delta{0};
  size_t updatedKeyIndex{0};
};

struct NullableInt64Key {
  bool isNull{false};
  int64_t value{0};

  bool operator==(const NullableInt64Key& other) const {
    return isNull == other.isNull && (isNull || value == other.value);
  }
};

struct NullableInt64KeyHash {
  size_t operator()(const NullableInt64Key& key) const {
    if (key.isNull) {
      return 0x9e3779b97f4a7c15ULL;
    }
    return std::hash<int64_t>{}(key.value);
  }
};

StateBytes encodeUnsafeRowNullableLong(bool isNull, int64_t value) {
  StateBytes bytes;
  bytes.reserve(sizeof(int64_t) * 2);
  appendInt64(bytes, isNull ? 1 : 0);
  appendInt64(bytes, isNull ? 0 : value);
  return bytes;
}

facebook::velox::RowVectorPtr makeKeyRowVector(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<int32_t>& keyOrdinals) {
  GLUTEN_CHECK(
      input != nullptr,
      "Native streaming aggregation columnar input batch must not be null");
  GLUTEN_CHECK(
      !keyOrdinals.empty(),
      "Native streaming aggregation columnar count requires at least one grouping key ordinal");

  const auto inputType = facebook::velox::asRowType(input->type());
  std::vector<std::string> keyNames;
  std::vector<facebook::velox::TypePtr> keyTypes;
  std::vector<facebook::velox::VectorPtr> keyChildren;
  keyNames.reserve(keyOrdinals.size());
  keyTypes.reserve(keyOrdinals.size());
  keyChildren.reserve(keyOrdinals.size());

  for (const auto ordinal : keyOrdinals) {
    GLUTEN_CHECK(
        ordinal >= 0 && ordinal < input->childrenSize(),
        "Native streaming aggregation key ordinal out of bounds: " + std::to_string(ordinal));
    keyNames.push_back(inputType->nameOf(ordinal));
    keyTypes.push_back(inputType->childAt(ordinal));
    keyChildren.push_back(input->childAt(ordinal));
  }

  return std::make_shared<facebook::velox::RowVector>(
      input->pool(),
      facebook::velox::ROW(std::move(keyNames), std::move(keyTypes)),
      nullptr,
      input->size(),
      std::move(keyChildren));
}

} // namespace

VeloxNativeStreamingAggregation::VeloxNativeStreamingAggregation(VeloxNativeStateStore& store)
    : store_(store) {}

NativeStreamingCountMetrics VeloxNativeStreamingAggregation::incrementCount(
    const std::vector<StateBytes>& keys) {
  std::vector<int64_t> deltas(keys.size(), 1);
  return incrementCount(keys, deltas);
}

NativeStreamingCountMetrics VeloxNativeStreamingAggregation::incrementCount(
    const std::vector<StateBytes>& keys,
    const std::vector<int64_t>& deltas) {
  GLUTEN_CHECK(
      keys.size() == deltas.size(),
      "Native streaming aggregation keys and count deltas must have the same length");

  NativeStreamingCountMetrics metrics;
  metrics.numInputRows = static_cast<int64_t>(keys.size());

  std::unordered_map<StateBytes, int64_t, StateBytesHash> batchDeltas;
  batchDeltas.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    const auto delta = deltas[i];
    if (delta == 0) {
      continue;
    }
    ++metrics.numUpdatedStateRows;
    auto result = batchDeltas.emplace(keys[i], 0);
    result.first->second = addChecked(result.first->second, delta);
  }

  for (const auto& [key, delta] : batchDeltas) {
    const auto existing = store_.get(key);
    const auto currentCount = existing.has_value() ? decodeUnsafeRowLong(existing.value()) : 0L;
    store_.put(key, encodeUnsafeRowLong(addChecked(currentCount, delta)));
  }

  const auto stateMetrics = store_.metrics();
  metrics.numOutputRows = static_cast<int64_t>(batchDeltas.size());
  metrics.numTotalStateRows = stateMetrics.numKeys;
  metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
  return metrics;
}

NativeStreamingCountMetrics VeloxNativeStreamingAggregation::incrementCount(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<int32_t>& keyOrdinals) {
  return incrementCountAndReturnKeys(input, keyOrdinals).metrics;
}

NativeStreamingCountUpdateResult VeloxNativeStreamingAggregation::incrementCountAndReturnKeys(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<int32_t>& keyOrdinals) {
  auto keyInput = makeKeyRowVector(input, keyOrdinals);
  facebook::velox::row::UnsafeRowFast unsafeRow(keyInput);

  NativeStreamingCountUpdateResult result;
  result.metrics.numInputRows = static_cast<int64_t>(keyInput->size());

  std::unordered_map<StateBytes, BatchDelta, StateBytesHash> batchDeltas;
  batchDeltas.reserve(static_cast<size_t>(keyInput->size()));
  for (facebook::velox::vector_size_t row = 0; row < keyInput->size(); ++row) {
    const auto rowSize = unsafeRow.rowSize(row);
    StateBytes key(static_cast<size_t>(rowSize));
    unsafeRow.serialize(row, reinterpret_cast<char*>(key.data()));

    ++result.metrics.numUpdatedStateRows;
    auto [it, inserted] = batchDeltas.emplace(std::move(key), BatchDelta{});
    if (inserted) {
      it->second.updatedKeyIndex = result.updatedKeys.size();
      result.updatedKeys.push_back(it->first);
    }
    it->second.delta = addChecked(it->second.delta, 1);
  }

  result.updatedValues.resize(result.updatedKeys.size());
  for (const auto& [key, update] : batchDeltas) {
    const auto existing = store_.get(key);
    const auto currentCount = existing.has_value() ? decodeUnsafeRowLong(existing.value()) : 0L;
    auto value = encodeUnsafeRowLong(addChecked(currentCount, update.delta));
    result.updatedValues[update.updatedKeyIndex] = value;
    store_.put(key, std::move(value));
  }

  const auto stateMetrics = store_.metrics();
  result.metrics.numOutputRows = static_cast<int64_t>(batchDeltas.size());
  result.metrics.numTotalStateRows = stateMetrics.numKeys;
  result.metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
  return result;
}

NativeStreamingCountUpdateResult
VeloxNativeStreamingAggregation::incrementCountSingleInt64KeyAndReturnKeys(
    const facebook::velox::RowVectorPtr& input,
    int32_t keyOrdinal) {
  GLUTEN_CHECK(
      input != nullptr,
      "Native streaming typed count columnar input batch must not be null");
  GLUTEN_CHECK(
      keyOrdinal >= 0 && keyOrdinal < input->childrenSize(),
      "Native streaming typed count key ordinal out of bounds: " + std::to_string(keyOrdinal));

  auto keyVector = input->childAt(keyOrdinal)->loadedVector();
  GLUTEN_CHECK(
      keyVector->typeKind() == facebook::velox::TypeKind::BIGINT,
      "Native streaming typed count requires a BIGINT grouping key");
  facebook::velox::DecodedVector decodedKeys(*keyVector);

  NativeStreamingCountUpdateResult result;
  result.metrics.numInputRows = static_cast<int64_t>(input->size());

  std::unordered_map<NullableInt64Key, BatchDelta, NullableInt64KeyHash> batchDeltas;
  batchDeltas.reserve(static_cast<size_t>(input->size()));
  for (facebook::velox::vector_size_t row = 0; row < input->size(); ++row) {
    NullableInt64Key key;
    key.isNull = decodedKeys.isNullAt(row);
    if (!key.isNull) {
      key.value = decodedKeys.valueAt<int64_t>(row);
    }

    ++result.metrics.numUpdatedStateRows;
    auto [it, inserted] = batchDeltas.emplace(key, BatchDelta{});
    if (inserted) {
      it->second.updatedKeyIndex = result.updatedKeys.size();
      result.updatedKeys.push_back(encodeUnsafeRowNullableLong(key.isNull, key.value));
    }
    it->second.delta = addChecked(it->second.delta, 1);
  }

  result.updatedValues.resize(result.updatedKeys.size());
  for (const auto& [batchKey, update] : batchDeltas) {
    // Typed fast path: see incrementLongSumSingleInt64KeyAndReturnKeys.
    const TypedInt64Key typedKey{batchKey.isNull, batchKey.value};
    const auto existing = store_.getTypedInt64(typedKey);
    const auto currentCount = existing.value_or(0L);
    const auto newCount = addChecked(currentCount, update.delta);
    result.updatedValues[update.updatedKeyIndex] = encodeUnsafeRowLong(newCount);
    store_.putTypedInt64(typedKey, newCount);
  }

  const auto stateMetrics = store_.metrics();
  result.metrics.numOutputRows = static_cast<int64_t>(batchDeltas.size());
  result.metrics.numTotalStateRows = stateMetrics.numKeys;
  result.metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
  return result;
}

NativeStreamingCountMetrics VeloxNativeStreamingAggregation::incrementCountSingleInt64Key(
    const facebook::velox::RowVectorPtr& input,
    int32_t keyOrdinal) {
  GLUTEN_CHECK(
      input != nullptr,
      "Native streaming typed count columnar input batch must not be null");
  GLUTEN_CHECK(
      keyOrdinal >= 0 && keyOrdinal < input->childrenSize(),
      "Native streaming typed count key ordinal out of bounds: " + std::to_string(keyOrdinal));

  auto keyVector = input->childAt(keyOrdinal)->loadedVector();
  GLUTEN_CHECK(
      keyVector->typeKind() == facebook::velox::TypeKind::BIGINT,
      "Native streaming typed count requires a BIGINT grouping key");
  facebook::velox::DecodedVector decodedKeys(*keyVector);

  NativeStreamingCountMetrics metrics;
  metrics.numInputRows = static_cast<int64_t>(input->size());

  std::unordered_map<NullableInt64Key, int64_t, NullableInt64KeyHash> batchDeltas;
  batchDeltas.reserve(static_cast<size_t>(input->size()));
  for (facebook::velox::vector_size_t row = 0; row < input->size(); ++row) {
    NullableInt64Key key;
    key.isNull = decodedKeys.isNullAt(row);
    if (!key.isNull) {
      key.value = decodedKeys.valueAt<int64_t>(row);
    }

    ++metrics.numUpdatedStateRows;
    auto result = batchDeltas.emplace(key, 0);
    result.first->second = addChecked(result.first->second, 1);
  }

  for (const auto& [typedKey, delta] : batchDeltas) {
    auto key = encodeUnsafeRowNullableLong(typedKey.isNull, typedKey.value);
    const auto existing = store_.get(key);
    const auto currentCount = existing.has_value() ? decodeUnsafeRowLong(existing.value()) : 0L;
    store_.put(key, encodeUnsafeRowLong(addChecked(currentCount, delta)));
  }

  const auto stateMetrics = store_.metrics();
  metrics.numOutputRows = static_cast<int64_t>(batchDeltas.size());
  metrics.numTotalStateRows = stateMetrics.numKeys;
  metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
  return metrics;
}

NativeStreamingCountMetrics VeloxNativeStreamingAggregation::incrementLongSum(
    const std::vector<StateBytes>& keys,
    const std::vector<int64_t>& deltas) {
  GLUTEN_CHECK(
      keys.size() == deltas.size(),
      "Native streaming aggregation keys and long sum deltas must have the same length");

  NativeStreamingCountMetrics metrics;
  metrics.numInputRows = static_cast<int64_t>(keys.size());

  std::unordered_map<StateBytes, int64_t, StateBytesHash> batchDeltas;
  batchDeltas.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    const auto delta = deltas[i];
    ++metrics.numUpdatedStateRows;
    auto result = batchDeltas.emplace(keys[i], 0);
    result.first->second = addCheckedSigned(result.first->second, delta);
  }

  for (const auto& [key, delta] : batchDeltas) {
    const auto existing = store_.get(key);
    const auto currentValue = existing.has_value() ? decodeUnsafeRowLong(existing.value()) : 0L;
    store_.put(key, encodeUnsafeRowLong(addCheckedSigned(currentValue, delta)));
  }

  const auto stateMetrics = store_.metrics();
  metrics.numOutputRows = static_cast<int64_t>(batchDeltas.size());
  metrics.numTotalStateRows = stateMetrics.numKeys;
  metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
  return metrics;
}

NativeStreamingCountMetrics VeloxNativeStreamingAggregation::incrementLongSum(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<int32_t>& keyOrdinals,
    int32_t valueOrdinal) {
  return incrementLongSumAndReturnKeys(input, keyOrdinals, valueOrdinal).metrics;
}

NativeStreamingCountUpdateResult VeloxNativeStreamingAggregation::incrementLongSumAndReturnKeys(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<int32_t>& keyOrdinals,
    int32_t valueOrdinal) {
  auto keyInput = makeKeyRowVector(input, keyOrdinals);
  GLUTEN_CHECK(
      valueOrdinal >= 0 && valueOrdinal < input->childrenSize(),
      "Native streaming aggregation long sum value ordinal out of bounds: " +
          std::to_string(valueOrdinal));

  auto valueVector = input->childAt(valueOrdinal)->loadedVector();
  GLUTEN_CHECK(
      valueVector->typeKind() == facebook::velox::TypeKind::BIGINT,
      "Native streaming aggregation columnar long sum requires BIGINT value input");
  facebook::velox::DecodedVector decodedValues(*valueVector);
  facebook::velox::row::UnsafeRowFast unsafeRow(keyInput);

  NativeStreamingCountUpdateResult result;
  result.metrics.numInputRows = static_cast<int64_t>(keyInput->size());

  std::unordered_map<StateBytes, BatchDelta, StateBytesHash> batchDeltas;
  batchDeltas.reserve(static_cast<size_t>(keyInput->size()));
  for (facebook::velox::vector_size_t row = 0; row < keyInput->size(); ++row) {
    GLUTEN_CHECK(
        !decodedValues.isNullAt(row),
        "Velox native long sum aggregation supports only non-null long partial sums");
    const auto delta = decodedValues.valueAt<int64_t>(row);

    const auto rowSize = unsafeRow.rowSize(row);
    StateBytes key(static_cast<size_t>(rowSize));
    unsafeRow.serialize(row, reinterpret_cast<char*>(key.data()));

    ++result.metrics.numUpdatedStateRows;
    auto [it, inserted] = batchDeltas.emplace(std::move(key), BatchDelta{});
    if (inserted) {
      it->second.updatedKeyIndex = result.updatedKeys.size();
      result.updatedKeys.push_back(it->first);
    }
    it->second.delta = addCheckedSigned(it->second.delta, delta);
  }

  result.updatedValues.resize(result.updatedKeys.size());
  for (const auto& [key, update] : batchDeltas) {
    const auto existing = store_.get(key);
    const auto currentValue = existing.has_value() ? decodeUnsafeRowLong(existing.value()) : 0L;
    auto value = encodeUnsafeRowLong(addCheckedSigned(currentValue, update.delta));
    result.updatedValues[update.updatedKeyIndex] = value;
    store_.put(key, std::move(value));
  }

  const auto stateMetrics = store_.metrics();
  result.metrics.numOutputRows = static_cast<int64_t>(batchDeltas.size());
  result.metrics.numTotalStateRows = stateMetrics.numKeys;
  result.metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
  return result;
}

NativeStreamingCountUpdateResult
VeloxNativeStreamingAggregation::incrementLongSumSingleInt64KeyAndReturnKeys(
    const facebook::velox::RowVectorPtr& input,
    int32_t keyOrdinal,
    int32_t valueOrdinal) {
  GLUTEN_CHECK(
      input != nullptr,
      "Native streaming typed long sum columnar input batch must not be null");
  GLUTEN_CHECK(
      keyOrdinal >= 0 && keyOrdinal < input->childrenSize(),
      "Native streaming typed long sum key ordinal out of bounds: " +
          std::to_string(keyOrdinal));
  GLUTEN_CHECK(
      valueOrdinal >= 0 && valueOrdinal < input->childrenSize(),
      "Native streaming typed long sum value ordinal out of bounds: " +
          std::to_string(valueOrdinal));

  auto keyVector = input->childAt(keyOrdinal)->loadedVector();
  auto valueVector = input->childAt(valueOrdinal)->loadedVector();
  GLUTEN_CHECK(
      keyVector->typeKind() == facebook::velox::TypeKind::BIGINT,
      "Native streaming typed long sum requires a BIGINT grouping key");
  GLUTEN_CHECK(
      valueVector->typeKind() == facebook::velox::TypeKind::BIGINT,
      "Native streaming typed long sum requires a BIGINT value input");

  facebook::velox::DecodedVector decodedKeys(*keyVector);
  facebook::velox::DecodedVector decodedValues(*valueVector);

  NativeStreamingCountUpdateResult result;
  result.metrics.numInputRows = static_cast<int64_t>(input->size());

  std::unordered_map<NullableInt64Key, BatchDelta, NullableInt64KeyHash> batchDeltas;
  batchDeltas.reserve(static_cast<size_t>(input->size()));
  for (facebook::velox::vector_size_t row = 0; row < input->size(); ++row) {
    GLUTEN_CHECK(
        !decodedValues.isNullAt(row),
        "Velox native typed long sum aggregation supports only non-null long partial sums");
    const auto delta = decodedValues.valueAt<int64_t>(row);

    NullableInt64Key key;
    key.isNull = decodedKeys.isNullAt(row);
    if (!key.isNull) {
      key.value = decodedKeys.valueAt<int64_t>(row);
    }

    ++result.metrics.numUpdatedStateRows;
    auto [it, inserted] = batchDeltas.emplace(key, BatchDelta{});
    if (inserted) {
      it->second.updatedKeyIndex = result.updatedKeys.size();
      result.updatedKeys.push_back(encodeUnsafeRowNullableLong(key.isNull, key.value));
    }
    it->second.delta = addCheckedSigned(it->second.delta, delta);
  }

  result.updatedValues.resize(result.updatedKeys.size());
  for (const auto& [batchKey, update] : batchDeltas) {
    // Typed fast path: probe/update the int64-keyed resident map directly,
    // avoiding a 16-byte UnsafeRow hash + allocation per key. The encoded
    // UnsafeRow value bytes are still returned for Spark Update-mode output.
    const TypedInt64Key typedKey{batchKey.isNull, batchKey.value};
    const auto existing = store_.getTypedInt64(typedKey);
    const auto currentValue = existing.value_or(0L);
    const auto newValue = addCheckedSigned(currentValue, update.delta);
    result.updatedValues[update.updatedKeyIndex] = encodeUnsafeRowLong(newValue);
    store_.putTypedInt64(typedKey, newValue);
  }

  const auto stateMetrics = store_.metrics();
  result.metrics.numOutputRows = static_cast<int64_t>(batchDeltas.size());
  result.metrics.numTotalStateRows = stateMetrics.numKeys;
  result.metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
  return result;
}

namespace {

// Shared typed update body for the single-non-null-BIGINT-key shape that returns primitive int64
// keys/sums. addDelta is addChecked (count) or addCheckedSigned (long sum). requireNonNullValue is
// true for the long-sum path (Spark SUM ignores nulls but the group is still observed; the caller
// guarantees non-null values upstream) and the count path always has a unit delta.
NativeStreamingTypedInt64UpdateResult incrementSingleInt64KeyTypedRowsImpl(
    VeloxNativeStateStore& store,
    const facebook::velox::RowVectorPtr& input,
    int32_t keyOrdinal,
    const std::optional<int32_t>& valueOrdinal,
    int64_t (*addDelta)(int64_t, int64_t)) {
  GLUTEN_CHECK(
      input != nullptr,
      "Native streaming typed columnar input batch must not be null");
  GLUTEN_CHECK(
      keyOrdinal >= 0 && keyOrdinal < input->childrenSize(),
      "Native streaming typed key ordinal out of bounds: " + std::to_string(keyOrdinal));

  auto keyVector = input->childAt(keyOrdinal)->loadedVector();
  GLUTEN_CHECK(
      keyVector->typeKind() == facebook::velox::TypeKind::BIGINT,
      "Native streaming typed path requires a BIGINT grouping key");
  facebook::velox::DecodedVector decodedKeys(*keyVector);

  std::unique_ptr<facebook::velox::DecodedVector> decodedValues;
  if (valueOrdinal.has_value()) {
    GLUTEN_CHECK(
        *valueOrdinal >= 0 && *valueOrdinal < input->childrenSize(),
        "Native streaming typed value ordinal out of bounds: " + std::to_string(*valueOrdinal));
    auto valueVector = input->childAt(*valueOrdinal)->loadedVector();
    GLUTEN_CHECK(
        valueVector->typeKind() == facebook::velox::TypeKind::BIGINT,
        "Native streaming typed path requires a BIGINT value input");
    decodedValues = std::make_unique<facebook::velox::DecodedVector>(*valueVector);
  }

  NativeStreamingTypedInt64UpdateResult result;
  result.metrics.numInputRows = static_cast<int64_t>(input->size());

  // Fold this batch's deltas into per-key running deltas, preserving first-observation order in
  // the parallel keys/sums vectors so Update-mode output ordering matches the byte[] path. The
  // single SQL NULL key is tracked in a dedicated slot (UnsafeRow null keys are valid Spark
  // grouping keys but cannot live in the int64-keyed batch map).
  std::unordered_map<int64_t, size_t> nonNullIndex;
  nonNullIndex.reserve(static_cast<size_t>(input->size()));
  bool nullSeen = false;
  int64_t nullDelta = 0;

  for (facebook::velox::vector_size_t row = 0; row < input->size(); ++row) {
    int64_t delta = 1;
    if (decodedValues) {
      GLUTEN_CHECK(
          !decodedValues->isNullAt(row),
          "Velox native typed aggregation supports only non-null long partial values");
      delta = decodedValues->valueAt<int64_t>(row);
    }
    ++result.metrics.numUpdatedStateRows;

    if (decodedKeys.isNullAt(row)) {
      nullDelta = addDelta(nullDelta, delta);
      nullSeen = true;
      continue;
    }
    const auto key = decodedKeys.valueAt<int64_t>(row);
    auto [it, inserted] = nonNullIndex.emplace(key, result.keys.size());
    if (inserted) {
      result.keys.push_back(key);
      result.sums.push_back(0);
    }
    result.sums[it->second] = addDelta(result.sums[it->second], delta);
  }

  // Apply the batch deltas to the resident typed map and replace the per-key accumulated delta
  // with the post-update running total for Update-mode output.
  for (size_t i = 0; i < result.keys.size(); ++i) {
    const TypedInt64Key typedKey{false, result.keys[i]};
    const auto existing = store.getTypedInt64(typedKey);
    const auto newValue = addDelta(existing.value_or(0L), result.sums[i]);
    result.sums[i] = newValue;
    store.putTypedInt64(typedKey, newValue);
  }
  if (nullSeen) {
    const TypedInt64Key typedKey{true, 0};
    const auto existing = store.getTypedInt64(typedKey);
    const auto newValue = addDelta(existing.value_or(0L), nullDelta);
    result.hasNullKey = true;
    result.nullKeySum = newValue;
    store.putTypedInt64(typedKey, newValue);
  }

  const auto stateMetrics = store.metrics();
  result.metrics.numOutputRows =
      static_cast<int64_t>(result.keys.size()) + (nullSeen ? 1 : 0);
  result.metrics.numTotalStateRows = stateMetrics.numKeys;
  result.metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
  return result;
}

} // namespace

NativeStreamingTypedInt64UpdateResult
VeloxNativeStreamingAggregation::incrementLongSumSingleInt64KeyAndReturnTypedRows(
    const facebook::velox::RowVectorPtr& input,
    int32_t keyOrdinal,
    int32_t valueOrdinal) {
  return incrementSingleInt64KeyTypedRowsImpl(
      store_, input, keyOrdinal, std::make_optional(valueOrdinal), &addCheckedSigned);
}

NativeStreamingTypedInt64UpdateResult
VeloxNativeStreamingAggregation::incrementCountSingleInt64KeyAndReturnTypedRows(
    const facebook::velox::RowVectorPtr& input,
    int32_t keyOrdinal) {
  return incrementSingleInt64KeyTypedRowsImpl(
      store_, input, keyOrdinal, std::nullopt, &addChecked);
}

NativeStreamingCountEvictionMetrics VeloxNativeStreamingAggregation::evictKeys(
    const std::vector<StateBytes>& keys) {
  NativeStreamingCountEvictionMetrics metrics;
  metrics.numInputRows = static_cast<int64_t>(keys.size());

  std::unordered_set<StateBytes, StateBytesHash> distinctKeys;
  distinctKeys.reserve(keys.size());
  for (const auto& key : keys) {
    distinctKeys.emplace(key);
  }

  for (const auto& key : distinctKeys) {
    if (store_.get(key).has_value()) {
      store_.remove(key);
      ++metrics.numRemovedStateRows;
    }
  }

  const auto stateMetrics = store_.metrics();
  metrics.numTotalStateRows = stateMetrics.numKeys;
  metrics.stateMemoryBytes = stateMetrics.memoryUsedBytes;
  return metrics;
}

} // namespace gluten
