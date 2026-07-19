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

#include <jni.h>

#include "jni/JniCommon.h"
#include "jni/JniError.h"
#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "state/VeloxNativeStateStore.h"
#include "state/VeloxNativeStreamingAggregation.h"
#include "state/VeloxNativeStreamingDeduplicate.h"
#include "utils/Exception.h"
#include "utils/ObjectStore.h"

#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

constexpr uint8_t kEmptySnapshotSentinel = 0;

template <typename T>
class NativeHandleStore {
 public:
  jlong save(std::shared_ptr<T> value) {
    const std::lock_guard<std::mutex> lock(mutex_);
    const jlong handle = nextHandle_++;
    handles_.emplace(handle, std::move(value));
    return handle;
  }

  std::shared_ptr<T> get(jlong handle) {
    const std::lock_guard<std::mutex> lock(mutex_);
    const auto it = handles_.find(handle);
    GLUTEN_CHECK(
        it != handles_.end(), "Unknown Velox native StateStore handle: " + std::to_string(handle));
    return it->second;
  }

  void release(jlong handle) {
    const std::lock_guard<std::mutex> lock(mutex_);
    handles_.erase(handle);
  }

  jlong size() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<jlong>(handles_.size());
  }

 private:
  mutable std::mutex mutex_;
  jlong nextHandle_{1};
  std::unordered_map<jlong, std::shared_ptr<T>> handles_;
};

NativeHandleStore<gluten::VeloxNativeStateStore>& storeHandles() {
  static NativeHandleStore<gluten::VeloxNativeStateStore> handles;
  return handles;
}

NativeHandleStore<gluten::VeloxNativeStateStoreIterator>& iteratorHandles() {
  static NativeHandleStore<gluten::VeloxNativeStateStoreIterator> handles;
  return handles;
}

gluten::StateBytes toStateBytes(JNIEnv* env, jbyteArray array) {
  if (array == nullptr) {
    return {};
  }
  const jsize length = env->GetArrayLength(array);
  gluten::StateBytes out(static_cast<size_t>(length));
  if (length > 0) {
    env->GetByteArrayRegion(array, 0, length, reinterpret_cast<jbyte*>(out.data()));
  }
  return out;
}

std::vector<gluten::StateBytes> toStateBytesVector(JNIEnv* env, jobjectArray arrays) {
  GLUTEN_CHECK(arrays != nullptr, "Velox native StateStore byte-array vector must not be null");
  const jsize length = env->GetArrayLength(arrays);
  std::vector<gluten::StateBytes> out;
  out.reserve(static_cast<size_t>(length));
  for (jsize index = 0; index < length; ++index) {
    auto element = static_cast<jbyteArray>(env->GetObjectArrayElement(arrays, index));
    GLUTEN_CHECK(
        element != nullptr,
        "Velox native StateStore byte-array vector contains null element at index " +
            std::to_string(index));
    out.emplace_back(toStateBytes(env, element));
    env->DeleteLocalRef(element);
  }
  return out;
}

std::vector<int64_t> toInt64Vector(JNIEnv* env, jlongArray array) {
  GLUTEN_CHECK(array != nullptr, "Velox native StateStore long vector must not be null");
  const jsize length = env->GetArrayLength(array);
  std::vector<jlong> raw(static_cast<size_t>(length));
  if (length > 0) {
    env->GetLongArrayRegion(array, 0, length, raw.data());
  }

  std::vector<int64_t> out;
  out.reserve(raw.size());
  for (const auto value : raw) {
    out.push_back(static_cast<int64_t>(value));
  }
  return out;
}

std::vector<int32_t> toInt32Vector(JNIEnv* env, jintArray array) {
  GLUTEN_CHECK(array != nullptr, "Velox native StateStore int vector must not be null");
  const jsize length = env->GetArrayLength(array);
  std::vector<jint> raw(static_cast<size_t>(length));
  if (length > 0) {
    env->GetIntArrayRegion(array, 0, length, raw.data());
  }

  std::vector<int32_t> out;
  out.reserve(raw.size());
  for (const auto value : raw) {
    out.push_back(static_cast<int32_t>(value));
  }
  return out;
}

size_t unsafeRowBitsetWidth(int32_t fields) {
  GLUTEN_CHECK(fields >= 0, "UnsafeRow field count must be non-negative");
  return static_cast<size_t>((fields + 63) / 64) * sizeof(uint64_t);
}

bool unsafeRowIsNullAt(const gluten::StateBytes& row, int32_t field) {
  const auto wordOffset = static_cast<size_t>(field / 64) * sizeof(uint64_t);
  uint64_t word = 0;
  std::memcpy(&word, row.data() + wordOffset, sizeof(uint64_t));
  return (word & (uint64_t{1} << (field % 64))) != 0;
}

void unsafeRowSetNullAt(gluten::StateBytes& row, int32_t field) {
  const auto wordOffset = static_cast<size_t>(field / 64) * sizeof(uint64_t);
  uint64_t word = 0;
  std::memcpy(&word, row.data() + wordOffset, sizeof(uint64_t));
  word |= uint64_t{1} << (field % 64);
  std::memcpy(row.data() + wordOffset, &word, sizeof(uint64_t));
}

gluten::StateBytes joinFixedWidthUnsafeRows(
    const gluten::StateBytes& key,
    int32_t keyFields,
    const gluten::StateBytes& value,
    int32_t valueFields) {
  GLUTEN_CHECK(keyFields > 0, "Velox native fixed-width streaming output requires key fields");
  GLUTEN_CHECK(valueFields > 0, "Velox native fixed-width streaming output requires value fields");

  const auto keyBitsetWidth = unsafeRowBitsetWidth(keyFields);
  const auto valueBitsetWidth = unsafeRowBitsetWidth(valueFields);
  const auto outputFields = keyFields + valueFields;
  const auto outputBitsetWidth = unsafeRowBitsetWidth(outputFields);
  const auto keyFixedSize = keyBitsetWidth + static_cast<size_t>(keyFields) * sizeof(uint64_t);
  const auto valueFixedSize = valueBitsetWidth + static_cast<size_t>(valueFields) * sizeof(uint64_t);
  const auto outputFixedSize =
      outputBitsetWidth + static_cast<size_t>(outputFields) * sizeof(uint64_t);

  GLUTEN_CHECK(
      key.size() == keyFixedSize,
      "Velox native fixed-width streaming output requires fixed-width UnsafeRow keys");
  GLUTEN_CHECK(
      value.size() == valueFixedSize,
      "Velox native fixed-width streaming output requires fixed-width UnsafeRow values");

  gluten::StateBytes output(outputFixedSize, 0);
  for (int32_t field = 0; field < keyFields; ++field) {
    if (unsafeRowIsNullAt(key, field)) {
      unsafeRowSetNullAt(output, field);
    }
    std::memcpy(
        output.data() + outputBitsetWidth + static_cast<size_t>(field) * sizeof(uint64_t),
        key.data() + keyBitsetWidth + static_cast<size_t>(field) * sizeof(uint64_t),
        sizeof(uint64_t));
  }
  for (int32_t field = 0; field < valueFields; ++field) {
    const auto outputField = keyFields + field;
    if (unsafeRowIsNullAt(value, field)) {
      unsafeRowSetNullAt(output, outputField);
    }
    std::memcpy(
        output.data() + outputBitsetWidth + static_cast<size_t>(outputField) * sizeof(uint64_t),
        value.data() + valueBitsetWidth + static_cast<size_t>(field) * sizeof(uint64_t),
        sizeof(uint64_t));
  }
  return output;
}

std::vector<gluten::StateBytes> makeFixedWidthOutputRows(
    const gluten::NativeStreamingCountUpdateResult& result,
    int32_t keyFields,
    int32_t valueFields) {
  GLUTEN_CHECK(
      result.updatedKeys.size() == result.updatedValues.size(),
      "Velox native streaming fixed-width output key/value count mismatch");
  std::vector<gluten::StateBytes> rows;
  rows.reserve(result.updatedKeys.size());
  for (size_t index = 0; index < result.updatedKeys.size(); ++index) {
    rows.push_back(joinFixedWidthUnsafeRows(
        result.updatedKeys[index],
        keyFields,
        result.updatedValues[index],
        valueFields));
  }
  return rows;
}

jbyteArray toJByteArray(JNIEnv* env, const gluten::StateBytes& bytes) {
  GLUTEN_CHECK(
      bytes.size() <= static_cast<size_t>(std::numeric_limits<jsize>::max()),
      "Velox native StateStore byte array exceeds JNI length limit");
  auto array = env->NewByteArray(static_cast<jsize>(bytes.size()));
  if (array == nullptr) {
    throw gluten::GlutenException("Failed to allocate JVM byte array for Velox native StateStore");
  }
  if (!bytes.empty()) {
    env->SetByteArrayRegion(
        array, 0, static_cast<jsize>(bytes.size()), reinterpret_cast<const jbyte*>(bytes.data()));
  }
  return array;
}

jobjectArray toJByteArrayArray(JNIEnv* env, const std::vector<gluten::StateBytes>& values) {
  GLUTEN_CHECK(
      values.size() <= static_cast<size_t>(std::numeric_limits<jsize>::max()),
      "Velox native StateStore byte-array result exceeds JNI length limit");
  auto byteArrayClass = env->FindClass("[B");
  auto result = env->NewObjectArray(static_cast<jsize>(values.size()), byteArrayClass, nullptr);
  if (result == nullptr) {
    throw gluten::GlutenException(
        "Failed to allocate JVM byte-array vector for Velox native StateStore");
  }
  for (size_t index = 0; index < values.size(); ++index) {
    auto element = toJByteArray(env, values[index]);
    env->SetObjectArrayElement(result, static_cast<jsize>(index), element);
    env->DeleteLocalRef(element);
  }
  return result;
}

jlongArray toJLongArray(
    JNIEnv* env,
    const gluten::NativeStreamingDeduplicateResult& result) {
  constexpr size_t kHeaderSize = 1;
  constexpr size_t kMetricsSize = 8;
  const auto outputCount = result.outputRowIndices.size();
  const auto totalSize = kHeaderSize + outputCount + kMetricsSize;
  GLUTEN_CHECK(
      totalSize <= static_cast<size_t>(std::numeric_limits<jsize>::max()),
      "Velox native streaming deduplicate result exceeds JNI array limit");

  std::vector<jlong> values;
  values.reserve(totalSize);
  values.push_back(static_cast<jlong>(outputCount));
  for (const auto index : result.outputRowIndices) {
    GLUTEN_CHECK(
        index <= static_cast<size_t>(std::numeric_limits<jlong>::max()),
        "Velox native streaming deduplicate output index exceeds JNI long limit");
    values.push_back(static_cast<jlong>(index));
  }

  const auto& metrics = result.metrics;
  values.push_back(static_cast<jlong>(metrics.numInputRows));
  values.push_back(static_cast<jlong>(metrics.numOutputRows));
  values.push_back(static_cast<jlong>(metrics.numUpdatedStateRows));
  values.push_back(static_cast<jlong>(metrics.numDroppedDuplicateRows));
  values.push_back(static_cast<jlong>(metrics.numRowsDroppedByWatermark));
  values.push_back(static_cast<jlong>(metrics.numRemovedStateRows));
  values.push_back(static_cast<jlong>(metrics.numTotalStateRows));
  values.push_back(static_cast<jlong>(metrics.stateMemoryBytes));

  auto array = env->NewLongArray(static_cast<jsize>(values.size()));
  if (array == nullptr) {
    throw gluten::GlutenException(
        "Failed to allocate JVM long array for Velox native streaming deduplicate result");
  }
  env->SetLongArrayRegion(array, 0, static_cast<jsize>(values.size()), values.data());
  return array;
}

jlongArray toJLongArray(JNIEnv* env, const gluten::NativeStreamingCountMetrics& metrics) {
  constexpr size_t kMetricsSize = 5;
  std::vector<jlong> values;
  values.reserve(kMetricsSize);
  values.push_back(static_cast<jlong>(metrics.numInputRows));
  values.push_back(static_cast<jlong>(metrics.numOutputRows));
  values.push_back(static_cast<jlong>(metrics.numUpdatedStateRows));
  values.push_back(static_cast<jlong>(metrics.numTotalStateRows));
  values.push_back(static_cast<jlong>(metrics.stateMemoryBytes));

  auto array = env->NewLongArray(static_cast<jsize>(values.size()));
  if (array == nullptr) {
    throw gluten::GlutenException(
        "Failed to allocate JVM long array for Velox native streaming aggregation result");
  }
  env->SetLongArrayRegion(array, 0, static_cast<jsize>(values.size()), values.data());
  return array;
}

// Packs a primitive-typed Update result into a single jlongArray:
//   [numInputRows, numOutputRows, numUpdatedStateRows, numTotalStateRows, stateMemoryBytes,
//    numNonNullKeys, hasNullKey(0/1), nullKeySum, key_0, sum_0, key_1, sum_1, ...]
// No per-key byte[] is allocated; the JVM decodes the longs and builds output UnsafeRows directly.
jlongArray toJLongArray(
    JNIEnv* env,
    const gluten::NativeStreamingTypedInt64UpdateResult& result) {
  GLUTEN_CHECK(
      result.keys.size() == result.sums.size(),
      "Velox native typed streaming update key/sum count mismatch");
  constexpr size_t kHeaderSize = 8;
  const auto numNonNull = result.keys.size();
  const auto totalSize = kHeaderSize + numNonNull * 2;
  GLUTEN_CHECK(
      totalSize <= static_cast<size_t>(std::numeric_limits<jsize>::max()),
      "Velox native typed streaming update result exceeds JNI array limit");

  std::vector<jlong> values;
  values.reserve(totalSize);
  const auto& metrics = result.metrics;
  values.push_back(static_cast<jlong>(metrics.numInputRows));
  values.push_back(static_cast<jlong>(metrics.numOutputRows));
  values.push_back(static_cast<jlong>(metrics.numUpdatedStateRows));
  values.push_back(static_cast<jlong>(metrics.numTotalStateRows));
  values.push_back(static_cast<jlong>(metrics.stateMemoryBytes));
  values.push_back(static_cast<jlong>(numNonNull));
  values.push_back(static_cast<jlong>(result.hasNullKey ? 1 : 0));
  values.push_back(static_cast<jlong>(result.nullKeySum));
  for (size_t i = 0; i < numNonNull; ++i) {
    values.push_back(static_cast<jlong>(result.keys[i]));
    values.push_back(static_cast<jlong>(result.sums[i]));
  }

  auto array = env->NewLongArray(static_cast<jsize>(values.size()));
  if (array == nullptr) {
    throw gluten::GlutenException(
        "Failed to allocate JVM long array for Velox native typed streaming update result");
  }
  env->SetLongArrayRegion(array, 0, static_cast<jsize>(values.size()), values.data());
  return array;
}

jobjectArray toJObjectArray(
    JNIEnv* env,
    const gluten::NativeStreamingCountUpdateResult& result) {
  auto objectClass = env->FindClass("java/lang/Object");
  auto out = env->NewObjectArray(2, objectClass, nullptr);
  if (out == nullptr) {
    throw gluten::GlutenException(
        "Failed to allocate JVM object array for Velox native streaming aggregation result");
  }

  auto metrics = toJLongArray(env, result.metrics);
  auto keys = toJByteArrayArray(env, result.updatedKeys);
  env->SetObjectArrayElement(out, 0, metrics);
  env->SetObjectArrayElement(out, 1, keys);
  env->DeleteLocalRef(metrics);
  env->DeleteLocalRef(keys);
  return out;
}

jobjectArray toJObjectArrayWithValues(
    JNIEnv* env,
    const gluten::NativeStreamingCountUpdateResult& result) {
  auto objectClass = env->FindClass("java/lang/Object");
  auto out = env->NewObjectArray(3, objectClass, nullptr);
  if (out == nullptr) {
    throw gluten::GlutenException(
        "Failed to allocate JVM object array for Velox native streaming aggregation result");
  }

  auto metrics = toJLongArray(env, result.metrics);
  auto keys = toJByteArrayArray(env, result.updatedKeys);
  auto values = toJByteArrayArray(env, result.updatedValues);
  env->SetObjectArrayElement(out, 0, metrics);
  env->SetObjectArrayElement(out, 1, keys);
  env->SetObjectArrayElement(out, 2, values);
  env->DeleteLocalRef(metrics);
  env->DeleteLocalRef(keys);
  env->DeleteLocalRef(values);
  return out;
}

jobjectArray toJObjectArrayWithFixedWidthRows(
    JNIEnv* env,
    const gluten::NativeStreamingCountUpdateResult& result,
    int32_t keyFields,
    int32_t valueFields) {
  auto objectClass = env->FindClass("java/lang/Object");
  auto out = env->NewObjectArray(3, objectClass, nullptr);
  if (out == nullptr) {
    throw gluten::GlutenException(
        "Failed to allocate JVM object array for Velox native streaming aggregation result");
  }

  auto outputRows = makeFixedWidthOutputRows(result, keyFields, valueFields);
  auto metrics = toJLongArray(env, result.metrics);
  auto keys = toJByteArrayArray(env, result.updatedKeys);
  auto rows = toJByteArrayArray(env, outputRows);
  env->SetObjectArrayElement(out, 0, metrics);
  env->SetObjectArrayElement(out, 1, keys);
  env->SetObjectArrayElement(out, 2, rows);
  env->DeleteLocalRef(metrics);
  env->DeleteLocalRef(keys);
  env->DeleteLocalRef(rows);
  return out;
}

jlongArray toJLongArray(
    JNIEnv* env,
    const gluten::NativeStreamingCountEvictionMetrics& metrics) {
  constexpr size_t kMetricsSize = 4;
  std::vector<jlong> values;
  values.reserve(kMetricsSize);
  values.push_back(static_cast<jlong>(metrics.numInputRows));
  values.push_back(static_cast<jlong>(metrics.numRemovedStateRows));
  values.push_back(static_cast<jlong>(metrics.numTotalStateRows));
  values.push_back(static_cast<jlong>(metrics.stateMemoryBytes));

  auto array = env->NewLongArray(static_cast<jsize>(values.size()));
  if (array == nullptr) {
    throw gluten::GlutenException(
        "Failed to allocate JVM long array for Velox native streaming aggregation eviction result");
  }
  env->SetLongArrayRegion(array, 0, static_cast<jsize>(values.size()), values.data());
  return array;
}

} // namespace

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeOpenStore( // NOLINT
    JNIEnv* env,
    jclass,
    jlong version,
    jbyteArray snapshot) {
  JNI_METHOD_START
  const auto snapshotBytes = toStateBytes(env, snapshot);
  const auto snapshotSize = snapshot == nullptr ? 0 : static_cast<int32_t>(snapshotBytes.size());
  const uint8_t* snapshotPtr = nullptr;
  if (snapshot != nullptr) {
    snapshotPtr = snapshotSize == 0 ? &kEmptySnapshotSentinel : snapshotBytes.data();
  }
  return storeHandles().save(std::make_shared<gluten::VeloxNativeStateStore>(version, snapshotPtr, snapshotSize));
  JNI_METHOD_END(-1L)
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeGet( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jbyteArray key) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  const auto value = store->get(toStateBytes(env, key));
  if (!value.has_value()) {
    return nullptr;
  }
  return toJByteArray(env, value.value());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativePut( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jbyteArray key,
    jbyteArray value) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  store->put(toStateBytes(env, key), toStateBytes(env, value));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeRemove( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jbyteArray key) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  store->remove(toStateBytes(env, key));
  JNI_METHOD_END()
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeCommit( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  return toJByteArray(env, store->commit());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeCommitDelta( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  return toJByteArray(env, store->commitDelta());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeApplyDelta( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jbyteArray delta) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  const auto deltaBytes = toStateBytes(env, delta);
  GLUTEN_CHECK(delta != nullptr, "Missing Velox native state delta");
  const auto deltaSize = static_cast<int32_t>(deltaBytes.size());
  const uint8_t* deltaPtr = deltaSize == 0 ? &kEmptySnapshotSentinel : deltaBytes.data();
  store->applyDelta(deltaPtr, deltaSize);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeClearDirty( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  store->clearDirty();
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeRollback( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  store->rollback();
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeSetVersion( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong version) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  GLUTEN_CHECK(version >= 0, "Velox native state store requires a non-negative version");
  store->setVersion(static_cast<int64_t>(version));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeCloseStore( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle) {
  JNI_METHOD_START
  storeHandles().release(storeHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIterator( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  return iteratorHandles().save(store->iterator());
  JNI_METHOD_END(-1L)
}

JNIEXPORT jboolean JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIteratorHasNext( // NOLINT
    JNIEnv* env,
    jclass,
    jlong iteratorHandle) {
  JNI_METHOD_START
  auto iterator = iteratorHandles().get(iteratorHandle);
  return static_cast<jboolean>(iterator->hasNext());
  JNI_METHOD_END(false)
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIteratorNext( // NOLINT
    JNIEnv* env,
    jclass,
    jlong iteratorHandle) {
  JNI_METHOD_START
  auto iterator = iteratorHandles().get(iteratorHandle);
  auto pair = iterator->next();
  auto byteArrayClass = env->FindClass("[B");
  auto result = env->NewObjectArray(2, byteArrayClass, nullptr);
  env->SetObjectArrayElement(result, 0, toJByteArray(env, pair.first));
  env->SetObjectArrayElement(result, 1, toJByteArray(env, pair.second));
  return result;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeCloseIterator( // NOLINT
    JNIEnv* env,
    jclass,
    jlong iteratorHandle) {
  JNI_METHOD_START
  iteratorHandles().release(iteratorHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeMetrics( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  const auto metrics = store->metrics();
  auto result = env->NewLongArray(2);
  jlong values[] = {metrics.numKeys, metrics.memoryUsedBytes};
  env->SetLongArrayRegion(result, 0, 2, values);
  return result;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementCount( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jobjectArray keys) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJLongArray(env, aggregation.incrementCount(toStateBytesVector(env, keys)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementCountWithDeltas( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jobjectArray keys,
    jlongArray deltas) {
  JNI_METHOD_START
  auto keyBytes = toStateBytesVector(env, keys);
  auto countDeltas = toInt64Vector(env, deltas);
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJLongArray(env, aggregation.incrementCount(keyBytes, countDeltas));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementCountFromColumnarBatch( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jintArray keyOrdinals) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming columnar count requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJLongArray(
      env,
      aggregation.incrementCount(veloxBatch->getRowVector(), toInt32Vector(env, keyOrdinals)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementCountFromColumnarBatchWithKeys( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jintArray keyOrdinals) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming columnar count requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJObjectArray(
      env,
      aggregation.incrementCountAndReturnKeys(
          veloxBatch->getRowVector(),
          toInt32Vector(env, keyOrdinals)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementCountFromColumnarBatchWithKeyValues( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jintArray keyOrdinals) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming columnar count requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJObjectArrayWithValues(
      env,
      aggregation.incrementCountAndReturnKeys(
          veloxBatch->getRowVector(),
          toInt32Vector(env, keyOrdinals)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementCountSingleInt64KeyFromColumnarBatchWithFixedWidthRows( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jint keyOrdinal,
    jint valueFieldCount) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming typed columnar count requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJObjectArrayWithFixedWidthRows(
      env,
      aggregation.incrementCountSingleInt64KeyAndReturnKeys(
          veloxBatch->getRowVector(), static_cast<int32_t>(keyOrdinal)),
      1,
      static_cast<int32_t>(valueFieldCount));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementCountSingleInt64KeyFromColumnarBatch( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jint keyOrdinal) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming typed columnar count requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJLongArray(
      env,
      aggregation.incrementCountSingleInt64Key(
          veloxBatch->getRowVector(), static_cast<int32_t>(keyOrdinal)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementLongSumWithDeltas( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jobjectArray keys,
    jlongArray deltas) {
  JNI_METHOD_START
  auto keyBytes = toStateBytesVector(env, keys);
  auto longSumDeltas = toInt64Vector(env, deltas);
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJLongArray(env, aggregation.incrementLongSum(keyBytes, longSumDeltas));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementLongSumFromColumnarBatch( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jintArray keyOrdinals,
    jint valueOrdinal) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming columnar long sum requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJLongArray(
      env,
      aggregation.incrementLongSum(
          veloxBatch->getRowVector(),
          toInt32Vector(env, keyOrdinals),
          static_cast<int32_t>(valueOrdinal)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementLongSumFromColumnarBatchWithKeys( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jintArray keyOrdinals,
    jint valueOrdinal) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming columnar long sum requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJObjectArray(
      env,
      aggregation.incrementLongSumAndReturnKeys(
          veloxBatch->getRowVector(),
          toInt32Vector(env, keyOrdinals),
          static_cast<int32_t>(valueOrdinal)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementLongSumFromColumnarBatchWithKeyValues( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jintArray keyOrdinals,
    jint valueOrdinal) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming columnar long sum requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJObjectArrayWithValues(
      env,
      aggregation.incrementLongSumAndReturnKeys(
          veloxBatch->getRowVector(),
          toInt32Vector(env, keyOrdinals),
          static_cast<int32_t>(valueOrdinal)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementLongSumFromColumnarBatchWithFixedWidthRows( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jintArray keyOrdinals,
    jint valueOrdinal,
    jint valueFieldCount) {
  JNI_METHOD_START
  auto keyOrdinalVector = toInt32Vector(env, keyOrdinals);
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming columnar long sum requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJObjectArrayWithFixedWidthRows(
      env,
      aggregation.incrementLongSumAndReturnKeys(
          veloxBatch->getRowVector(), keyOrdinalVector, static_cast<int32_t>(valueOrdinal)),
      static_cast<int32_t>(keyOrdinalVector.size()),
      static_cast<int32_t>(valueFieldCount));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementLongSumSingleInt64KeyFromColumnarBatchWithFixedWidthRows( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jint keyOrdinal,
    jint valueOrdinal,
    jint valueFieldCount) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming typed columnar long sum requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJObjectArrayWithFixedWidthRows(
      env,
      aggregation.incrementLongSumSingleInt64KeyAndReturnKeys(
          veloxBatch->getRowVector(),
          static_cast<int32_t>(keyOrdinal),
          static_cast<int32_t>(valueOrdinal)),
      1,
      static_cast<int32_t>(valueFieldCount));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementLongSumSingleInt64KeyFromColumnarBatchTypedRows( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jint keyOrdinal,
    jint valueOrdinal) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming typed columnar long sum requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJLongArray(
      env,
      aggregation.incrementLongSumSingleInt64KeyAndReturnTypedRows(
          veloxBatch->getRowVector(),
          static_cast<int32_t>(keyOrdinal),
          static_cast<int32_t>(valueOrdinal)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeIncrementCountSingleInt64KeyFromColumnarBatchTypedRows( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jlong batchHandle,
    jint keyOrdinal) {
  JNI_METHOD_START
  auto batch = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(batchHandle);
  auto veloxBatch = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(batch);
  GLUTEN_CHECK(
      veloxBatch != nullptr,
      "Velox native streaming typed columnar count requires VeloxColumnarBatch input");
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJLongArray(
      env,
      aggregation.incrementCountSingleInt64KeyAndReturnTypedRows(
          veloxBatch->getRowVector(), static_cast<int32_t>(keyOrdinal)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeEvictCountKeys( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jobjectArray keys) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingAggregation aggregation(*store);
  return toJLongArray(env, aggregation.evictKeys(toStateBytesVector(env, keys)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeDeduplicate( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jobjectArray keys) {
  JNI_METHOD_START
  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingDeduplicate deduplicate(*store);
  return toJLongArray(env, deduplicate.deduplicate(toStateBytesVector(env, keys)));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeDeduplicateWithinWatermark( // NOLINT
    JNIEnv* env,
    jclass,
    jlong storeHandle,
    jobjectArray keys,
    jlongArray eventTimeMicros,
    jboolean hasLateWatermark,
    jlong lateWatermarkMicros,
    jlong delayThresholdMicros,
    jboolean hasEvictionWatermark,
    jlong evictionWatermarkMicros) {
  JNI_METHOD_START
  auto keyBytes = toStateBytesVector(env, keys);
  auto eventTimes = toInt64Vector(env, eventTimeMicros);
  GLUTEN_CHECK(
      keyBytes.size() == eventTimes.size(),
      "Velox native streaming deduplicate key and event-time vectors must have the same length");

  std::vector<gluten::NativeStreamingDeduplicateWatermarkInput> rows;
  rows.reserve(keyBytes.size());
  for (size_t index = 0; index < keyBytes.size(); ++index) {
    rows.push_back(gluten::NativeStreamingDeduplicateWatermarkInput{
        std::move(keyBytes[index]),
        eventTimes[index]});
  }

  auto store = storeHandles().get(storeHandle);
  gluten::VeloxNativeStreamingDeduplicate deduplicate(*store);
  return toJLongArray(
      env,
      deduplicate.deduplicateWithinWatermark(
          rows,
          hasLateWatermark ? std::make_optional(static_cast<int64_t>(lateWatermarkMicros))
                           : std::nullopt,
          static_cast<int64_t>(delayThresholdMicros),
          hasEvictionWatermark ? std::make_optional(static_cast<int64_t>(evictionWatermarkMicros))
                               : std::nullopt));
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeActiveStoreHandles( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  return storeHandles().size();
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_execution_streaming_state_VeloxNativeStateStoreJniWrapper_nativeActiveIteratorHandles( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  return iteratorHandles().size();
  JNI_METHOD_END(-1L)
}

#ifdef __cplusplus
}
#endif
