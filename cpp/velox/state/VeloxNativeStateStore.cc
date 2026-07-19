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

#include "state/VeloxNativeStateStore.h"

#include "utils/Exception.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <string>

namespace gluten {
namespace {

constexpr uint8_t kMagic[] = {'G', 'L', 'N', 'S', 'T', '0', '1', '\0'};
constexpr uint32_t kFormatVersion = 1;

// Delta files use a distinct magic so a full snapshot and a delta can never be
// deserialized through the wrong path. The trailing byte differs from kMagic.
constexpr uint8_t kDeltaMagic[] = {'G', 'L', 'N', 'S', 'T', '0', '1', 'D'};
constexpr uint32_t kDeltaFormatVersion = 1;

// Each delta entry is tagged so apply-time can distinguish an upsert (carries a
// value) from a tombstone (carries only the key).
constexpr uint8_t kDeltaEntryUpsert = 1;
constexpr uint8_t kDeltaEntryTombstone = 2;

uint32_t readU32(const uint8_t* data, int32_t size, int32_t& offset) {
  GLUTEN_CHECK(offset >= 0 && size - offset >= 4, "Corrupt Velox native state snapshot: truncated uint32");
  uint32_t value = static_cast<uint32_t>(data[offset]) |
      (static_cast<uint32_t>(data[offset + 1]) << 8) |
      (static_cast<uint32_t>(data[offset + 2]) << 16) |
      (static_cast<uint32_t>(data[offset + 3]) << 24);
  offset += 4;
  return value;
}

StateBytes readBytes(const uint8_t* data, int32_t size, int32_t& offset) {
  const uint32_t length = readU32(data, size, offset);
  GLUTEN_CHECK(length <= static_cast<uint32_t>(std::numeric_limits<int32_t>::max()), "Snapshot value too large");
  GLUTEN_CHECK(
      offset >= 0 && size - offset >= static_cast<int32_t>(length),
      "Corrupt Velox native state snapshot: truncated byte vector");
  StateBytes bytes(data + offset, data + offset + length);
  offset += static_cast<int32_t>(length);
  return bytes;
}

void writeU32(StateBytes& out, uint32_t value) {
  out.push_back(static_cast<uint8_t>(value & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 24) & 0xff));
}

void writeBytes(StateBytes& out, const StateBytes& bytes) {
  GLUTEN_CHECK(
      bytes.size() <= static_cast<size_t>(std::numeric_limits<uint32_t>::max()),
      "Velox native state byte vector exceeds checkpoint format limit");
  writeU32(out, static_cast<uint32_t>(bytes.size()));
  out.insert(out.end(), bytes.begin(), bytes.end());
}

// Typed int64 fast-path codec. A single-BIGINT-key streaming aggregation encodes
// keys and values as fixed 16-byte Spark UnsafeRows: an 8-byte null-bitset word
// followed by one 8-byte little-endian field. These helpers translate between
// that byte form (the authoritative on-disk/JNI format) and the int64 the typed
// resident map stores, so the typed path never hashes or allocates the blob.
constexpr size_t kTypedRowSize = 2 * sizeof(int64_t);

void appendInt64LE(StateBytes& out, int64_t value) {
  uint64_t raw = 0;
  std::memcpy(&raw, &value, sizeof(value));
  for (int offset = 0; offset < 64; offset += 8) {
    out.push_back(static_cast<uint8_t>((raw >> offset) & 0xff));
  }
}

int64_t readInt64LE(const StateBytes& bytes, size_t start) {
  uint64_t raw = 0;
  for (size_t i = 0; i < sizeof(int64_t); ++i) {
    raw |= static_cast<uint64_t>(bytes[start + i]) << (i * 8);
  }
  int64_t value = 0;
  std::memcpy(&value, &raw, sizeof(value));
  return value;
}

StateBytes encodeTypedKey(const TypedInt64Key& key) {
  StateBytes bytes;
  bytes.reserve(kTypedRowSize);
  appendInt64LE(bytes, key.isNull ? 1 : 0);
  appendInt64LE(bytes, key.isNull ? 0 : key.value);
  return bytes;
}

StateBytes encodeTypedValue(int64_t value) {
  StateBytes bytes;
  bytes.reserve(kTypedRowSize);
  appendInt64LE(bytes, 0);
  appendInt64LE(bytes, value);
  return bytes;
}

TypedInt64Key decodeTypedKey(const StateBytes& bytes) {
  GLUTEN_CHECK(
      bytes.size() == kTypedRowSize,
      "Velox native typed state requires a fixed-width 16-byte UnsafeRow key");
  TypedInt64Key key;
  key.isNull = readInt64LE(bytes, 0) != 0;
  key.value = key.isNull ? 0 : readInt64LE(bytes, sizeof(int64_t));
  return key;
}

int64_t decodeTypedValue(const StateBytes& bytes) {
  GLUTEN_CHECK(
      bytes.size() == kTypedRowSize,
      "Velox native typed state requires a fixed-width 16-byte UnsafeRow value");
  return readInt64LE(bytes, sizeof(int64_t));
}

} // namespace

size_t StateBytesHash::operator()(const StateBytes& bytes) const {
  size_t hash = 1469598103934665603ULL;
  for (const auto byte : bytes) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  return hash;
}

VeloxNativeStateStoreIterator::VeloxNativeStateStoreIterator(
    std::vector<std::pair<StateBytes, StateBytes>> entries)
    : entries_(std::move(entries)) {}

bool VeloxNativeStateStoreIterator::hasNext() const {
  return index_ < entries_.size();
}

std::pair<StateBytes, StateBytes> VeloxNativeStateStoreIterator::next() {
  GLUTEN_CHECK(hasNext(), "Velox native state iterator exhausted");
  return entries_[index_++];
}

VeloxNativeStateStore::VeloxNativeStateStore(int64_t version, const uint8_t* snapshot, int32_t snapshotSize)
    : version_(version) {
  GLUTEN_CHECK(version >= 0, "Velox native state store requires a non-negative version");
  if (snapshot == nullptr) {
    GLUTEN_CHECK(version == 0, "Missing Velox native state snapshot for non-zero version");
    return;
  }
  GLUTEN_CHECK(snapshotSize > 0, "Corrupt Velox native state snapshot: empty snapshot");

  int32_t offset = 0;
  GLUTEN_CHECK(
      snapshotSize >= static_cast<int32_t>(sizeof(kMagic)),
      "Corrupt Velox native state snapshot: missing magic");
  GLUTEN_CHECK(
      std::memcmp(snapshot, kMagic, sizeof(kMagic)) == 0,
      "Corrupt Velox native state snapshot: invalid magic");
  offset += sizeof(kMagic);

  const uint32_t formatVersion = readU32(snapshot, snapshotSize, offset);
  GLUTEN_CHECK(
      formatVersion == kFormatVersion,
      "Unsupported Velox native state snapshot format version: " + std::to_string(formatVersion));

  const uint32_t entryCount = readU32(snapshot, snapshotSize, offset);
  const auto remainingBytes = snapshotSize - offset;
  constexpr int32_t kMinBytesPerEntry = 2 * static_cast<int32_t>(sizeof(uint32_t));
  GLUTEN_CHECK(
      remainingBytes >= 0 && entryCount <= static_cast<uint32_t>(remainingBytes / kMinBytesPerEntry),
      "Corrupt Velox native state snapshot: entry count exceeds remaining snapshot bytes");
  state_.reserve(entryCount);
  for (uint32_t i = 0; i < entryCount; ++i) {
    StateBytes key = readBytes(snapshot, snapshotSize, offset);
    StateBytes value = readBytes(snapshot, snapshotSize, offset);
    const auto inserted = state_.emplace(std::move(key), std::move(value)).second;
    GLUTEN_CHECK(inserted, "Corrupt Velox native state snapshot: duplicate key");
  }
  GLUTEN_CHECK(
      offset == snapshotSize,
      "Corrupt Velox native state snapshot: trailing bytes after state entries");
}

std::optional<StateBytes> VeloxNativeStateStore::get(const StateBytes& key) const {
  if (typedInt64Mode_) {
    const auto typedKey = decodeTypedKey(key);
    if (typedKey.isNull) {
      return typedNullKeyValue_.has_value()
          ? std::make_optional(encodeTypedValue(typedNullKeyValue_.value()))
          : std::nullopt;
    }
    const auto it = typedState_.find(typedKey.value);
    if (it == typedState_.end()) {
      return std::nullopt;
    }
    return encodeTypedValue(it->second);
  }
  const auto it = state_.find(key);
  if (it == state_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void VeloxNativeStateStore::journalFirstTouch(
    const StateBytes& key,
    std::optional<StateBytes> priorValue) {
  // Only the first mutation of a key in a batch records its pre-batch value;
  // later mutations leave the recorded prior value untouched so rollback
  // restores the original, not an intermediate, value.
  journal_.emplace(key, std::move(priorValue));
}

void VeloxNativeStateStore::put(StateBytes key, StateBytes value) {
  if (typedInt64Mode_) {
    // A byte-level put on a typed store (e.g. JNI nativePut) round-trips through
    // the typed codec so the typed map stays authoritative.
    putTypedInt64(decodeTypedKey(key), decodeTypedValue(value));
    return;
  }
  const auto it = state_.find(key);
  journalFirstTouch(key, it == state_.end() ? std::nullopt : std::make_optional(it->second));
  if (it == state_.end()) {
    state_.emplace(std::move(key), std::move(value));
  } else {
    it->second = std::move(value);
  }
}

void VeloxNativeStateStore::remove(const StateBytes& key) {
  if (typedInt64Mode_) {
    const auto typedKey = decodeTypedKey(key);
    if (typedKey.isNull) {
      if (!typedNullKeyValue_.has_value()) {
        return;
      }
      journalFirstTouch(key, std::make_optional(encodeTypedValue(typedNullKeyValue_.value())));
      typedNullKeyValue_.reset();
      return;
    }
    const auto it = typedState_.find(typedKey.value);
    if (it == typedState_.end()) {
      return;
    }
    journalFirstTouch(key, std::make_optional(encodeTypedValue(it->second)));
    typedState_.erase(it);
    return;
  }
  const auto it = state_.find(key);
  if (it == state_.end()) {
    // Removing a key that was never present produces neither a change nor a
    // tombstone, so it must not enter the journal or the delta.
    return;
  }
  journalFirstTouch(key, std::make_optional(it->second));
  state_.erase(it);
}

void VeloxNativeStateStore::promoteToTypedInt64() {
  if (typedInt64Mode_) {
    return;
  }
  typedState_.reserve(state_.size());
  for (const auto& [key, value] : state_) {
    const auto typedKey = decodeTypedKey(key);
    const auto typedValue = decodeTypedValue(value);
    if (typedKey.isNull) {
      typedNullKeyValue_ = typedValue;
    } else {
      typedState_.emplace(typedKey.value, typedValue);
    }
  }
  state_.clear();
  typedInt64Mode_ = true;
}

std::optional<int64_t> VeloxNativeStateStore::getTypedInt64(const TypedInt64Key& key) {
  promoteToTypedInt64();
  if (key.isNull) {
    return typedNullKeyValue_;
  }
  const auto it = typedState_.find(key.value);
  if (it == typedState_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void VeloxNativeStateStore::putTypedInt64(const TypedInt64Key& key, int64_t value) {
  promoteToTypedInt64();
  if (key.isNull) {
    journalFirstTouch(
        encodeTypedKey(key),
        typedNullKeyValue_.has_value()
            ? std::make_optional(encodeTypedValue(typedNullKeyValue_.value()))
            : std::nullopt);
    typedNullKeyValue_ = value;
    return;
  }
  const auto it = typedState_.find(key.value);
  journalFirstTouch(
      encodeTypedKey(key),
      it == typedState_.end() ? std::nullopt
                              : std::make_optional(encodeTypedValue(it->second)));
  if (it == typedState_.end()) {
    typedState_.emplace(key.value, value);
  } else {
    it->second = value;
  }
}

StateBytes VeloxNativeStateStore::commit() const {
  if (typedInt64Mode_) {
    // Synthesize the identical sorted 16-byte UnsafeRow snapshot from the typed
    // map so the on-disk format and checksum parity are unchanged.
    std::vector<std::pair<StateBytes, StateBytes>> entries;
    entries.reserve(typedState_.size() + (typedNullKeyValue_.has_value() ? 1 : 0));
    for (const auto& [key, value] : typedState_) {
      entries.emplace_back(encodeTypedKey(TypedInt64Key{false, key}), encodeTypedValue(value));
    }
    if (typedNullKeyValue_.has_value()) {
      entries.emplace_back(
          encodeTypedKey(TypedInt64Key{true, 0}), encodeTypedValue(typedNullKeyValue_.value()));
    }
    std::sort(entries.begin(), entries.end(), [](const auto& left, const auto& right) {
      return std::lexicographical_compare(
          left.first.begin(), left.first.end(), right.first.begin(), right.first.end());
    });

    StateBytes snapshot;
    snapshot.insert(snapshot.end(), kMagic, kMagic + sizeof(kMagic));
    writeU32(snapshot, kFormatVersion);
    GLUTEN_CHECK(
        entries.size() <= static_cast<size_t>(std::numeric_limits<uint32_t>::max()),
        "Velox native state key count exceeds checkpoint format limit");
    writeU32(snapshot, static_cast<uint32_t>(entries.size()));
    for (const auto& [key, value] : entries) {
      writeBytes(snapshot, key);
      writeBytes(snapshot, value);
    }
    return snapshot;
  }

  std::vector<const StateBytes*> keys;
  keys.reserve(state_.size());
  for (const auto& entry : state_) {
    keys.emplace_back(&entry.first);
  }
  std::sort(keys.begin(), keys.end(), [](const StateBytes* left, const StateBytes* right) {
    return std::lexicographical_compare(left->begin(), left->end(), right->begin(), right->end());
  });

  StateBytes snapshot;
  snapshot.insert(snapshot.end(), kMagic, kMagic + sizeof(kMagic));
  writeU32(snapshot, kFormatVersion);
  GLUTEN_CHECK(
      state_.size() <= static_cast<size_t>(std::numeric_limits<uint32_t>::max()),
      "Velox native state key count exceeds checkpoint format limit");
  writeU32(snapshot, static_cast<uint32_t>(state_.size()));
  for (const auto* key : keys) {
    writeBytes(snapshot, *key);
    writeBytes(snapshot, state_.at(*key));
  }
  return snapshot;
}

StateBytes VeloxNativeStateStore::commitDelta() const {
  // Sort the dirty keys lexicographically so a given mutation set always
  // produces identical bytes, mirroring the full-snapshot determinism.
  std::vector<const StateBytes*> keys;
  keys.reserve(journal_.size());
  for (const auto& entry : journal_) {
    keys.emplace_back(&entry.first);
  }
  std::sort(keys.begin(), keys.end(), [](const StateBytes* left, const StateBytes* right) {
    return std::lexicographical_compare(left->begin(), left->end(), right->begin(), right->end());
  });

  StateBytes delta;
  delta.insert(delta.end(), kDeltaMagic, kDeltaMagic + sizeof(kDeltaMagic));
  writeU32(delta, kDeltaFormatVersion);
  GLUTEN_CHECK(
      journal_.size() <= static_cast<size_t>(std::numeric_limits<uint32_t>::max()),
      "Velox native state delta key count exceeds checkpoint format limit");
  writeU32(delta, static_cast<uint32_t>(journal_.size()));
  for (const auto* key : keys) {
    // In typed mode the current value lives in the typed map; get() synthesizes
    // the byte form so the delta layout stays identical to the byte-map path.
    const auto current = get(*key);
    if (!current.has_value()) {
      delta.push_back(kDeltaEntryTombstone);
      writeBytes(delta, *key);
    } else {
      delta.push_back(kDeltaEntryUpsert);
      writeBytes(delta, *key);
      writeBytes(delta, current.value());
    }
  }
  return delta;
}

void VeloxNativeStateStore::applyDelta(const uint8_t* delta, int32_t deltaSize) {
  GLUTEN_CHECK(delta != nullptr, "Missing Velox native state delta");
  GLUTEN_CHECK(deltaSize > 0, "Corrupt Velox native state delta: empty delta");

  int32_t offset = 0;
  GLUTEN_CHECK(
      deltaSize >= static_cast<int32_t>(sizeof(kDeltaMagic)),
      "Corrupt Velox native state delta: missing magic");
  GLUTEN_CHECK(
      std::memcmp(delta, kDeltaMagic, sizeof(kDeltaMagic)) == 0,
      "Corrupt Velox native state delta: invalid magic");
  offset += sizeof(kDeltaMagic);

  const uint32_t formatVersion = readU32(delta, deltaSize, offset);
  GLUTEN_CHECK(
      formatVersion == kDeltaFormatVersion,
      "Unsupported Velox native state delta format version: " + std::to_string(formatVersion));

  const uint32_t entryCount = readU32(delta, deltaSize, offset);
  for (uint32_t i = 0; i < entryCount; ++i) {
    GLUTEN_CHECK(
        offset >= 0 && deltaSize - offset >= 1,
        "Corrupt Velox native state delta: truncated entry tag");
    const uint8_t tag = delta[offset++];
    if (tag == kDeltaEntryTombstone) {
      StateBytes key = readBytes(delta, deltaSize, offset);
      if (typedInt64Mode_) {
        const auto typedKey = decodeTypedKey(key);
        if (typedKey.isNull) {
          typedNullKeyValue_.reset();
        } else {
          typedState_.erase(typedKey.value);
        }
      } else {
        state_.erase(key);
      }
    } else if (tag == kDeltaEntryUpsert) {
      StateBytes key = readBytes(delta, deltaSize, offset);
      StateBytes value = readBytes(delta, deltaSize, offset);
      if (typedInt64Mode_) {
        const auto typedKey = decodeTypedKey(key);
        const auto typedValue = decodeTypedValue(value);
        if (typedKey.isNull) {
          typedNullKeyValue_ = typedValue;
        } else {
          typedState_[typedKey.value] = typedValue;
        }
      } else {
        state_[std::move(key)] = std::move(value);
      }
    } else {
      throw GlutenException("Corrupt Velox native state delta: unknown entry tag");
    }
  }
  GLUTEN_CHECK(
      offset == deltaSize,
      "Corrupt Velox native state delta: trailing bytes after delta entries");
}

void VeloxNativeStateStore::clearDirty() {
  journal_.clear();
}

void VeloxNativeStateStore::rollback() {
  for (auto& [key, priorValue] : journal_) {
    if (typedInt64Mode_) {
      const auto typedKey = decodeTypedKey(key);
      if (priorValue.has_value()) {
        const auto typedValue = decodeTypedValue(priorValue.value());
        if (typedKey.isNull) {
          typedNullKeyValue_ = typedValue;
        } else {
          typedState_[typedKey.value] = typedValue;
        }
      } else if (typedKey.isNull) {
        typedNullKeyValue_.reset();
      } else {
        typedState_.erase(typedKey.value);
      }
    } else if (priorValue.has_value()) {
      state_[key] = std::move(priorValue.value());
    } else {
      state_.erase(key);
    }
  }
  journal_.clear();
}

std::shared_ptr<VeloxNativeStateStoreIterator> VeloxNativeStateStore::iterator() const {
  std::vector<std::pair<StateBytes, StateBytes>> entries;
  if (typedInt64Mode_) {
    entries.reserve(typedState_.size() + (typedNullKeyValue_.has_value() ? 1 : 0));
    for (const auto& [key, value] : typedState_) {
      entries.emplace_back(encodeTypedKey(TypedInt64Key{false, key}), encodeTypedValue(value));
    }
    if (typedNullKeyValue_.has_value()) {
      entries.emplace_back(
          encodeTypedKey(TypedInt64Key{true, 0}), encodeTypedValue(typedNullKeyValue_.value()));
    }
  } else {
    entries.reserve(state_.size());
    for (const auto& [key, value] : state_) {
      entries.emplace_back(key, value);
    }
  }
  std::sort(entries.begin(), entries.end(), [](const auto& left, const auto& right) {
    return std::lexicographical_compare(
        left.first.begin(), left.first.end(), right.first.begin(), right.first.end());
  });
  return std::make_shared<VeloxNativeStateStoreIterator>(std::move(entries));
}

VeloxNativeStateStoreMetrics VeloxNativeStateStore::metrics() const {
  if (typedInt64Mode_) {
    const int64_t numKeys =
        static_cast<int64_t>(typedState_.size()) + (typedNullKeyValue_.has_value() ? 1 : 0);
    // Each typed entry serializes to a fixed 16-byte UnsafeRow key + 16-byte
    // value, matching the byte-map accounting for this shape.
    const int64_t memoryUsedBytes = numKeys * static_cast<int64_t>(2 * kTypedRowSize);
    return VeloxNativeStateStoreMetrics{numKeys, memoryUsedBytes};
  }
  int64_t memoryUsedBytes = 0;
  for (const auto& [key, value] : state_) {
    memoryUsedBytes += static_cast<int64_t>(key.size() + value.size());
  }
  return VeloxNativeStateStoreMetrics{static_cast<int64_t>(state_.size()), memoryUsedBytes};
}

} // namespace gluten
