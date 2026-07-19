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

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace gluten {

using StateBytes = std::vector<uint8_t>;

struct StateBytesHash {
  size_t operator()(const StateBytes& bytes) const;
};

struct VeloxNativeStateStoreMetrics {
  int64_t numKeys{0};
  int64_t memoryUsedBytes{0};
};

class VeloxNativeStateStoreIterator {
 public:
  explicit VeloxNativeStateStoreIterator(std::vector<std::pair<StateBytes, StateBytes>> entries);

  bool hasNext() const;

  std::pair<StateBytes, StateBytes> next();

 private:
  std::vector<std::pair<StateBytes, StateBytes>> entries_;
  size_t index_{0};
};

// A grouping key that is either a single non-null int64 or the (single) SQL NULL
// key. Used by the typed fast path so the hot update loop can probe an int64-keyed
// map instead of hashing/allocating a 16-byte UnsafeRow blob per key.
struct TypedInt64Key {
  bool isNull{false};
  int64_t value{0};
};

class VeloxNativeStateStore {
 public:
  VeloxNativeStateStore(int64_t version, const uint8_t* snapshot, int32_t snapshotSize);

  std::optional<StateBytes> get(const StateBytes& key) const;

  void put(StateBytes key, StateBytes value);

  void remove(const StateBytes& key);

  // Typed fast path for the single-BIGINT-key + non-null-BIGINT-value streaming
  // aggregation shape (count / long-sum). The first call promotes the store into
  // typedInt64 mode: the resident map is rebuilt as an int64->int64 map (plus an
  // optional null-key slot) decoded from the existing 16-byte UnsafeRow entries,
  // and every later put/get/remove keeps the typed map authoritative. commit(),
  // commitDelta(), iterator() and metrics() synthesize the identical 16-byte
  // UnsafeRow snapshot/delta bytes from the typed map, so the on-disk format,
  // checksum parity, and dirty/rollback semantics are unchanged. Mixing typed and
  // byte-keyed mutations on the same store is unsupported and rejected.
  std::optional<int64_t> getTypedInt64(const TypedInt64Key& key);

  void putTypedInt64(const TypedInt64Key& key, int64_t value);

  // True once the store has been promoted to the typed int64 fast path.
  bool typedInt64Mode() const {
    return typedInt64Mode_;
  }

  // Serializes the full state map (used for periodic snapshots and back-compat).
  StateBytes commit() const;

  // Serializes only the keys mutated since this store was opened or since the
  // dirty set was last cleared: changed key/value pairs plus tombstones for
  // removed keys. The on-disk layout has a distinct magic from the full
  // snapshot so the two can never be confused. Returns the delta bytes; the
  // caller is responsible for clearing the dirty set after a durable write.
  StateBytes commitDelta() const;

  // Applies a delta produced by commitDelta() onto the resident map: upserts
  // changed keys and erases tombstoned keys. The delta itself is considered
  // already durable, so applied keys are NOT marked dirty.
  void applyDelta(const uint8_t* delta, int32_t deltaSize);

  // Drops the dirty/undo journal without touching the resident map. Called
  // after a delta/snapshot has been durably written so the next batch tracks
  // only its own mutations and so an abort after this point cannot undo
  // committed state.
  void clearDirty();

  // Reverts every key mutated since open / last clearDirty() back to its
  // pre-batch value (or removes it if it did not exist before the batch), then
  // clears the journal. This is the resident-store equivalent of discarding an
  // uncommitted batch and is O(keys mutated this batch), not O(total state).
  void rollback();

  // Advances the logical version of a resident store so it can be reused for
  // the next batch without re-deserializing from disk.
  void setVersion(int64_t version) {
    version_ = version;
  }

  std::shared_ptr<VeloxNativeStateStoreIterator> iterator() const;

  VeloxNativeStateStoreMetrics metrics() const;

  int64_t version() const {
    return version_;
  }

 private:
  // Records the pre-batch value of a key the first time it is mutated in the
  // current batch, so the mutation can be rolled back or serialized as a delta.
  void journalFirstTouch(const StateBytes& key, std::optional<StateBytes> priorValue);

  // Promotes the store into typedInt64 mode, decoding the existing 16-byte
  // UnsafeRow entries in state_ into the int64-keyed typed map. Idempotent.
  void promoteToTypedInt64();

  int64_t version_;
  std::unordered_map<StateBytes, StateBytes, StateBytesHash> state_;
  // Keys mutated since open / last clearDirty(), each mapped to the value the
  // key held before this batch started. A nullopt prior value means the key did
  // not exist before the batch. The key set doubles as the delta dirty set; the
  // prior values double as the undo log for rollback().
  std::unordered_map<StateBytes, std::optional<StateBytes>, StateBytesHash> journal_;

  // Typed int64 fast path. When typedInt64Mode_ is true, typedState_ (+ the
  // optional null-key slot) is the authoritative resident map and state_ is left
  // empty; the byte-oriented methods translate to/from the 16-byte UnsafeRow
  // encoding at their boundaries. Off by default; promoted on first typed access.
  bool typedInt64Mode_{false};
  std::unordered_map<int64_t, int64_t> typedState_;
  std::optional<int64_t> typedNullKeyValue_;
};

} // namespace gluten
