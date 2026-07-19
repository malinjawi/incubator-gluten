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

#include <gtest/gtest.h>

#include "state/VeloxNativeStateStore.h"
#include "utils/Exception.h"

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

namespace gluten {
namespace {

StateBytes bytes(std::initializer_list<uint8_t> values) {
  return StateBytes(values);
}

StateBytes repeatedBytes(uint8_t value, size_t size) {
  return StateBytes(size, value);
}

void appendU32(StateBytes& out, uint32_t value) {
  out.push_back(static_cast<uint8_t>(value & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 24) & 0xff));
}

void appendBytes(StateBytes& out, const StateBytes& bytes) {
  appendU32(out, static_cast<uint32_t>(bytes.size()));
  out.insert(out.end(), bytes.begin(), bytes.end());
}

StateBytes snapshotHeader(uint32_t entryCount) {
  StateBytes snapshot{'G', 'L', 'N', 'S', 'T', '0', '1', '\0'};
  appendU32(snapshot, 1);
  appendU32(snapshot, entryCount);
  return snapshot;
}

StateBytes snapshotWithEntries(const std::vector<std::pair<StateBytes, StateBytes>>& entries) {
  auto snapshot = snapshotHeader(static_cast<uint32_t>(entries.size()));
  for (const auto& [key, value] : entries) {
    appendBytes(snapshot, key);
    appendBytes(snapshot, value);
  }
  return snapshot;
}

VeloxNativeStateStore reopen(int64_t version, const StateBytes& snapshot) {
  return VeloxNativeStateStore(version, snapshot.data(), static_cast<int32_t>(snapshot.size()));
}

int64_t positiveIntEnv(const char* name, int64_t defaultValue) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || raw[0] == '\0') {
    return defaultValue;
  }

  char* end = nullptr;
  errno = 0;
  const auto parsed = std::strtoll(raw, &end, 10);
  if (errno != 0 || end == raw || *end != '\0' || parsed <= 0) {
    throw GlutenException(std::string(name) + " must be a positive integer, got '" + raw + "'");
  }
  return parsed;
}

template <typename Func>
void expectGlutenExceptionMessageContains(Func&& action, const std::string& expected) {
  try {
    action();
    FAIL() << "Expected GlutenException containing '" << expected << "'";
  } catch (const GlutenException& error) {
    ASSERT_TRUE(std::string(error.what()).find(expected) != std::string::npos)
        << "Expected error message to contain '" << expected << "', but received '"
        << error.what() << "'.";
  }
}

} // namespace

TEST(VeloxNativeStateStoreTest, commitAndReopen) {
  VeloxNativeStateStore store(0, nullptr, 0);
  store.put(bytes({2}), bytes({20}));
  store.put(bytes({1}), bytes({10}));

  const auto snapshot = store.commit();
  auto reopened = reopen(1, snapshot);

  EXPECT_EQ(reopened.version(), 1);
  ASSERT_TRUE(reopened.get(bytes({1})).has_value());
  ASSERT_TRUE(reopened.get(bytes({2})).has_value());
  EXPECT_EQ(reopened.get(bytes({1})).value(), bytes({10}));
  EXPECT_EQ(reopened.get(bytes({2})).value(), bytes({20}));
  EXPECT_FALSE(reopened.get(bytes({3})).has_value());
  EXPECT_EQ(reopened.metrics().numKeys, 2);
  EXPECT_EQ(reopened.metrics().memoryUsedBytes, 4);
}

TEST(VeloxNativeStateStoreTest, commitIsDeterministic) {
  VeloxNativeStateStore left(0, nullptr, 0);
  left.put(bytes({2}), bytes({20}));
  left.put(bytes({1}), bytes({10}));

  VeloxNativeStateStore right(0, nullptr, 0);
  right.put(bytes({1}), bytes({10}));
  right.put(bytes({2}), bytes({20}));

  EXPECT_EQ(left.commit(), right.commit());
}

TEST(VeloxNativeStateStoreTest, removeAndIteratorReplayInKeyOrder) {
  VeloxNativeStateStore store(0, nullptr, 0);
  store.put(bytes({3}), bytes({30}));
  store.put(bytes({1}), bytes({10}));
  store.put(bytes({2}), bytes({20}));
  store.remove(bytes({2}));

  auto reopened = reopen(1, store.commit());
  EXPECT_FALSE(reopened.get(bytes({2})).has_value());

  auto iterator = reopened.iterator();
  ASSERT_TRUE(iterator->hasNext());
  EXPECT_EQ(iterator->next(), std::make_pair(bytes({1}), bytes({10})));
  ASSERT_TRUE(iterator->hasNext());
  EXPECT_EQ(iterator->next(), std::make_pair(bytes({3}), bytes({30})));
  EXPECT_FALSE(iterator->hasNext());
  EXPECT_THROW(iterator->next(), GlutenException);
}

TEST(VeloxNativeStateStoreTest, rejectsCorruptSnapshots) {
  const auto corrupt = bytes({0x6e, 0x6f, 0x70, 0x65});
  EXPECT_THROW(reopen(1, corrupt), GlutenException);
  EXPECT_THROW(VeloxNativeStateStore(1, nullptr, 0), GlutenException);
}

TEST(VeloxNativeStateStoreTest, rejectsInvalidVersionsAndEmptySnapshots) {
  expectGlutenExceptionMessageContains(
      [&]() { VeloxNativeStateStore(-1, nullptr, 0); },
      "non-negative version");

  const uint8_t emptySnapshotSentinel = 0;
  expectGlutenExceptionMessageContains(
      [&]() { VeloxNativeStateStore(0, &emptySnapshotSentinel, 0); },
      "empty snapshot");
}

TEST(VeloxNativeStateStoreTest, rejectsImpossibleSnapshotEntryCountBeforeReserve) {
  const auto corrupt = snapshotHeader(std::numeric_limits<uint32_t>::max());

  expectGlutenExceptionMessageContains(
      [&]() { reopen(1, corrupt); },
      "entry count exceeds remaining snapshot bytes");
}

TEST(VeloxNativeStateStoreTest, rejectsDuplicateSerializedSnapshotKeys) {
  const auto corrupt = snapshotWithEntries({
      {bytes({1}), bytes({10})},
      {bytes({1}), bytes({11})},
  });

  expectGlutenExceptionMessageContains([&]() { reopen(1, corrupt); }, "duplicate key");
}

TEST(VeloxNativeStateStoreTest, repeatedCommitReopenLifecycle) {
  StateBytes snapshot;
  const auto versions = positiveIntEnv("GLUTEN_NATIVE_STREAMING_STATE_STORE_REOPEN_VERSIONS", 100);
  for (int64_t version = 0; version < versions; ++version) {
    auto store = reopen(version, snapshot);
    store.put(bytes({static_cast<uint8_t>(version % 17)}), bytes({static_cast<uint8_t>(version)}));
    if (version % 3 == 0) {
      store.remove(bytes({static_cast<uint8_t>((version + 5) % 17)}));
    }
    snapshot = store.commit();
  }

  auto reopened = reopen(versions, snapshot);
  EXPECT_EQ(reopened.version(), versions);
  EXPECT_LE(reopened.metrics().numKeys, 17);
  EXPECT_GT(reopened.metrics().numKeys, 0);
  EXPECT_EQ(reopened.metrics().memoryUsedBytes, reopened.metrics().numKeys * 2);

  auto iterator = reopened.iterator();
  int64_t iteratedKeys = 0;
  while (iterator->hasNext()) {
    iterator->next();
    ++iteratedKeys;
  }
  EXPECT_EQ(iteratedKeys, reopened.metrics().numKeys);
}

TEST(VeloxNativeStateStoreTest, deltaCapturesUpsertsAndTombstones) {
  // Base snapshot with two keys.
  VeloxNativeStateStore base(0, nullptr, 0);
  base.put(bytes({1}), bytes({10}));
  base.put(bytes({2}), bytes({20}));
  const auto baseSnapshot = base.commit();

  // Open the next version, mutate, and serialize only the delta.
  auto store = reopen(1, baseSnapshot);
  store.put(bytes({2}), bytes({22})); // change existing key
  store.put(bytes({3}), bytes({30})); // new key
  store.remove(bytes({1})); // remove existing key
  store.remove(bytes({9})); // removing an absent key must not appear in the delta
  const auto delta = store.commitDelta();

  // Replay the delta onto a fresh resident copy of the base snapshot.
  auto replay = reopen(1, baseSnapshot);
  replay.applyDelta(delta.data(), static_cast<int32_t>(delta.size()));

  EXPECT_FALSE(replay.get(bytes({1})).has_value());
  EXPECT_EQ(replay.get(bytes({2})).value(), bytes({22}));
  EXPECT_EQ(replay.get(bytes({3})).value(), bytes({30}));
  EXPECT_EQ(replay.metrics().numKeys, 2);

  // A delta-replayed store yields the same full snapshot as one that applied
  // the mutations directly.
  EXPECT_EQ(replay.commit(), store.commit());
}

TEST(VeloxNativeStateStoreTest, clearDirtyResetsDeltaScope) {
  VeloxNativeStateStore store(0, nullptr, 0);
  store.put(bytes({1}), bytes({10}));
  store.clearDirty();

  // After clearDirty, only subsequent mutations belong to the next delta.
  store.put(bytes({2}), bytes({20}));
  const auto delta = store.commitDelta();

  VeloxNativeStateStore replay(0, nullptr, 0);
  replay.put(bytes({1}), bytes({10})); // pretend the base already had key 1
  replay.clearDirty();
  replay.applyDelta(delta.data(), static_cast<int32_t>(delta.size()));

  EXPECT_EQ(replay.get(bytes({1})).value(), bytes({10}));
  EXPECT_EQ(replay.get(bytes({2})).value(), bytes({20}));
  EXPECT_EQ(replay.metrics().numKeys, 2);
}

TEST(VeloxNativeStateStoreTest, rollbackRestoresPreBatchStateInChangedKeyOrder) {
  VeloxNativeStateStore base(0, nullptr, 0);
  base.put(bytes({1}), bytes({10}));
  base.put(bytes({2}), bytes({20}));
  const auto baseSnapshot = base.commit();

  auto store = reopen(1, baseSnapshot);
  store.put(bytes({2}), bytes({99})); // mutate existing
  store.put(bytes({2}), bytes({77})); // mutate again - rollback must restore 20, not 99
  store.put(bytes({3}), bytes({30})); // add new
  store.remove(bytes({1})); // remove existing
  store.rollback();

  // After rollback the store must be byte-identical to the base snapshot.
  EXPECT_EQ(store.commit(), baseSnapshot);
  EXPECT_EQ(store.get(bytes({1})).value(), bytes({10}));
  EXPECT_EQ(store.get(bytes({2})).value(), bytes({20}));
  EXPECT_FALSE(store.get(bytes({3})).has_value());
  EXPECT_EQ(store.metrics().numKeys, 2);

  // A delta serialized after rollback must be empty (no surviving mutations).
  auto fresh = reopen(1, baseSnapshot);
  fresh.applyDelta(
      store.commitDelta().data(), static_cast<int32_t>(store.commitDelta().size()));
  EXPECT_EQ(fresh.commit(), baseSnapshot);
}

TEST(VeloxNativeStateStoreTest, clearDirtyMakesMutationsUnrollbackable) {
  VeloxNativeStateStore store(0, nullptr, 0);
  store.put(bytes({1}), bytes({10}));
  store.clearDirty(); // commit point: prior batch is now durable

  store.put(bytes({2}), bytes({20}));
  store.rollback(); // only the post-clearDirty mutation is undone

  EXPECT_EQ(store.get(bytes({1})).value(), bytes({10}));
  EXPECT_FALSE(store.get(bytes({2})).has_value());
  EXPECT_EQ(store.metrics().numKeys, 1);
}

TEST(VeloxNativeStateStoreTest, emptyDeltaIsValidAndIdempotent) {
  VeloxNativeStateStore base(0, nullptr, 0);
  base.put(bytes({1}), bytes({10}));
  const auto baseSnapshot = base.commit();

  auto store = reopen(1, baseSnapshot);
  const auto delta = store.commitDelta(); // no mutations this batch

  auto replay = reopen(1, baseSnapshot);
  replay.applyDelta(delta.data(), static_cast<int32_t>(delta.size()));
  EXPECT_EQ(replay.get(bytes({1})).value(), bytes({10}));
  EXPECT_EQ(replay.metrics().numKeys, 1);
}

TEST(VeloxNativeStateStoreTest, applyDeltaRejectsSnapshotBytes) {
  VeloxNativeStateStore base(0, nullptr, 0);
  base.put(bytes({1}), bytes({10}));
  const auto snapshot = base.commit();

  VeloxNativeStateStore store(0, nullptr, 0);
  expectGlutenExceptionMessageContains(
      [&]() { store.applyDelta(snapshot.data(), static_cast<int32_t>(snapshot.size())); },
      "invalid magic");
}

TEST(VeloxNativeStateStoreTest, openStoreRejectsDeltaBytes) {
  VeloxNativeStateStore base(0, nullptr, 0);
  base.put(bytes({1}), bytes({10}));
  const auto delta = base.commitDelta();

  expectGlutenExceptionMessageContains([&]() { reopen(1, delta); }, "invalid magic");
}

TEST(VeloxNativeStateStoreTest, deltaReplayMatchesFullSnapshotAcrossVersions) {
  // Drive a resident store across versions writing only deltas, and rebuild a
  // cold-start replica from the base snapshot + delta chain. Both must agree.
  VeloxNativeStateStore resident(0, nullptr, 0);
  const auto baseSnapshot = resident.commit(); // empty base at version 0

  std::vector<StateBytes> deltas;
  const auto versions = positiveIntEnv("GLUTEN_NATIVE_STREAMING_STATE_STORE_DELTA_VERSIONS", 64);
  for (int64_t version = 0; version < versions; ++version) {
    resident.clearDirty();
    resident.setVersion(version);
    const auto keyId = static_cast<uint8_t>(version % 17);
    resident.put(bytes({keyId}), bytes({static_cast<uint8_t>(version)}));
    if (version % 3 == 0) {
      resident.remove(bytes({static_cast<uint8_t>((version + 5) % 17)}));
    }
    deltas.push_back(resident.commitDelta());
  }

  auto replay = reopen(0, baseSnapshot);
  for (const auto& delta : deltas) {
    replay.applyDelta(delta.data(), static_cast<int32_t>(delta.size()));
  }
  EXPECT_EQ(replay.commit(), resident.commit());
}

TEST(VeloxNativeStateStoreTest, repeatedLargeSnapshotLifecycleTracksMemory) {
  StateBytes snapshot;
  const auto versions =
      positiveIntEnv("GLUTEN_NATIVE_STREAMING_STATE_STORE_LARGE_SNAPSHOT_VERSIONS", 250);
  constexpr int64_t kLiveKeys = 64;
  constexpr int64_t kKeySpace = kLiveKeys * 2;
  constexpr size_t kValueSize = 128;

  for (int64_t version = 0; version < versions; ++version) {
    auto store = reopen(version, snapshot);
    const auto keyId = static_cast<uint8_t>(version % kKeySpace);
    store.put(bytes({keyId}), repeatedBytes(static_cast<uint8_t>(version), kValueSize));
    if (version >= kLiveKeys) {
      store.remove(bytes({static_cast<uint8_t>((version - kLiveKeys) % kKeySpace)}));
    }
    snapshot = store.commit();

    auto reopened = reopen(version + 1, snapshot);
    EXPECT_LE(reopened.metrics().numKeys, kLiveKeys);
    EXPECT_LE(reopened.metrics().memoryUsedBytes, kLiveKeys * static_cast<int64_t>(kValueSize + 1));
    EXPECT_EQ(reopened.commit(), snapshot);
  }

  auto finalStore = reopen(versions, snapshot);
  EXPECT_GT(finalStore.metrics().numKeys, 0);
  EXPECT_LE(finalStore.metrics().numKeys, kLiveKeys);
  EXPECT_LE(finalStore.metrics().memoryUsedBytes, kLiveKeys * static_cast<int64_t>(kValueSize + 1));
}

namespace {
// Encodes the fixed 16-byte Spark UnsafeRow form of a single-int64 key/value the
// typed fast path produces, so a byte-map store can be driven with identical
// logical content for snapshot-parity assertions.
StateBytes typedKeyBytes(bool isNull, int64_t value) {
  StateBytes out;
  auto appendI64 = [&](int64_t v) {
    auto raw = static_cast<uint64_t>(v);
    for (int o = 0; o < 64; o += 8) {
      out.push_back(static_cast<uint8_t>((raw >> o) & 0xff));
    }
  };
  appendI64(isNull ? 1 : 0);
  appendI64(isNull ? 0 : value);
  return out;
}
StateBytes typedValueBytes(int64_t value) {
  return typedKeyBytes(false, value); // [0][value] matches the value encoding
}
} // namespace

TEST(VeloxNativeStateStoreTest, typedInt64ModeMatchesByteMapSnapshotAndSupportsDeltaRollback) {
  // Byte-map reference: drive the store through the byte API with the same 16-byte
  // UnsafeRow encoding the typed path uses.
  VeloxNativeStateStore reference(0, nullptr, 0);
  reference.put(typedKeyBytes(false, 7), typedValueBytes(70));
  reference.put(typedKeyBytes(false, 42), typedValueBytes(420));
  reference.put(typedKeyBytes(true, 0), typedValueBytes(999)); // null key
  const auto referenceSnapshot = reference.commit();

  // Typed store: same logical content via the typed accessors. The snapshot bytes
  // must be byte-identical so on-disk format + checksum parity hold.
  VeloxNativeStateStore typed(0, nullptr, 0);
  EXPECT_FALSE(typed.typedInt64Mode());
  typed.putTypedInt64(TypedInt64Key{false, 7}, 70);
  typed.putTypedInt64(TypedInt64Key{false, 42}, 420);
  typed.putTypedInt64(TypedInt64Key{true, 0}, 999);
  EXPECT_TRUE(typed.typedInt64Mode());
  EXPECT_EQ(typed.commit(), referenceSnapshot);

  // Typed reads round-trip through the byte API too.
  EXPECT_EQ(typed.getTypedInt64(TypedInt64Key{false, 7}).value(), 70);
  EXPECT_EQ(typed.getTypedInt64(TypedInt64Key{false, 99}).has_value(), false);
  EXPECT_EQ(typed.get(typedKeyBytes(false, 42)).value(), typedValueBytes(420));

  // Reopen from the typed snapshot, promote on first typed access, mutate, and
  // assert the delta replays onto a fresh byte-map replica to the same snapshot.
  auto resident = reopen(1, referenceSnapshot);
  resident.getTypedInt64(TypedInt64Key{false, 7}); // promote
  EXPECT_TRUE(resident.typedInt64Mode());
  resident.putTypedInt64(TypedInt64Key{false, 7}, 71); // update existing
  resident.putTypedInt64(TypedInt64Key{false, 5}, 50); // new key
  resident.remove(typedKeyBytes(false, 42)); // remove existing
  const auto delta = resident.commitDelta();

  auto replay = reopen(1, referenceSnapshot);
  replay.applyDelta(delta.data(), static_cast<int32_t>(delta.size()));
  EXPECT_EQ(replay.commit(), resident.commit());

  // Rollback in typed mode restores the pre-batch snapshot exactly.
  auto rollbackStore = reopen(1, referenceSnapshot);
  rollbackStore.getTypedInt64(TypedInt64Key{false, 7}); // promote
  rollbackStore.putTypedInt64(TypedInt64Key{false, 7}, 7100);
  rollbackStore.putTypedInt64(TypedInt64Key{false, 123}, 1230);
  rollbackStore.remove(typedKeyBytes(true, 0));
  rollbackStore.rollback();
  EXPECT_EQ(rollbackStore.commit(), referenceSnapshot);
  EXPECT_EQ(rollbackStore.metrics().numKeys, 3);
}

} // namespace gluten
