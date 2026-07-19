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
#include "state/VeloxNativeStreamingDeduplicate.h"
#include "utils/Exception.h"

#include <cstring>
#include <limits>
#include <utility>

namespace gluten {
namespace {

StateBytes bytes(std::initializer_list<uint8_t> values) {
  return StateBytes(values);
}

VeloxNativeStateStore reopen(int64_t version, const StateBytes& snapshot) {
  return VeloxNativeStateStore(version, snapshot.data(), static_cast<int32_t>(snapshot.size()));
}

NativeStreamingDeduplicateWatermarkInput watermarkRow(StateBytes key, int64_t eventTimeMicros) {
  return NativeStreamingDeduplicateWatermarkInput{std::move(key), eventTimeMicros};
}

int64_t unsafeRowLongValue(const StateBytes& bytes) {
  EXPECT_EQ(bytes.size(), 16);
  int64_t nullBits = 0;
  int64_t value = 0;
  std::memcpy(&nullBits, bytes.data(), sizeof(nullBits));
  std::memcpy(&value, bytes.data() + sizeof(nullBits), sizeof(value));
  EXPECT_EQ(nullBits, 0);
  return value;
}

} // namespace

TEST(VeloxNativeStreamingDeduplicateTest, deduplicateEmitsFirstSeenKeysAndReplaysCheckpoint) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingDeduplicate deduplicate(store);

  const auto first = deduplicate.deduplicate({bytes({1}), bytes({1}), bytes({2})});
  EXPECT_EQ(first.outputRowIndices, std::vector<size_t>({0, 2}));
  EXPECT_EQ(first.metrics.numInputRows, 3);
  EXPECT_EQ(first.metrics.numOutputRows, 2);
  EXPECT_EQ(first.metrics.numUpdatedStateRows, 2);
  EXPECT_EQ(first.metrics.numDroppedDuplicateRows, 1);
  EXPECT_EQ(first.metrics.numTotalStateRows, 2);

  auto replayedStore = reopen(1, store.commit());
  VeloxNativeStreamingDeduplicate replayedDeduplicate(replayedStore);

  const auto second = replayedDeduplicate.deduplicate({bytes({1}), bytes({3}), bytes({2})});
  EXPECT_EQ(second.outputRowIndices, std::vector<size_t>({1}));
  EXPECT_EQ(second.metrics.numInputRows, 3);
  EXPECT_EQ(second.metrics.numOutputRows, 1);
  EXPECT_EQ(second.metrics.numUpdatedStateRows, 1);
  EXPECT_EQ(second.metrics.numDroppedDuplicateRows, 2);
  EXPECT_EQ(second.metrics.numTotalStateRows, 3);
}

TEST(VeloxNativeStreamingDeduplicateTest, withinWatermarkFiltersLateRowsSuppressesDuplicatesAndEvicts) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingDeduplicate deduplicate(store);

  const auto first = deduplicate.deduplicateWithinWatermark(
      {
          watermarkRow(bytes({1}), 1'000),
          watermarkRow(bytes({1}), 2'000),
          watermarkRow(bytes({2}), 3'000),
          watermarkRow(bytes({3}), 500),
      },
      900,
      10'000,
      12'000);

  EXPECT_EQ(first.outputRowIndices, std::vector<size_t>({0, 2}));
  EXPECT_EQ(first.metrics.numInputRows, 4);
  EXPECT_EQ(first.metrics.numOutputRows, 2);
  EXPECT_EQ(first.metrics.numUpdatedStateRows, 2);
  EXPECT_EQ(first.metrics.numDroppedDuplicateRows, 1);
  EXPECT_EQ(first.metrics.numRowsDroppedByWatermark, 1);
  EXPECT_EQ(first.metrics.numRemovedStateRows, 1);
  EXPECT_EQ(first.metrics.numTotalStateRows, 1);
  EXPECT_EQ(first.metrics.stateMemoryBytes, 17);
  EXPECT_EQ(unsafeRowLongValue(store.get(bytes({2})).value()), 13'000);

  auto replayedStore = reopen(1, store.commit());
  VeloxNativeStreamingDeduplicate replayedDeduplicate(replayedStore);
  const auto second = replayedDeduplicate.deduplicateWithinWatermark(
      {
          watermarkRow(bytes({1}), 25'000),
          watermarkRow(bytes({2}), 26'000),
      },
      900,
      10'000,
      std::nullopt);

  EXPECT_EQ(second.outputRowIndices, std::vector<size_t>({0}));
  EXPECT_EQ(second.metrics.numDroppedDuplicateRows, 1);
  EXPECT_EQ(second.metrics.numRowsDroppedByWatermark, 0);
  EXPECT_EQ(second.metrics.numTotalStateRows, 2);
}

TEST(VeloxNativeStreamingDeduplicateTest, withinWatermarkEvictsAfterCurrentBatchDuplicateCheck) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingDeduplicate deduplicate(store);

  const auto initial = deduplicate.deduplicateWithinWatermark(
      {watermarkRow(bytes({1}), 1'000)},
      std::nullopt,
      10'000,
      std::nullopt);
  EXPECT_EQ(initial.outputRowIndices, std::vector<size_t>({0}));
  EXPECT_EQ(initial.metrics.numTotalStateRows, 1);

  auto replayedStore = reopen(1, store.commit());
  VeloxNativeStreamingDeduplicate replayedDeduplicate(replayedStore);

  const auto duplicateThenEvict = replayedDeduplicate.deduplicateWithinWatermark(
      {watermarkRow(bytes({1}), 20'000)},
      0,
      10'000,
      12'000);
  EXPECT_TRUE(duplicateThenEvict.outputRowIndices.empty());
  EXPECT_EQ(duplicateThenEvict.metrics.numDroppedDuplicateRows, 1);
  EXPECT_EQ(duplicateThenEvict.metrics.numRemovedStateRows, 1);
  EXPECT_EQ(duplicateThenEvict.metrics.numTotalStateRows, 0);

  auto afterEvictionStore = reopen(2, replayedStore.commit());
  VeloxNativeStreamingDeduplicate afterEvictionDeduplicate(afterEvictionStore);
  const auto emittedAgain = afterEvictionDeduplicate.deduplicateWithinWatermark(
      {watermarkRow(bytes({1}), 25'000)},
      0,
      10'000,
      std::nullopt);
  EXPECT_EQ(emittedAgain.outputRowIndices, std::vector<size_t>({0}));
  EXPECT_EQ(emittedAgain.metrics.numDroppedDuplicateRows, 0);
  EXPECT_EQ(emittedAgain.metrics.numTotalStateRows, 1);
}

TEST(VeloxNativeStreamingDeduplicateTest, withinWatermarkFailsClosedOnCorruptStateValue) {
  VeloxNativeStateStore store(0, nullptr, 0);
  store.put(bytes({1}), bytes({0, 1, 2}));
  VeloxNativeStreamingDeduplicate deduplicate(store);

  EXPECT_THROW(
      deduplicate.deduplicateWithinWatermark(
          {},
          std::nullopt,
          10'000,
          12'000),
      GlutenException);
}

TEST(VeloxNativeStreamingDeduplicateTest, withinWatermarkRejectsNegativeDelayAndOverflow) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingDeduplicate deduplicate(store);

  EXPECT_THROW(
      deduplicate.deduplicateWithinWatermark(
          {watermarkRow(bytes({1}), 1'000)},
          std::nullopt,
          -1,
          std::nullopt),
      GlutenException);

  EXPECT_THROW(
      deduplicate.deduplicateWithinWatermark(
          {watermarkRow(bytes({2}), std::numeric_limits<int64_t>::max())},
          std::nullopt,
          1,
          std::nullopt),
      GlutenException);
}

} // namespace gluten
