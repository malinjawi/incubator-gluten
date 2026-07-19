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
#include "state/VeloxNativeStreamingAggregation.h"
#include "utils/Exception.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <cstring>
#include <limits>
#include <optional>

namespace gluten {
namespace {

StateBytes bytes(std::initializer_list<uint8_t> values) {
  return StateBytes(values);
}

void appendInt64(StateBytes& bytes, int64_t value) {
  uint64_t raw = 0;
  std::memcpy(&raw, &value, sizeof(value));
  for (auto offset = 0; offset < 64; offset += 8) {
    bytes.push_back(static_cast<uint8_t>((raw >> offset) & 0xff));
  }
}

StateBytes unsafeLong(int64_t value) {
  StateBytes bytes;
  appendInt64(bytes, 0);
  appendInt64(bytes, value);
  return bytes;
}

int64_t readUnsafeLong(const StateBytes& bytes) {
  EXPECT_EQ(bytes.size(), sizeof(int64_t) * 2);
  int64_t value = 0;
  uint64_t raw = 0;
  for (size_t i = 0; i < sizeof(int64_t); ++i) {
    raw |= static_cast<uint64_t>(bytes[sizeof(int64_t) + i]) << (i * 8);
  }
  std::memcpy(&value, &raw, sizeof(value));
  return value;
}

StateBytes unsafeRowBytes(facebook::velox::row::UnsafeRowFast& unsafeRow, int32_t row) {
  const auto rowSize = unsafeRow.rowSize(row);
  StateBytes bytes(static_cast<size_t>(rowSize));
  unsafeRow.serialize(row, reinterpret_cast<char*>(bytes.data()));
  return bytes;
}

VeloxNativeStateStore reopen(int64_t version, const StateBytes& snapshot) {
  return VeloxNativeStateStore(version, snapshot.data(), static_cast<int32_t>(snapshot.size()));
}

} // namespace

class VeloxNativeStreamingAggregationVectorTest : public ::testing::Test,
                                                  public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance(
        facebook::velox::memory::MemoryManager::Options{});
  }
};

TEST(VeloxNativeStreamingAggregationTest, incrementCountUpdatesAndReplaysUnsafeRowLongState) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  const auto first = aggregation.incrementCount({bytes({1}), bytes({1}), bytes({2})});
  EXPECT_EQ(first.numInputRows, 3);
  EXPECT_EQ(first.numOutputRows, 2);
  EXPECT_EQ(first.numUpdatedStateRows, 3);
  EXPECT_EQ(first.numTotalStateRows, 2);
  EXPECT_EQ(readUnsafeLong(store.get(bytes({1})).value()), 2);
  EXPECT_EQ(readUnsafeLong(store.get(bytes({2})).value()), 1);

  auto reopened = reopen(1, store.commit());
  VeloxNativeStreamingAggregation replayedAggregation(reopened);
  const auto replayed = replayedAggregation.incrementCount({bytes({1}), bytes({3})});
  EXPECT_EQ(replayed.numInputRows, 2);
  EXPECT_EQ(replayed.numOutputRows, 2);
  EXPECT_EQ(replayed.numUpdatedStateRows, 2);
  EXPECT_EQ(replayed.numTotalStateRows, 3);
  EXPECT_EQ(readUnsafeLong(reopened.get(bytes({1})).value()), 3);
  EXPECT_EQ(readUnsafeLong(reopened.get(bytes({2})).value()), 1);
  EXPECT_EQ(readUnsafeLong(reopened.get(bytes({3})).value()), 1);
}

TEST(VeloxNativeStreamingAggregationTest, incrementCountAddsPartialAggregateDeltas) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  const auto first =
      aggregation.incrementCount({bytes({1}), bytes({2}), bytes({1})}, {10, 20, 5});
  EXPECT_EQ(first.numInputRows, 3);
  EXPECT_EQ(first.numOutputRows, 2);
  EXPECT_EQ(first.numUpdatedStateRows, 3);
  EXPECT_EQ(first.numTotalStateRows, 2);
  EXPECT_EQ(readUnsafeLong(store.get(bytes({1})).value()), 15);
  EXPECT_EQ(readUnsafeLong(store.get(bytes({2})).value()), 20);
}

TEST(VeloxNativeStreamingAggregationTest, incrementCountIgnoresZeroDeltas) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  const auto metrics = aggregation.incrementCount({bytes({1}), bytes({2})}, {0, 7});
  EXPECT_EQ(metrics.numInputRows, 2);
  EXPECT_EQ(metrics.numOutputRows, 1);
  EXPECT_EQ(metrics.numUpdatedStateRows, 1);
  EXPECT_EQ(metrics.numTotalStateRows, 1);
  EXPECT_FALSE(store.get(bytes({1})).has_value());
  EXPECT_EQ(readUnsafeLong(store.get(bytes({2})).value()), 7);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementCountDerivesUnsafeRowKeysFromColumnarInput) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  auto input = makeRowVector(
      {"shipmode", "priority", "payload"},
      {
          makeFlatVector<facebook::velox::StringView>({"MAIL", "MAIL", "RAIL", "RAIL", "MAIL"}),
          makeFlatVector<int64_t>({10, 10, 20, 20, 10}),
          makeFlatVector<int64_t>({100, 101, 102, 103, 104}),
      });

  const auto metrics = aggregation.incrementCount(input, {0, 1});
  EXPECT_EQ(metrics.numInputRows, 5);
  EXPECT_EQ(metrics.numOutputRows, 2);
  EXPECT_EQ(metrics.numUpdatedStateRows, 5);
  EXPECT_EQ(metrics.numTotalStateRows, 2);

  auto expectedKeys = makeRowVector(
      {"shipmode", "priority"},
      {
          makeFlatVector<facebook::velox::StringView>({"MAIL", "RAIL"}),
          makeFlatVector<int64_t>({10, 20}),
      });
  facebook::velox::row::UnsafeRowFast unsafeRow(expectedKeys);
  EXPECT_EQ(readUnsafeLong(store.get(unsafeRowBytes(unsafeRow, 0)).value()), 3);
  EXPECT_EQ(readUnsafeLong(store.get(unsafeRowBytes(unsafeRow, 1)).value()), 2);

  auto reopened = reopen(1, store.commit());
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 0)).value()), 3);
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 1)).value()), 2);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementCountFromColumnarInputRejectsBadKeyOrdinal) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);
  auto input = makeRowVector({"key"}, {makeFlatVector<int64_t>({1, 2, 3})});
  EXPECT_THROW(aggregation.incrementCount(input, {1}), GlutenException);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementCountFromColumnarInputReturnsUpdatedKeys) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  auto input = makeRowVector(
      {"shipmode", "priority", "payload"},
      {
          makeFlatVector<facebook::velox::StringView>({"MAIL", "MAIL", "RAIL", "MAIL"}),
          makeFlatVector<int64_t>({10, 10, 20, 10}),
          makeFlatVector<int64_t>({100, 101, 102, 103}),
      });

  const auto result = aggregation.incrementCountAndReturnKeys(input, {0, 1});
  EXPECT_EQ(result.metrics.numInputRows, 4);
  EXPECT_EQ(result.metrics.numOutputRows, 2);
  EXPECT_EQ(result.metrics.numUpdatedStateRows, 4);
  EXPECT_EQ(result.metrics.numTotalStateRows, 2);
  ASSERT_EQ(result.updatedKeys.size(), 2);

  auto expectedKeys = makeRowVector(
      {"shipmode", "priority"},
      {
          makeFlatVector<facebook::velox::StringView>({"MAIL", "RAIL"}),
          makeFlatVector<int64_t>({10, 20}),
      });
  facebook::velox::row::UnsafeRowFast unsafeRow(expectedKeys);
  EXPECT_EQ(result.updatedKeys[0], unsafeRowBytes(unsafeRow, 0));
  EXPECT_EQ(result.updatedKeys[1], unsafeRowBytes(unsafeRow, 1));
  ASSERT_EQ(result.updatedValues.size(), 2);
  EXPECT_EQ(readUnsafeLong(result.updatedValues[0]), 3);
  EXPECT_EQ(readUnsafeLong(result.updatedValues[1]), 1);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[0]).value()), 3);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[1]).value()), 1);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementLongSumDerivesUnsafeRowKeysAndDeltasFromColumnarInput) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  auto input = makeRowVector(
      {"shipmode", "priority", "revenue_delta", "payload"},
      {
          makeFlatVector<facebook::velox::StringView>({"MAIL", "MAIL", "RAIL", "MAIL", "RAIL"}),
          makeFlatVector<int64_t>({10, 10, 20, 10, 20}),
          makeFlatVector<int64_t>({100, -25, 7, 0, 3}),
          makeFlatVector<int64_t>({1000, 1001, 1002, 1003, 1004}),
      });

  const auto metrics = aggregation.incrementLongSum(input, {0, 1}, 2);
  EXPECT_EQ(metrics.numInputRows, 5);
  EXPECT_EQ(metrics.numOutputRows, 2);
  EXPECT_EQ(metrics.numUpdatedStateRows, 5);
  EXPECT_EQ(metrics.numTotalStateRows, 2);

  auto expectedKeys = makeRowVector(
      {"shipmode", "priority"},
      {
          makeFlatVector<facebook::velox::StringView>({"MAIL", "RAIL"}),
          makeFlatVector<int64_t>({10, 20}),
      });
  facebook::velox::row::UnsafeRowFast unsafeRow(expectedKeys);
  EXPECT_EQ(readUnsafeLong(store.get(unsafeRowBytes(unsafeRow, 0)).value()), 75);
  EXPECT_EQ(readUnsafeLong(store.get(unsafeRowBytes(unsafeRow, 1)).value()), 10);

  auto reopened = reopen(1, store.commit());
  VeloxNativeStreamingAggregation replayedAggregation(reopened);
  const auto replayed = replayedAggregation.incrementLongSum(input, {0, 1}, 2);
  EXPECT_EQ(replayed.numInputRows, 5);
  EXPECT_EQ(replayed.numOutputRows, 2);
  EXPECT_EQ(replayed.numUpdatedStateRows, 5);
  EXPECT_EQ(replayed.numTotalStateRows, 2);
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 0)).value()), 150);
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 1)).value()), 20);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementLongSumFromColumnarInputReturnsUpdatedKeys) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  auto input = makeRowVector(
      {"shipmode", "priority", "revenue_delta", "payload"},
      {
          makeFlatVector<facebook::velox::StringView>({"MAIL", "MAIL", "RAIL", "MAIL", "RAIL"}),
          makeFlatVector<int64_t>({10, 10, 20, 10, 20}),
          makeFlatVector<int64_t>({100, -25, 7, 0, 3}),
          makeFlatVector<int64_t>({1000, 1001, 1002, 1003, 1004}),
      });

  const auto result = aggregation.incrementLongSumAndReturnKeys(input, {0, 1}, 2);
  EXPECT_EQ(result.metrics.numInputRows, 5);
  EXPECT_EQ(result.metrics.numOutputRows, 2);
  EXPECT_EQ(result.metrics.numUpdatedStateRows, 5);
  EXPECT_EQ(result.metrics.numTotalStateRows, 2);
  ASSERT_EQ(result.updatedKeys.size(), 2);

  auto expectedKeys = makeRowVector(
      {"shipmode", "priority"},
      {
          makeFlatVector<facebook::velox::StringView>({"MAIL", "RAIL"}),
          makeFlatVector<int64_t>({10, 20}),
      });
  facebook::velox::row::UnsafeRowFast unsafeRow(expectedKeys);
  EXPECT_EQ(result.updatedKeys[0], unsafeRowBytes(unsafeRow, 0));
  EXPECT_EQ(result.updatedKeys[1], unsafeRowBytes(unsafeRow, 1));
  ASSERT_EQ(result.updatedValues.size(), 2);
  EXPECT_EQ(readUnsafeLong(result.updatedValues[0]), 75);
  EXPECT_EQ(readUnsafeLong(result.updatedValues[1]), 10);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[0]).value()), 75);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[1]).value()), 10);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementLongSumFromColumnarInputRejectsBadOrdinalsAndType) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);
  auto input = makeRowVector(
      {"key", "revenue_delta", "bad_delta"},
      {
          makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVector<int64_t>({10, 20, 30}),
          makeFlatVector<int32_t>({10, 20, 30}),
      });
  EXPECT_THROW(aggregation.incrementLongSum(input, {3}, 1), GlutenException);
  EXPECT_THROW(aggregation.incrementLongSum(input, {0}, 3), GlutenException);
  EXPECT_THROW(aggregation.incrementLongSum(input, {0}, 2), GlutenException);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementCountSingleInt64KeyUsesTypedPathAndSparkUnsafeRowBytes) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  auto input = makeRowVector(
      {"orderkey", "payload"},
      {
          makeNullableFlatVector<int64_t>({10, 10, std::nullopt, std::nullopt, 20}),
          makeFlatVector<int64_t>({100, 101, 102, 103, 104}),
      });

  const auto result = aggregation.incrementCountSingleInt64KeyAndReturnKeys(input, 0);
  EXPECT_EQ(result.metrics.numInputRows, 5);
  EXPECT_EQ(result.metrics.numOutputRows, 3);
  EXPECT_EQ(result.metrics.numUpdatedStateRows, 5);
  EXPECT_EQ(result.metrics.numTotalStateRows, 3);
  ASSERT_EQ(result.updatedKeys.size(), 3);
  ASSERT_EQ(result.updatedValues.size(), 3);

  auto expectedKeys = makeRowVector(
      {"orderkey"},
      {
          makeNullableFlatVector<int64_t>({10, std::nullopt, 20}),
      });
  facebook::velox::row::UnsafeRowFast unsafeRow(expectedKeys);
  EXPECT_EQ(result.updatedKeys[0], unsafeRowBytes(unsafeRow, 0));
  EXPECT_EQ(result.updatedKeys[1], unsafeRowBytes(unsafeRow, 1));
  EXPECT_EQ(result.updatedKeys[2], unsafeRowBytes(unsafeRow, 2));
  EXPECT_EQ(readUnsafeLong(result.updatedValues[0]), 2);
  EXPECT_EQ(readUnsafeLong(result.updatedValues[1]), 2);
  EXPECT_EQ(readUnsafeLong(result.updatedValues[2]), 1);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[0]).value()), 2);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[1]).value()), 2);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[2]).value()), 1);

  auto reopened = reopen(1, store.commit());
  EXPECT_EQ(readUnsafeLong(reopened.get(result.updatedKeys[0]).value()), 2);
  EXPECT_EQ(readUnsafeLong(reopened.get(result.updatedKeys[1]).value()), 2);
  EXPECT_EQ(readUnsafeLong(reopened.get(result.updatedKeys[2]).value()), 1);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementCountSingleInt64KeyUsesTypedMetricsOnlyPathAndReplays) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  auto input = makeRowVector(
      {"orderkey", "payload"},
      {
          makeNullableFlatVector<int64_t>({10, 10, std::nullopt, std::nullopt, 20}),
          makeFlatVector<int64_t>({100, 101, 102, 103, 104}),
      });

  const auto metrics = aggregation.incrementCountSingleInt64Key(input, 0);
  EXPECT_EQ(metrics.numInputRows, 5);
  EXPECT_EQ(metrics.numOutputRows, 3);
  EXPECT_EQ(metrics.numUpdatedStateRows, 5);
  EXPECT_EQ(metrics.numTotalStateRows, 3);

  auto expectedKeys = makeRowVector(
      {"orderkey"},
      {
          makeNullableFlatVector<int64_t>({10, std::nullopt, 20}),
      });
  facebook::velox::row::UnsafeRowFast unsafeRow(expectedKeys);
  EXPECT_EQ(readUnsafeLong(store.get(unsafeRowBytes(unsafeRow, 0)).value()), 2);
  EXPECT_EQ(readUnsafeLong(store.get(unsafeRowBytes(unsafeRow, 1)).value()), 2);
  EXPECT_EQ(readUnsafeLong(store.get(unsafeRowBytes(unsafeRow, 2)).value()), 1);

  auto reopened = reopen(1, store.commit());
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 0)).value()), 2);
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 1)).value()), 2);
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 2)).value()), 1);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementLongSumSingleInt64KeyUsesTypedPathAndSparkUnsafeRowBytes) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  auto input = makeRowVector(
      {"orderkey", "revenue_delta"},
      {
          makeNullableFlatVector<int64_t>({10, 10, std::nullopt, std::nullopt, 20, 30}),
          makeFlatVector<int64_t>({100, -25, 7, 3, 5, 0}),
      });

  const auto result = aggregation.incrementLongSumSingleInt64KeyAndReturnKeys(input, 0, 1);
  EXPECT_EQ(result.metrics.numInputRows, 6);
  EXPECT_EQ(result.metrics.numOutputRows, 4);
  EXPECT_EQ(result.metrics.numUpdatedStateRows, 6);
  EXPECT_EQ(result.metrics.numTotalStateRows, 4);
  ASSERT_EQ(result.updatedKeys.size(), 4);
  ASSERT_EQ(result.updatedValues.size(), 4);

  auto expectedKeys = makeRowVector(
      {"orderkey"},
      {
          makeNullableFlatVector<int64_t>({10, std::nullopt, 20, 30}),
      });
  facebook::velox::row::UnsafeRowFast unsafeRow(expectedKeys);
  EXPECT_EQ(result.updatedKeys[0], unsafeRowBytes(unsafeRow, 0));
  EXPECT_EQ(result.updatedKeys[1], unsafeRowBytes(unsafeRow, 1));
  EXPECT_EQ(result.updatedKeys[2], unsafeRowBytes(unsafeRow, 2));
  EXPECT_EQ(result.updatedKeys[3], unsafeRowBytes(unsafeRow, 3));
  EXPECT_EQ(readUnsafeLong(result.updatedValues[0]), 75);
  EXPECT_EQ(readUnsafeLong(result.updatedValues[1]), 10);
  EXPECT_EQ(readUnsafeLong(result.updatedValues[2]), 5);
  EXPECT_EQ(readUnsafeLong(result.updatedValues[3]), 0);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[0]).value()), 75);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[1]).value()), 10);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[2]).value()), 5);
  EXPECT_EQ(readUnsafeLong(store.get(result.updatedKeys[3]).value()), 0);

  auto reopened = reopen(1, store.commit());
  EXPECT_EQ(readUnsafeLong(reopened.get(result.updatedKeys[0]).value()), 75);
  EXPECT_EQ(readUnsafeLong(reopened.get(result.updatedKeys[1]).value()), 10);
  EXPECT_EQ(readUnsafeLong(reopened.get(result.updatedKeys[2]).value()), 5);
  EXPECT_EQ(readUnsafeLong(reopened.get(result.updatedKeys[3]).value()), 0);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementLongSumSingleInt64KeyTypedRowsReturnsPrimitiveKeysAndSums) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  auto input = makeRowVector(
      {"orderkey", "revenue_delta"},
      {
          makeNullableFlatVector<int64_t>({10, 10, std::nullopt, std::nullopt, 20, 30}),
          makeFlatVector<int64_t>({100, -25, 7, 3, 5, 0}),
      });

  const auto result =
      aggregation.incrementLongSumSingleInt64KeyAndReturnTypedRows(input, 0, 1);
  EXPECT_EQ(result.metrics.numInputRows, 6);
  EXPECT_EQ(result.metrics.numOutputRows, 4);
  EXPECT_EQ(result.metrics.numUpdatedStateRows, 6);
  EXPECT_EQ(result.metrics.numTotalStateRows, 4);

  // Non-null groups in first-observation order: 10 -> 75, 20 -> 5, 30 -> 0.
  ASSERT_EQ(result.keys.size(), 3);
  ASSERT_EQ(result.sums.size(), 3);
  EXPECT_EQ(result.keys[0], 10);
  EXPECT_EQ(result.sums[0], 75);
  EXPECT_EQ(result.keys[1], 20);
  EXPECT_EQ(result.sums[1], 5);
  EXPECT_EQ(result.keys[2], 30);
  EXPECT_EQ(result.sums[2], 0);
  // The single SQL NULL group: 7 + 3 = 10.
  EXPECT_TRUE(result.hasNullKey);
  EXPECT_EQ(result.nullKeySum, 10);

  // The resident map (and therefore the Spark checkpoint bytes) is byte-for-byte identical to the
  // byte[]-returning path: reopening from the committed snapshot yields the same UnsafeRow state.
  auto expectedKeys = makeRowVector(
      {"orderkey"},
      {
          makeNullableFlatVector<int64_t>({10, std::nullopt, 20, 30}),
      });
  facebook::velox::row::UnsafeRowFast unsafeRow(expectedKeys);
  auto reopened = reopen(1, store.commit());
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 0)).value()), 75);
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 1)).value()), 10);
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 2)).value()), 5);
  EXPECT_EQ(readUnsafeLong(reopened.get(unsafeRowBytes(unsafeRow, 3)).value()), 0);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementCountSingleInt64KeyTypedRowsReturnsPrimitiveKeysAndCounts) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  auto input = makeRowVector(
      {"orderkey"},
      {
          makeNullableFlatVector<int64_t>({10, 10, 20, std::nullopt}),
      });

  const auto result = aggregation.incrementCountSingleInt64KeyAndReturnTypedRows(input, 0);
  EXPECT_EQ(result.metrics.numInputRows, 4);
  EXPECT_EQ(result.metrics.numOutputRows, 3);
  ASSERT_EQ(result.keys.size(), 2);
  EXPECT_EQ(result.keys[0], 10);
  EXPECT_EQ(result.sums[0], 2);
  EXPECT_EQ(result.keys[1], 20);
  EXPECT_EQ(result.sums[1], 1);
  EXPECT_TRUE(result.hasNullKey);
  EXPECT_EQ(result.nullKeySum, 1);
}

TEST_F(
    VeloxNativeStreamingAggregationVectorTest,
    incrementLongSumFromColumnarInputRejectsNullValues) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);
  auto input = makeRowVector(
      {"key", "revenue_delta"},
      {
          makeFlatVector<int64_t>({1, 2, 3}),
          makeNullableFlatVector<int64_t>({10, std::nullopt, 30}),
      });
  EXPECT_THROW(aggregation.incrementLongSum(input, {0}, 1), GlutenException);
}

TEST(VeloxNativeStreamingAggregationTest, evictKeysRemovesCountStateAndReplaysSnapshot) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  aggregation.incrementCount({bytes({1}), bytes({2}), bytes({3})}, {10, 20, 30});
  const auto evicted = aggregation.evictKeys({bytes({2}), bytes({4}), bytes({2})});
  EXPECT_EQ(evicted.numInputRows, 3);
  EXPECT_EQ(evicted.numRemovedStateRows, 1);
  EXPECT_EQ(evicted.numTotalStateRows, 2);
  EXPECT_TRUE(store.get(bytes({1})).has_value());
  EXPECT_FALSE(store.get(bytes({2})).has_value());
  EXPECT_TRUE(store.get(bytes({3})).has_value());

  auto reopened = reopen(1, store.commit());
  EXPECT_EQ(readUnsafeLong(reopened.get(bytes({1})).value()), 10);
  EXPECT_FALSE(reopened.get(bytes({2})).has_value());
  EXPECT_EQ(readUnsafeLong(reopened.get(bytes({3})).value()), 30);
}

TEST(VeloxNativeStreamingAggregationTest, evictKeysReportsMemoryAfterLastKeyRemoved) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  aggregation.incrementCount({bytes({1})}, {5});
  const auto evicted = aggregation.evictKeys({bytes({1})});
  EXPECT_EQ(evicted.numInputRows, 1);
  EXPECT_EQ(evicted.numRemovedStateRows, 1);
  EXPECT_EQ(evicted.numTotalStateRows, 0);
  EXPECT_EQ(evicted.stateMemoryBytes, 0);
  EXPECT_FALSE(store.get(bytes({1})).has_value());
}

TEST(VeloxNativeStreamingAggregationTest, incrementCountRejectsCorruptStateValue) {
  VeloxNativeStateStore store(0, nullptr, 0);
  store.put(bytes({1}), bytes({1, 2, 3}));
  VeloxNativeStreamingAggregation aggregation(store);
  EXPECT_THROW(aggregation.incrementCount({bytes({1})}), GlutenException);
}

TEST(VeloxNativeStreamingAggregationTest, incrementCountRejectsNegativeDelta) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);
  EXPECT_THROW(aggregation.incrementCount({bytes({1})}, {-1}), GlutenException);
}

TEST(VeloxNativeStreamingAggregationTest, incrementCountRejectsOverflow) {
  VeloxNativeStateStore store(0, nullptr, 0);
  store.put(bytes({1}), unsafeLong(std::numeric_limits<int64_t>::max()));
  VeloxNativeStreamingAggregation aggregation(store);
  EXPECT_THROW(aggregation.incrementCount({bytes({1})}), GlutenException);
}

TEST(VeloxNativeStreamingAggregationTest, incrementLongSumAddsSignedPartialDeltasAndReplays) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  const auto first =
      aggregation.incrementLongSum({bytes({1}), bytes({2}), bytes({1})}, {10, -3, 5});
  EXPECT_EQ(first.numInputRows, 3);
  EXPECT_EQ(first.numOutputRows, 2);
  EXPECT_EQ(first.numUpdatedStateRows, 3);
  EXPECT_EQ(first.numTotalStateRows, 2);
  EXPECT_EQ(readUnsafeLong(store.get(bytes({1})).value()), 15);
  EXPECT_EQ(readUnsafeLong(store.get(bytes({2})).value()), -3);

  auto reopened = reopen(1, store.commit());
  VeloxNativeStreamingAggregation replayedAggregation(reopened);
  const auto replayed = replayedAggregation.incrementLongSum({bytes({1}), bytes({3})}, {-2, 7});
  EXPECT_EQ(replayed.numInputRows, 2);
  EXPECT_EQ(replayed.numOutputRows, 2);
  EXPECT_EQ(replayed.numUpdatedStateRows, 2);
  EXPECT_EQ(replayed.numTotalStateRows, 3);
  EXPECT_EQ(readUnsafeLong(reopened.get(bytes({1})).value()), 13);
  EXPECT_EQ(readUnsafeLong(reopened.get(bytes({2})).value()), -3);
  EXPECT_EQ(readUnsafeLong(reopened.get(bytes({3})).value()), 7);
}

TEST(VeloxNativeStreamingAggregationTest, incrementLongSumPreservesZeroDeltas) {
  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);

  const auto metrics = aggregation.incrementLongSum({bytes({1}), bytes({2})}, {0, -7});
  EXPECT_EQ(metrics.numInputRows, 2);
  EXPECT_EQ(metrics.numOutputRows, 2);
  EXPECT_EQ(metrics.numUpdatedStateRows, 2);
  EXPECT_EQ(metrics.numTotalStateRows, 2);
  EXPECT_EQ(readUnsafeLong(store.get(bytes({1})).value()), 0);
  EXPECT_EQ(readUnsafeLong(store.get(bytes({2})).value()), -7);
}

TEST(VeloxNativeStreamingAggregationTest, incrementLongSumRejectsOverflowAndUnderflow) {
  {
    VeloxNativeStateStore store(0, nullptr, 0);
    store.put(bytes({1}), unsafeLong(std::numeric_limits<int64_t>::max()));
    VeloxNativeStreamingAggregation aggregation(store);
    EXPECT_THROW(aggregation.incrementLongSum({bytes({1})}, {1}), GlutenException);
  }
  {
    VeloxNativeStateStore store(0, nullptr, 0);
    store.put(bytes({1}), unsafeLong(std::numeric_limits<int64_t>::min()));
    VeloxNativeStreamingAggregation aggregation(store);
    EXPECT_THROW(aggregation.incrementLongSum({bytes({1})}, {-1}), GlutenException);
  }
}

} // namespace gluten
