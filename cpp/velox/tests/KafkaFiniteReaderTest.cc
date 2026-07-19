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

#include "compute/kafka/KafkaConnector.h"
#include "compute/kafka/KafkaFiniteReader.h"
#include "compute/kafka/KafkaRdkafkaConsumer.h"
#include "compute/kafka/KafkaSplitReader.h"
#include "substrait/SubstraitToVeloxPlan.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/PrefixSortConfig.h"
#include "velox/common/base/SpillConfig.h"
#include "velox/common/config/Config.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/Connector.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace gluten::kafka {
namespace {

KafkaBytes bytes(std::initializer_list<uint8_t> values) {
  return KafkaBytes(values);
}

KafkaBytes bytes(facebook::velox::StringView value) {
  const auto* data = reinterpret_cast<const uint8_t*>(value.data());
  return KafkaBytes(data, data + value.size());
}

std::optional<std::string> envString(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || std::string(value).empty()) {
    return std::nullopt;
  }
  return std::string(value);
}

int64_t envInt64OrDefault(const char* name, int64_t defaultValue) {
  auto value = envString(name);
  if (!value.has_value()) {
    return defaultValue;
  }
  return std::stoll(value.value());
}

std::vector<std::string> splitCsv(const std::string& values) {
  std::vector<std::string> result;
  std::stringstream stream(values);
  std::string value;
  while (std::getline(stream, value, ',')) {
    if (!value.empty()) {
      result.push_back(value);
    }
  }
  return result;
}

bool startsWith(const std::string& value, const std::string& prefix) {
  return value.rfind(prefix, 0) == 0;
}

KafkaRecord record(int64_t offset) {
  return KafkaRecord{
      bytes({static_cast<uint8_t>(offset)}),
      bytes({static_cast<uint8_t>(offset + 10)}),
      "native-topic",
      2,
      offset,
      1000 + offset,
      0,
      {KafkaHeader{"header", bytes({1, 2, 3})}}};
}

KafkaPartitionRange range(int64_t startOffset, int64_t endOffset) {
  return KafkaPartitionRange{
      "native-topic",
      2,
      startOffset,
      endOffset,
      250,
      true,
      false,
      {{"bootstrap.servers", "localhost:9092"}}};
}

KafkaMicroBatchRangeRequest microBatchRequest(int64_t startOffset) {
  return KafkaMicroBatchRangeRequest{
      "native-topic",
      2,
      startOffset,
      std::nullopt,
      250,
      true,
      false,
      {{"bootstrap.servers", "localhost:9092"}}};
}

template <typename Func>
void expectVeloxExceptionMessageContains(Func&& action, const std::string& expected) {
  try {
    action();
    FAIL() << "Expected VeloxException containing '" << expected << "'";
  } catch (const facebook::velox::VeloxException& error) {
    ASSERT_TRUE(error.message().find(expected) != std::string::npos)
        << "Expected error message to contain '" << expected << "', but received '"
        << error.message() << "'.";
  }
}

class FakeKafkaConsumer final : public KafkaConsumer {
 public:
  explicit FakeKafkaConsumer(
      std::vector<std::optional<KafkaRecord>> records,
      std::shared_ptr<std::vector<std::tuple<std::string, int32_t, int64_t>>> sharedSeeks = nullptr,
      std::optional<std::pair<int64_t, int64_t>> watermarks = std::nullopt)
      : records_(std::move(records)), sharedSeeks_(std::move(sharedSeeks)), watermarks_(watermarks) {}

  void seek(const std::string& topic, int32_t partition, int64_t offset) override {
    seeks.emplace_back(topic, partition, offset);
    if (sharedSeeks_ != nullptr) {
      sharedSeeks_->emplace_back(topic, partition, offset);
    }
  }

  std::optional<KafkaRecord> poll(int64_t timeoutMs) override {
    pollTimeouts.push_back(timeoutMs);
    if (nextPoll_ >= records_.size()) {
      return std::nullopt;
    }
    return records_[nextPoll_++];
  }

  std::optional<std::pair<int64_t, int64_t>> watermarkOffsets(
      const std::string& topic,
      int32_t partition,
      int64_t timeoutMs) override {
    watermarkRequests.emplace_back(topic, partition, timeoutMs);
    return watermarks_;
  }

  std::vector<std::tuple<std::string, int32_t, int64_t>> seeks;
  std::vector<int64_t> pollTimeouts;
  std::vector<std::tuple<std::string, int32_t, int64_t>> watermarkRequests;

 private:
  std::vector<std::optional<KafkaRecord>> records_;
  std::shared_ptr<std::vector<std::tuple<std::string, int32_t, int64_t>>> sharedSeeks_;
  std::optional<std::pair<int64_t, int64_t>> watermarks_;
  size_t nextPoll_{0};
};

class FakeKafkaConsumerFactory final : public KafkaConsumerFactory {
 public:
  explicit FakeKafkaConsumerFactory(std::vector<std::optional<KafkaRecord>> records)
      : records_(std::move(records)) {}

  std::unique_ptr<KafkaConsumer> create(const KafkaPartitionRange& range) override {
    ranges.push_back(range);
    return std::make_unique<FakeKafkaConsumer>(records_, seeks);
  }

  std::vector<KafkaPartitionRange> ranges;
  std::shared_ptr<std::vector<std::tuple<std::string, int32_t, int64_t>>> seeks{
      std::make_shared<std::vector<std::tuple<std::string, int32_t, int64_t>>>()};

 private:
  std::vector<std::optional<KafkaRecord>> records_;
};

class ScopedKafkaConnector {
 public:
  ScopedKafkaConnector(std::string connectorId, std::shared_ptr<KafkaConsumerFactory> factory)
      : connectorId_(std::move(connectorId)) {
    if (facebook::velox::connector::hasConnector(connectorId_)) {
      facebook::velox::connector::unregisterConnector(connectorId_);
    }

    auto config = std::make_shared<facebook::velox::config::ConfigBase>(
        std::unordered_map<std::string, std::string>());
    VELOX_CHECK(facebook::velox::connector::registerConnector(
        std::make_shared<KafkaConnector>(connectorId_, std::move(config), std::move(factory))));
  }

  ~ScopedKafkaConnector() {
    facebook::velox::connector::unregisterConnector(connectorId_);
  }

 private:
  std::string connectorId_;
};

class KafkaFiniteReaderVectorTest : public ::testing::Test, public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance(facebook::velox::memory::MemoryManager::Options{});
  }

  std::unique_ptr<facebook::velox::connector::ConnectorQueryCtx> makeConnectorQueryCtx() {
    config_ = std::make_shared<facebook::velox::config::ConfigBase>(
        std::unordered_map<std::string, std::string>());
    spillConfig_ = std::make_unique<facebook::velox::common::SpillConfig>();
    return std::make_unique<facebook::velox::connector::ConnectorQueryCtx>(
        pool(),
        pool(),
        config_.get(),
        spillConfig_.get(),
        facebook::velox::common::PrefixSortConfig{},
        nullptr,
        nullptr,
        "native-kafka-query",
        "native-kafka-task",
        "native-kafka-scan",
        0,
        "");
  }

  std::shared_ptr<facebook::velox::core::TableScanNode> makeKafkaTableScanNode(
      const std::string& nodeId,
      const facebook::velox::RowTypePtr& outputType,
      const std::string& connectorId) {
    auto tableHandle = std::make_shared<KafkaTableHandle>(connectorId);
    facebook::velox::connector::ColumnHandleMap assignments;
    for (int32_t idx = 0; idx < outputType->size(); ++idx) {
      const auto& outputName = outputType->nameOf(idx);
      assignments[outputName] = std::make_shared<KafkaColumnHandle>(
          outputName, outputName, outputType->childAt(idx));
    }
    return std::make_shared<facebook::velox::core::TableScanNode>(
        nodeId, outputType, std::move(tableHandle), std::move(assignments));
  }

  std::vector<int64_t> readOffsetsFromKafkaTask(
      const std::string& taskId,
      const std::string& connectorId,
      const KafkaSplitInfo& splitInfo) {
    auto outputType = facebook::velox::ROW(
        {"value", "offset"},
        {facebook::velox::VARBINARY(), facebook::velox::BIGINT()});
    auto scanNode = makeKafkaTableScanNode(taskId + "-scan", outputType, connectorId);

    auto queryCtx = facebook::velox::core::QueryCtx::create();
    auto task = facebook::velox::exec::Task::create(
        taskId,
        facebook::velox::core::PlanFragment{scanNode},
        0,
        queryCtx,
        facebook::velox::exec::Task::ExecutionMode::kSerial);

    task->addSplit(
        scanNode->id(),
        facebook::velox::exec::Split{
            std::make_shared<KafkaConnectorSplit>(connectorId, splitInfo)});
    task->noMoreSplits(scanNode->id());

    std::vector<int64_t> observed;
    facebook::velox::ContinueFuture future = facebook::velox::ContinueFuture::makeEmpty();
    while (auto batch = task->next(&future)) {
      VELOX_CHECK_EQ(batch->childrenSize(), 2);
      const auto* values =
          batch->childAt(0)->as<facebook::velox::FlatVector<facebook::velox::StringView>>();
      const auto* offsets = batch->childAt(1)->as<facebook::velox::FlatVector<int64_t>>();
      VELOX_CHECK_NOT_NULL(values);
      VELOX_CHECK_NOT_NULL(offsets);
      for (auto i = 0; i < batch->size(); ++i) {
        EXPECT_EQ(
            bytes({static_cast<uint8_t>(offsets->valueAt(i) + 10)}),
            bytes(values->valueAt(i)));
        observed.push_back(offsets->valueAt(i));
      }
    }
    return observed;
  }

 private:
  std::shared_ptr<facebook::velox::config::ConfigBase> config_;
  std::unique_ptr<facebook::velox::common::SpillConfig> spillConfig_;
};

} // namespace

TEST(KafkaFiniteReaderTest, emptyRangeDoesNotSeekOrPoll) {
  FakeKafkaConsumer consumer({record(0)});
  KafkaFiniteReader reader(range(5, 5), &consumer);

  const auto records = reader.readAll();

  EXPECT_TRUE(records.empty());
  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, rejectsInvalidRangeMetadataBeforeSeek) {
  FakeKafkaConsumer consumer({record(0)});

  auto emptyTopic = range(5, 6);
  emptyTopic.topic = "";
  EXPECT_THROW(KafkaFiniteReader(emptyTopic, &consumer), facebook::velox::VeloxException);

  auto negativePartition = range(5, 6);
  negativePartition.partition = -1;
  EXPECT_THROW(KafkaFiniteReader(negativePartition, &consumer), facebook::velox::VeloxException);

  auto negativePollTimeout = range(5, 6);
  negativePollTimeout.pollTimeoutMs = -1;
  EXPECT_THROW(KafkaFiniteReader(negativePollTimeout, &consumer), facebook::velox::VeloxException);

  auto negativeStartOffset = range(-1, 6);
  EXPECT_THROW(KafkaFiniteReader(negativeStartOffset, &consumer), facebook::velox::VeloxException);

  auto reversedOffsets = range(6, 5);
  EXPECT_THROW(KafkaFiniteReader(reversedOffsets, &consumer), facebook::velox::VeloxException);

  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, plansNativeMicroBatchRangeFromBrokerWatermarks) {
  FakeKafkaConsumer consumer({}, nullptr, std::make_pair<int64_t, int64_t>(3, 10));
  auto request = microBatchRequest(5);
  request.maxOffsetsPerTrigger = 2;

  const auto plan = planKafkaMicroBatchRange(request, &consumer);

  EXPECT_EQ("native-topic", plan.range.topic);
  EXPECT_EQ(2, plan.range.partition);
  EXPECT_EQ(5, plan.range.startOffset);
  EXPECT_EQ(7, plan.range.endOffset);
  EXPECT_EQ(250, plan.range.pollTimeoutMs);
  EXPECT_TRUE(plan.range.failOnDataLoss);
  EXPECT_FALSE(plan.range.includeHeaders);
  EXPECT_EQ("localhost:9092", plan.range.params.at("bootstrap.servers"));
  EXPECT_EQ(3, plan.lowWatermark);
  EXPECT_EQ(10, plan.highWatermark);
  EXPECT_EQ(0, plan.skippedMessages);
  EXPECT_FALSE(plan.advancedStartOffset);
  EXPECT_FALSE(plan.resetStartToHighWatermark);
  EXPECT_TRUE(plan.limitedByMaxOffsetsPerTrigger);
  std::vector<std::tuple<std::string, int32_t, int64_t>> expectedWatermarkRequests{
      std::make_tuple(std::string("native-topic"), 2, 250)};
  EXPECT_EQ(expectedWatermarkRequests, consumer.watermarkRequests);
  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, plansEmptyNativeMicroBatchAtHighWatermark) {
  FakeKafkaConsumer consumer({}, nullptr, std::make_pair<int64_t, int64_t>(3, 10));
  const auto plan = planKafkaMicroBatchRange(microBatchRequest(10), &consumer);

  EXPECT_EQ(10, plan.range.startOffset);
  EXPECT_EQ(10, plan.range.endOffset);
  EXPECT_FALSE(plan.limitedByMaxOffsetsPerTrigger);
  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, nativeMicroBatchRangeAllowsZeroMaxOffsetsLimit) {
  FakeKafkaConsumer consumer({}, nullptr, std::make_pair<int64_t, int64_t>(3, 10));
  auto request = microBatchRequest(5);
  request.maxOffsetsPerTrigger = 0;

  const auto plan = planKafkaMicroBatchRange(request, &consumer);

  EXPECT_EQ(5, plan.range.startOffset);
  EXPECT_EQ(5, plan.range.endOffset);
  EXPECT_TRUE(plan.limitedByMaxOffsetsPerTrigger);
}

TEST(KafkaFiniteReaderTest, nativeMicroBatchRangeAdvancesDeletedStartWhenDataLossIsAllowed) {
  FakeKafkaConsumer consumer({}, nullptr, std::make_pair<int64_t, int64_t>(5, 9));
  auto request = microBatchRequest(2);
  request.failOnDataLoss = false;

  const auto plan = planKafkaMicroBatchRange(request, &consumer);

  EXPECT_EQ(5, plan.range.startOffset);
  EXPECT_EQ(9, plan.range.endOffset);
  EXPECT_EQ(3, plan.skippedMessages);
  EXPECT_TRUE(plan.advancedStartOffset);
  EXPECT_FALSE(plan.resetStartToHighWatermark);
}

TEST(KafkaFiniteReaderTest, nativeMicroBatchRangeRejectsDeletedStartWhenFailOnDataLossIsTrue) {
  FakeKafkaConsumer consumer({}, nullptr, std::make_pair<int64_t, int64_t>(5, 9));

  expectVeloxExceptionMessageContains(
      [&]() { planKafkaMicroBatchRange(microBatchRequest(2), &consumer); },
      "Native Kafka source data-loss offset-planning failure");

  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, nativeMicroBatchRangeResetsStartPastHighWhenDataLossIsAllowed) {
  FakeKafkaConsumer consumer({}, nullptr, std::make_pair<int64_t, int64_t>(5, 9));
  auto request = microBatchRequest(12);
  request.failOnDataLoss = false;

  const auto plan = planKafkaMicroBatchRange(request, &consumer);

  EXPECT_EQ(9, plan.range.startOffset);
  EXPECT_EQ(9, plan.range.endOffset);
  EXPECT_EQ(0, plan.skippedMessages);
  EXPECT_FALSE(plan.advancedStartOffset);
  EXPECT_TRUE(plan.resetStartToHighWatermark);
}

TEST(KafkaFiniteReaderTest, nativeMicroBatchRangeRejectsStartPastHighWhenFailOnDataLossIsTrue) {
  FakeKafkaConsumer consumer({}, nullptr, std::make_pair<int64_t, int64_t>(5, 9));

  expectVeloxExceptionMessageContains(
      [&]() { planKafkaMicroBatchRange(microBatchRequest(12), &consumer); },
      "Native Kafka source data-loss offset-planning failure");

  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, nativeMicroBatchRangeRequiresBrokerWatermarks) {
  FakeKafkaConsumer consumer({});

  expectVeloxExceptionMessageContains(
      [&]() { planKafkaMicroBatchRange(microBatchRequest(5), &consumer); },
      "Native Kafka source fatal offset-planning failure");

  ASSERT_EQ(1, consumer.watermarkRequests.size());
  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, nativeMicroBatchRangeRejectsInvalidWatermarks) {
  FakeKafkaConsumer consumer({}, nullptr, std::make_pair<int64_t, int64_t>(9, 5));

  expectVeloxExceptionMessageContains(
      [&]() { planKafkaMicroBatchRange(microBatchRequest(5), &consumer); },
      "Native Kafka reader fatal watermark lookup failure");

  ASSERT_EQ(1, consumer.watermarkRequests.size());
  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, readsFiniteRangeWithExclusiveEndOffset) {
  FakeKafkaConsumer consumer({record(5), record(6), record(7)});
  KafkaFiniteReader reader(range(5, 7), &consumer);

  const auto records = reader.readAll();

  ASSERT_EQ(2, records.size());
  EXPECT_EQ(5, records[0].offset);
  EXPECT_EQ(6, records[1].offset);
  ASSERT_EQ(1, consumer.seeks.size());
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, 5), consumer.seeks[0]);
  EXPECT_EQ(std::vector<int64_t>({250, 250}), consumer.pollTimeouts);
}

TEST(KafkaFiniteReaderTest, readsFiniteRangeInBoundedBatches) {
  FakeKafkaConsumer consumer({record(5), record(6), record(7)});
  KafkaFiniteReader reader(range(5, 8), &consumer);

  const auto first = reader.readNext(1);
  ASSERT_EQ(1, first.size());
  EXPECT_EQ(5, first[0].offset);
  EXPECT_FALSE(reader.done());

  const auto second = reader.readNext(2);
  ASSERT_EQ(2, second.size());
  EXPECT_EQ(6, second[0].offset);
  EXPECT_EQ(7, second[1].offset);
  EXPECT_TRUE(reader.done());

  const auto done = reader.readNext(2);
  EXPECT_TRUE(done.empty());
  ASSERT_EQ(1, consumer.seeks.size());
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, 5), consumer.seeks[0]);
  EXPECT_EQ(std::vector<int64_t>({250, 250, 250}), consumer.pollTimeouts);
}

TEST(KafkaFiniteReaderTest, reportsNativeConsumerAttemptCounters) {
  FakeKafkaConsumer consumer({record(5), record(6)});
  KafkaFiniteReader reader(range(5, 7), &consumer);

  EXPECT_EQ(0, reader.watermarkLookups());
  EXPECT_EQ(0, reader.seeks());
  EXPECT_EQ(0, reader.polls());

  const auto first = reader.readNext(1);
  ASSERT_EQ(1, first.size());
  EXPECT_EQ(1, reader.watermarkLookups());
  EXPECT_EQ(1, reader.seeks());
  EXPECT_EQ(1, reader.polls());

  const auto second = reader.readNext(1);
  ASSERT_EQ(1, second.size());
  EXPECT_EQ(1, reader.watermarkLookups());
  EXPECT_EQ(1, reader.seeks());
  EXPECT_EQ(2, reader.polls());
  EXPECT_TRUE(reader.done());
}

TEST(KafkaFiniteReaderTest, stripsHeadersUnlessRequested) {
  FakeKafkaConsumer withoutHeadersConsumer({record(5)});
  KafkaFiniteReader withoutHeaders(range(5, 6), &withoutHeadersConsumer);

  const auto withoutHeaderRecords = withoutHeaders.readAll();
  ASSERT_EQ(1, withoutHeaderRecords.size());
  EXPECT_TRUE(withoutHeaderRecords[0].headers.empty());

  auto withHeadersRange = range(5, 6);
  withHeadersRange.includeHeaders = true;
  FakeKafkaConsumer withHeadersConsumer({record(5)});
  KafkaFiniteReader withHeaders(withHeadersRange, &withHeadersConsumer);

  const auto withHeaderRecords = withHeaders.readAll();
  ASSERT_EQ(1, withHeaderRecords.size());
  ASSERT_EQ(1, withHeaderRecords[0].headers.size());
  EXPECT_EQ("header", withHeaderRecords[0].headers[0].key);
  ASSERT_TRUE(withHeaderRecords[0].headers[0].value.has_value());
  EXPECT_EQ(bytes({1, 2, 3}), withHeaderRecords[0].headers[0].value.value());
}

TEST(KafkaFiniteReaderTest, detectsOffsetGapWhenFailOnDataLossIsTrue) {
  FakeKafkaConsumer consumer({record(5), record(7)});
  KafkaFiniteReader reader(range(5, 8), &consumer);

  expectVeloxExceptionMessageContains(
      [&]() { reader.readAll(); }, "Native Kafka reader data-loss finite-range failure");
}

TEST(KafkaFiniteReaderTest, skipsOffsetGapWhenFailOnDataLossIsFalse) {
  auto readRange = range(5, 8);
  readRange.failOnDataLoss = false;
  FakeKafkaConsumer consumer({record(5), record(7)});
  KafkaFiniteReader reader(readRange, &consumer);

  const auto records = reader.readAll();

  ASSERT_EQ(2, records.size());
  EXPECT_EQ(5, records[0].offset);
  EXPECT_EQ(7, records[1].offset);
  EXPECT_EQ(1, reader.skippedMessages());
}

TEST(KafkaFiniteReaderTest, rejectsStartBeforeLowWatermarkWhenFailOnDataLossIsTrue) {
  FakeKafkaConsumer consumer({record(6)}, nullptr, std::make_pair<int64_t, int64_t>(6, 8));
  KafkaFiniteReader reader(range(5, 8), &consumer);

  EXPECT_THROW(reader.readAll(), facebook::velox::VeloxException);

  ASSERT_EQ(1, consumer.watermarkRequests.size());
  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, skipsToLowWatermarkWhenFailOnDataLossIsFalse) {
  auto readRange = range(5, 8);
  readRange.failOnDataLoss = false;
  FakeKafkaConsumer consumer(
      {record(6), record(7)}, nullptr, std::make_pair<int64_t, int64_t>(6, 8));
  KafkaFiniteReader reader(readRange, &consumer);

  const auto records = reader.readAll();

  ASSERT_EQ(1, consumer.watermarkRequests.size());
  ASSERT_EQ(1, consumer.seeks.size());
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, 6), consumer.seeks[0]);
  ASSERT_EQ(2, records.size());
  EXPECT_EQ(6, records[0].offset);
  EXPECT_EQ(7, records[1].offset);
  EXPECT_EQ(1, reader.skippedMessages());
}

TEST(KafkaFiniteReaderTest, skipsWholeRangeWhenLowWatermarkIsAfterEndAndDataLossIsAllowed) {
  auto readRange = range(5, 8);
  readRange.failOnDataLoss = false;
  FakeKafkaConsumer consumer({record(9)}, nullptr, std::make_pair<int64_t, int64_t>(9, 12));
  KafkaFiniteReader reader(readRange, &consumer);

  const auto records = reader.readAll();

  EXPECT_TRUE(records.empty());
  EXPECT_EQ(3, reader.skippedMessages());
  ASSERT_EQ(1, consumer.watermarkRequests.size());
  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, rejectsEndAfterHighWatermarkWhenFailOnDataLossIsTrue) {
  FakeKafkaConsumer consumer(
      {record(5), record(6)}, nullptr, std::make_pair<int64_t, int64_t>(5, 7));
  KafkaFiniteReader reader(range(5, 8), &consumer);

  expectVeloxExceptionMessageContains(
      [&]() { reader.readAll(); }, "Native Kafka reader data-loss finite-range failure");

  ASSERT_EQ(1, consumer.watermarkRequests.size());
  EXPECT_TRUE(consumer.seeks.empty());
  EXPECT_TRUE(consumer.pollTimeouts.empty());
}

TEST(KafkaFiniteReaderTest, rejectsInvalidWatermarkMetadataBeforeSeek) {
  auto assertInvalidWatermark = [](std::pair<int64_t, int64_t> watermarks) {
    FakeKafkaConsumer consumer({record(5)}, nullptr, watermarks);
    KafkaFiniteReader reader(range(5, 6), &consumer);

    expectVeloxExceptionMessageContains(
        [&]() { reader.readAll(); }, "Native Kafka reader fatal watermark lookup failure");

    ASSERT_EQ(1, consumer.watermarkRequests.size());
    EXPECT_TRUE(consumer.seeks.empty());
    EXPECT_TRUE(consumer.pollTimeouts.empty());
    EXPECT_EQ(1, reader.watermarkLookups());
    EXPECT_EQ(0, reader.seeks());
    EXPECT_EQ(0, reader.polls());
  };

  assertInvalidWatermark(std::make_pair<int64_t, int64_t>(-1, 6));
  assertInvalidWatermark(std::make_pair<int64_t, int64_t>(6, 5));
}

TEST(KafkaFiniteReaderTest, doesNotClampSparkPlannedEndWhenFailOnDataLossIsFalse) {
  auto readRange = range(5, 8);
  readRange.failOnDataLoss = false;
  FakeKafkaConsumer consumer(
      {record(5), record(6), std::nullopt}, nullptr, std::make_pair<int64_t, int64_t>(5, 7));
  KafkaFiniteReader reader(readRange, &consumer);

  expectVeloxExceptionMessageContains(
      [&]() { reader.readAll(); }, "Native Kafka reader timeout poll failure");

  ASSERT_EQ(1, consumer.watermarkRequests.size());
  ASSERT_EQ(1, consumer.seeks.size());
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, 5), consumer.seeks[0]);
  EXPECT_EQ(std::vector<int64_t>({250, 250, 250}), consumer.pollTimeouts);
}

TEST(KafkaFiniteReaderTest, rejectsPollTimeoutBeforeEndOffsetEvenWhenDataLossIsAllowed) {
  auto readRange = range(5, 8);
  readRange.failOnDataLoss = false;
  FakeKafkaConsumer consumer({record(5), std::nullopt});
  KafkaFiniteReader reader(readRange, &consumer);

  expectVeloxExceptionMessageContains(
      [&]() { reader.readAll(); }, "Native Kafka reader timeout poll failure");
}

TEST(KafkaFiniteReaderTest, rejectsUnexpectedTopicPartition) {
  auto unexpected = record(5);
  unexpected.partition = 3;
  FakeKafkaConsumer consumer({unexpected});
  KafkaFiniteReader reader(range(5, 6), &consumer);

  expectVeloxExceptionMessageContains(
      [&]() { reader.readAll(); }, "Native Kafka reader fatal finite-range failure");
}

TEST(KafkaFiniteReaderTest, rejectsNegativeRecordOffsetsBeforeStaleRecordSkip) {
  auto invalid = record(5);
  invalid.offset = -1;
  FakeKafkaConsumer consumer({invalid, record(5)});
  KafkaFiniteReader reader(range(5, 6), &consumer);

  expectVeloxExceptionMessageContains(
      [&]() { reader.readAll(); }, "received negative offset -1");

  ASSERT_EQ(1, consumer.seeks.size());
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, 5), consumer.seeks[0]);
  EXPECT_EQ(std::vector<int64_t>({250}), consumer.pollTimeouts);
}

TEST_F(KafkaFiniteReaderVectorTest, materializesSparkKafkaRowsWithoutHeaders) {
  auto first = record(5);
  first.key = std::nullopt;
  first.value = bytes({0, 1, 2, 3});
  first.timestamp = 123456;
  first.timestampType = 1;

  auto second = record(6);
  second.key = KafkaBytes{};
  second.value = std::nullopt;
  second.topic = "native-topic";
  second.partition = 2;
  second.timestamp = 123999;
  second.timestampType = 0;

  const auto batch = makeKafkaRowVector({first, second}, false, pool());

  ASSERT_EQ(2, batch->size());
  ASSERT_EQ(7, batch->childrenSize());
  EXPECT_TRUE(batch->type()->kindEquals(sparkKafkaRowType(false)));
  EXPECT_EQ(
      std::vector<std::string>({"key", "value", "topic", "partition", "offset", "timestamp", "timestampType"}),
      batch->type()->asRow().names());

  const auto* keys = batch->childAt(0)->as<facebook::velox::FlatVector<facebook::velox::StringView>>();
  const auto* values = batch->childAt(1)->as<facebook::velox::FlatVector<facebook::velox::StringView>>();
  const auto* topics = batch->childAt(2)->as<facebook::velox::FlatVector<facebook::velox::StringView>>();
  const auto* partitions = batch->childAt(3)->as<facebook::velox::FlatVector<int32_t>>();
  const auto* offsets = batch->childAt(4)->as<facebook::velox::FlatVector<int64_t>>();
  const auto* timestamps = batch->childAt(5)->as<facebook::velox::FlatVector<facebook::velox::Timestamp>>();
  const auto* timestampTypes = batch->childAt(6)->as<facebook::velox::FlatVector<int32_t>>();

  ASSERT_TRUE(keys->isNullAt(0));
  ASSERT_FALSE(keys->isNullAt(1));
  EXPECT_EQ(KafkaBytes({}), bytes(keys->valueAt(1)));
  EXPECT_EQ(bytes({0, 1, 2, 3}), bytes(values->valueAt(0)));
  EXPECT_TRUE(values->isNullAt(1));
  EXPECT_EQ("native-topic", topics->valueAt(0).getString());
  EXPECT_EQ("native-topic", topics->valueAt(1).getString());
  EXPECT_EQ(2, partitions->valueAt(0));
  EXPECT_EQ(2, partitions->valueAt(1));
  EXPECT_EQ(5, offsets->valueAt(0));
  EXPECT_EQ(6, offsets->valueAt(1));
  EXPECT_EQ(123456, timestamps->valueAt(0).toMillis());
  EXPECT_EQ(123999, timestamps->valueAt(1).toMillis());
  EXPECT_EQ(1, timestampTypes->valueAt(0));
  EXPECT_EQ(0, timestampTypes->valueAt(1));
}

TEST_F(KafkaFiniteReaderVectorTest, materializesSparkKafkaHeadersArray) {
  auto first = record(5);
  first.headers = {
      KafkaHeader{"first", bytes({1})},
      KafkaHeader{"null-value", std::nullopt},
  };
  auto second = record(6);
  second.headers = {KafkaHeader{"second", bytes({2, 3})}};

  const auto batch = makeKafkaRowVector({first, second}, true, pool());

  ASSERT_EQ(2, batch->size());
  ASSERT_EQ(8, batch->childrenSize());
  EXPECT_TRUE(batch->type()->kindEquals(sparkKafkaRowType(true)));
  EXPECT_EQ("headers", batch->type()->asRow().nameOf(7));

  const auto* headers = batch->childAt(7)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(nullptr, headers);
  EXPECT_EQ(0, headers->offsetAt(0));
  EXPECT_EQ(2, headers->sizeAt(0));
  EXPECT_EQ(2, headers->offsetAt(1));
  EXPECT_EQ(1, headers->sizeAt(1));

  const auto* headerRows = headers->elements()->as<facebook::velox::RowVector>();
  ASSERT_NE(nullptr, headerRows);
  ASSERT_EQ(3, headerRows->size());
  const auto* headerKeys = headerRows->childAt(0)->as<facebook::velox::FlatVector<facebook::velox::StringView>>();
  const auto* headerValues = headerRows->childAt(1)->as<facebook::velox::FlatVector<facebook::velox::StringView>>();

  EXPECT_EQ("first", headerKeys->valueAt(0).getString());
  EXPECT_EQ(bytes({1}), bytes(headerValues->valueAt(0)));
  EXPECT_EQ("null-value", headerKeys->valueAt(1).getString());
  EXPECT_TRUE(headerValues->isNullAt(1));
  EXPECT_EQ("second", headerKeys->valueAt(2).getString());
  EXPECT_EQ(bytes({2, 3}), bytes(headerValues->valueAt(2)));
}

TEST_F(KafkaFiniteReaderVectorTest, readsKafkaSplitInfoIntoSparkKafkaRows) {
  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 7;
  splitInfo.pollTimeoutMs = 1000;
  splitInfo.failOnDataLoss = true;
  splitInfo.includeHeaders = true;
  splitInfo.params = {{"bootstrap.servers", "localhost:9092"}};

  const auto range = kafkaPartitionRangeFromSplit(splitInfo);
  EXPECT_EQ("native-topic", range.topic);
  EXPECT_EQ(2, range.partition);
  EXPECT_EQ(5, range.startOffset);
  EXPECT_EQ(7, range.endOffset);
  EXPECT_EQ(1000, range.pollTimeoutMs);
  EXPECT_TRUE(range.failOnDataLoss);
  EXPECT_TRUE(range.includeHeaders);
  EXPECT_EQ("localhost:9092", range.params.at("bootstrap.servers"));

  FakeKafkaConsumer consumer({record(5), record(6)});
  const auto batch = readKafkaSplit(splitInfo, &consumer, pool());

  ASSERT_EQ(2, batch->size());
  ASSERT_EQ(8, batch->childrenSize());
  EXPECT_TRUE(batch->type()->kindEquals(sparkKafkaRowType(true)));
  const auto* offsets = batch->childAt(4)->as<facebook::velox::FlatVector<int64_t>>();
  EXPECT_EQ(5, offsets->valueAt(0));
  EXPECT_EQ(6, offsets->valueAt(1));
  const auto* headers = batch->childAt(7)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(nullptr, headers);
  EXPECT_EQ(1, headers->sizeAt(0));
  EXPECT_EQ(1, headers->sizeAt(1));
  ASSERT_EQ(1, consumer.seeks.size());
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, 5), consumer.seeks[0]);
  EXPECT_EQ(std::vector<int64_t>({1000, 1000}), consumer.pollTimeouts);
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceReadsProjectedSplitRows) {
  auto outputType = facebook::velox::ROW(
      {"projected_value", "projected_offset"},
      {facebook::velox::VARBINARY(), facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"projected_value",
       std::make_shared<KafkaColumnHandle>("projected_value", "value", facebook::velox::VARBINARY())},
      {"projected_offset",
       std::make_shared<KafkaColumnHandle>("projected_offset", "offset", facebook::velox::BIGINT())},
  };

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5), record(6)});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 7;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));
  auto future = facebook::velox::ContinueFuture::makeEmpty();
  auto batchResult = source.next(4096, future);

  ASSERT_TRUE(batchResult.has_value());
  auto batch = batchResult.value();
  ASSERT_NE(nullptr, batch);
  ASSERT_EQ(2, batch->size());
  ASSERT_EQ(2, batch->childrenSize());
  EXPECT_TRUE(batch->type()->kindEquals(outputType));

  const auto* values = batch->childAt(0)->as<facebook::velox::FlatVector<facebook::velox::StringView>>();
  const auto* offsets = batch->childAt(1)->as<facebook::velox::FlatVector<int64_t>>();
  EXPECT_EQ(bytes({15}), bytes(values->valueAt(0)));
  EXPECT_EQ(bytes({16}), bytes(values->valueAt(1)));
  EXPECT_EQ(5, offsets->valueAt(0));
  EXPECT_EQ(6, offsets->valueAt(1));

  ASSERT_EQ(1, factory->ranges.size());
  EXPECT_EQ("native-topic", factory->ranges[0].topic);
  EXPECT_EQ(2, factory->ranges[0].partition);
  EXPECT_EQ(5, factory->ranges[0].startOffset);
  EXPECT_EQ(7, factory->ranges[0].endOffset);
  EXPECT_EQ(123, factory->ranges[0].pollTimeoutMs);

  auto done = source.next(4096, future);
  ASSERT_TRUE(done.has_value());
  EXPECT_EQ(nullptr, done.value());
  EXPECT_EQ(2, source.getCompletedRows());
  const auto stats = source.getRuntimeStats();
  EXPECT_EQ(1, stats.at("startedSplits").sum);
  EXPECT_EQ(1, stats.at("completedSplits").sum);
  EXPECT_EQ(2, stats.at("plannedMessages").sum);
  EXPECT_EQ(2, stats.at("readMessages").sum);
  EXPECT_EQ(0, stats.at("skippedMessages").sum);
  EXPECT_EQ(1, stats.at("watermarkLookups").sum);
  EXPECT_EQ(1, stats.at("seeks").sum);
  EXPECT_EQ(2, stats.at("polls").sum);
  EXPECT_EQ(0, stats.at("emptySplits").sum);
  EXPECT_EQ(1, stats.at("consumerCreations").sum);
  EXPECT_EQ(1, stats.at("readBatches").sum);
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceProjectsCaseInsensitiveSparkColumns) {
  auto outputType = facebook::velox::ROW({"projected_timestamp_type"}, {facebook::velox::INTEGER()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"projected_timestamp_type",
       std::make_shared<KafkaColumnHandle>("projected_timestamp_type", "timestamptype", facebook::velox::INTEGER())},
  };

  auto input = record(5);
  input.timestampType = 1;
  auto factory = std::make_shared<FakeKafkaConsumerFactory>(std::vector<std::optional<KafkaRecord>>{input});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 6;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));
  auto future = facebook::velox::ContinueFuture::makeEmpty();
  auto batchResult = source.next(4096, future);

  ASSERT_TRUE(batchResult.has_value());
  auto batch = batchResult.value();
  ASSERT_NE(nullptr, batch);
  ASSERT_EQ(1, batch->size());

  const auto* timestampTypes = batch->childAt(0)->as<facebook::velox::FlatVector<int32_t>>();
  ASSERT_NE(nullptr, timestampTypes);
  EXPECT_EQ(1, timestampTypes->valueAt(0));
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceCompletesEmptySplitWithoutConsumer) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5)});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 5;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));
  auto future = facebook::velox::ContinueFuture::makeEmpty();
  auto result = source.next(4096, future);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(nullptr, result.value());
  EXPECT_TRUE(factory->ranges.empty());
  EXPECT_TRUE(factory->seeks->empty());
  EXPECT_EQ(0, source.getCompletedRows());
  const auto stats = source.getRuntimeStats();
  EXPECT_EQ(1, stats.at("startedSplits").sum);
  EXPECT_EQ(1, stats.at("completedSplits").sum);
  EXPECT_EQ(0, stats.at("plannedMessages").sum);
  EXPECT_EQ(0, stats.at("readMessages").sum);
  EXPECT_EQ(0, stats.at("skippedMessages").sum);
  EXPECT_EQ(0, stats.at("watermarkLookups").sum);
  EXPECT_EQ(0, stats.at("seeks").sum);
  EXPECT_EQ(0, stats.at("polls").sum);
  EXPECT_EQ(1, stats.at("emptySplits").sum);
  EXPECT_EQ(0, stats.at("consumerCreations").sum);
  EXPECT_EQ(0, stats.at("readBatches").sum);
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceRejectsZeroRequestedBatchSizeWithFatalLabel) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5)});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 6;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));
  auto future = facebook::velox::ContinueFuture::makeEmpty();

  expectVeloxExceptionMessageContains(
      [&]() { source.next(0, future); },
      "Native Kafka data source fatal batch-size failure");
  EXPECT_TRUE(factory->ranges.empty());
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceReportsSkippedMessagesForToleratedOffsetGap) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5), record(7)});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 8;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = false;

  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));
  auto future = facebook::velox::ContinueFuture::makeEmpty();
  auto result = source.next(4096, future);

  ASSERT_TRUE(result.has_value());
  auto batch = result.value();
  ASSERT_NE(nullptr, batch);
  ASSERT_EQ(2, batch->size());

  const auto* offsets = batch->childAt(0)->as<facebook::velox::FlatVector<int64_t>>();
  ASSERT_NE(nullptr, offsets);
  EXPECT_EQ(5, offsets->valueAt(0));
  EXPECT_EQ(7, offsets->valueAt(1));

  const auto stats = source.getRuntimeStats();
  EXPECT_EQ(1, stats.at("startedSplits").sum);
  EXPECT_EQ(1, stats.at("completedSplits").sum);
  EXPECT_EQ(3, stats.at("plannedMessages").sum);
  EXPECT_EQ(2, stats.at("readMessages").sum);
  EXPECT_EQ(1, stats.at("skippedMessages").sum);
  EXPECT_EQ(1, stats.at("watermarkLookups").sum);
  EXPECT_EQ(1, stats.at("seeks").sum);
  EXPECT_EQ(2, stats.at("polls").sum);
  EXPECT_EQ(0, stats.at("emptySplits").sum);
  EXPECT_EQ(1, stats.at("consumerCreations").sum);
  EXPECT_EQ(1, stats.at("readBatches").sum);
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceRejectsInvalidFiniteRangeBeforeConsumerFactory) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };

  auto validSplit = []() {
    gluten::KafkaSplitInfo splitInfo;
    splitInfo.topic = "native-topic";
    splitInfo.partition = 2;
    splitInfo.startOffset = 5;
    splitInfo.endOffset = 8;
    splitInfo.pollTimeoutMs = 123;
    splitInfo.failOnDataLoss = true;
    return splitInfo;
  };

  auto assertRejectedBeforeConsumer = [&](gluten::KafkaSplitInfo splitInfo, const std::string& expected) {
    auto factory = std::make_shared<FakeKafkaConsumerFactory>(
        std::vector<std::optional<KafkaRecord>>{record(5)});
    auto queryCtx = makeConnectorQueryCtx();
    KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

    expectVeloxExceptionMessageContains(
        [&]() { source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo)); },
        expected);
    EXPECT_TRUE(factory->ranges.empty());
    EXPECT_TRUE(factory->seeks->empty());
  };

  auto emptyTopic = validSplit();
  emptyTopic.topic = "";
  assertRejectedBeforeConsumer(emptyTopic, "requires a non-empty topic");

  auto negativePartition = validSplit();
  negativePartition.partition = -1;
  assertRejectedBeforeConsumer(negativePartition, "requires a non-negative partition");

  auto negativePollTimeout = validSplit();
  negativePollTimeout.pollTimeoutMs = -1;
  assertRejectedBeforeConsumer(negativePollTimeout, "requires a non-negative poll timeout");

  auto negativeOffset = validSplit();
  negativeOffset.startOffset = -1;
  assertRejectedBeforeConsumer(negativeOffset, "requires finite non-negative offsets");

  auto reversedOffsets = validSplit();
  reversedOffsets.endOffset = 4;
  assertRejectedBeforeConsumer(reversedOffsets, "requires endOffset >= startOffset");
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceHonorsRequestedBatchSizeAcrossNextCalls) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5), record(6), record(7)});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 8;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));
  auto future = facebook::velox::ContinueFuture::makeEmpty();

  auto firstResult = source.next(1, future);
  ASSERT_TRUE(firstResult.has_value());
  auto firstBatch = firstResult.value();
  ASSERT_NE(nullptr, firstBatch);
  ASSERT_EQ(1, firstBatch->size());
  const auto* firstOffsets = firstBatch->childAt(0)->as<facebook::velox::FlatVector<int64_t>>();
  EXPECT_EQ(5, firstOffsets->valueAt(0));
  EXPECT_EQ(1, factory->ranges.size());
  {
    const auto stats = source.getRuntimeStats();
    EXPECT_EQ(1, stats.at("startedSplits").sum);
    EXPECT_EQ(0, stats.at("completedSplits").sum);
    EXPECT_EQ(3, stats.at("plannedMessages").sum);
    EXPECT_EQ(1, stats.at("readMessages").sum);
    EXPECT_EQ(0, stats.at("skippedMessages").sum);
    EXPECT_EQ(0, stats.at("emptySplits").sum);
    EXPECT_EQ(1, stats.at("consumerCreations").sum);
    EXPECT_EQ(1, stats.at("readBatches").sum);
  }

  auto secondResult = source.next(2, future);
  ASSERT_TRUE(secondResult.has_value());
  auto secondBatch = secondResult.value();
  ASSERT_NE(nullptr, secondBatch);
  ASSERT_EQ(2, secondBatch->size());
  const auto* secondOffsets = secondBatch->childAt(0)->as<facebook::velox::FlatVector<int64_t>>();
  EXPECT_EQ(6, secondOffsets->valueAt(0));
  EXPECT_EQ(7, secondOffsets->valueAt(1));
  EXPECT_EQ(1, factory->ranges.size());
  EXPECT_EQ(3, source.getCompletedRows());
  {
    const auto stats = source.getRuntimeStats();
    EXPECT_EQ(1, stats.at("startedSplits").sum);
    EXPECT_EQ(1, stats.at("completedSplits").sum);
    EXPECT_EQ(3, stats.at("plannedMessages").sum);
    EXPECT_EQ(3, stats.at("readMessages").sum);
    EXPECT_EQ(0, stats.at("skippedMessages").sum);
    EXPECT_EQ(0, stats.at("emptySplits").sum);
    EXPECT_EQ(1, stats.at("consumerCreations").sum);
    EXPECT_EQ(2, stats.at("readBatches").sum);
  }

  auto done = source.next(2, future);
  ASSERT_TRUE(done.has_value());
  EXPECT_EQ(nullptr, done.value());
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceReplaysSparkPlannedSplitFromStartForFreshAttempt) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 8;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  auto runAttempt = [&]() {
    auto factory = std::make_shared<FakeKafkaConsumerFactory>(
        std::vector<std::optional<KafkaRecord>>{record(5), record(6), record(7)});
    auto queryCtx = makeConnectorQueryCtx();
    KafkaDataSource source(outputType, assignments, factory, queryCtx.get());
    source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));

    auto future = facebook::velox::ContinueFuture::makeEmpty();
    auto result = source.next(4096, future);
    EXPECT_TRUE(result.has_value());
    auto batch = result.value();
    EXPECT_NE(nullptr, batch);
    EXPECT_EQ(3, batch->size());

    const auto* offsets = batch->childAt(0)->as<facebook::velox::FlatVector<int64_t>>();
    std::vector<int64_t> observed;
    for (auto i = 0; i < batch->size(); ++i) {
      observed.push_back(offsets->valueAt(i));
    }

    auto done = source.next(4096, future);
    EXPECT_TRUE(done.has_value());
    EXPECT_EQ(nullptr, done.value());
    EXPECT_EQ(3, source.getCompletedRows());
    EXPECT_EQ(1, source.getRuntimeStats().at("completedSplits").sum);

    EXPECT_EQ(1, factory->ranges.size());
    EXPECT_EQ(1, factory->seeks->size());
    EXPECT_EQ(
        std::make_tuple(std::string("native-topic"), 2, int64_t{5}),
        factory->seeks->at(0));
    return observed;
  };

  const auto firstAttempt = runAttempt();
  const auto replayedAttempt = runAttempt();

  EXPECT_EQ(std::vector<int64_t>({5, 6, 7}), firstAttempt);
  EXPECT_EQ(firstAttempt, replayedAttempt);
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceFreshAttemptReplaysAfterPartialAttemptFailure) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5), record(6), record(7)});

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 8;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  {
    auto queryCtx = makeConnectorQueryCtx();
    KafkaDataSource failedAttempt(outputType, assignments, factory, queryCtx.get());
    failedAttempt.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));

    auto future = facebook::velox::ContinueFuture::makeEmpty();
    auto result = failedAttempt.next(1, future);
    ASSERT_TRUE(result.has_value());
    auto batch = result.value();
    ASSERT_NE(nullptr, batch);
    ASSERT_EQ(1, batch->size());
    const auto* offsets = batch->childAt(0)->as<facebook::velox::FlatVector<int64_t>>();
    ASSERT_NE(nullptr, offsets);
    EXPECT_EQ(5, offsets->valueAt(0));
    EXPECT_EQ(0, failedAttempt.getRuntimeStats().at("completedSplits").sum);
  }

  std::vector<int64_t> replayed;
  {
    auto queryCtx = makeConnectorQueryCtx();
    KafkaDataSource freshAttempt(outputType, assignments, factory, queryCtx.get());
    freshAttempt.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));

    auto future = facebook::velox::ContinueFuture::makeEmpty();
    auto result = freshAttempt.next(4096, future);
    ASSERT_TRUE(result.has_value());
    auto batch = result.value();
    ASSERT_NE(nullptr, batch);

    const auto* offsets = batch->childAt(0)->as<facebook::velox::FlatVector<int64_t>>();
    ASSERT_NE(nullptr, offsets);
    for (auto i = 0; i < batch->size(); ++i) {
      replayed.push_back(offsets->valueAt(i));
    }

    auto done = freshAttempt.next(4096, future);
    ASSERT_TRUE(done.has_value());
    EXPECT_EQ(nullptr, done.value());
    EXPECT_EQ(3, freshAttempt.getCompletedRows());
    EXPECT_EQ(1, freshAttempt.getRuntimeStats().at("completedSplits").sum);
  }

  EXPECT_EQ(std::vector<int64_t>({5, 6, 7}), replayed);
  ASSERT_EQ(2, factory->seeks->size());
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, int64_t{5}), factory->seeks->at(0));
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, int64_t{5}), factory->seeks->at(1));
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaTableScanTaskReplaysFiniteSplitFromSparkPlannedStartOffset) {
  const std::string connectorId = "native-kafka-task-replay";
  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5), record(6), record(7)});
  ScopedKafkaConnector connector(connectorId, factory);

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 8;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  const auto firstAttempt = readOffsetsFromKafkaTask("native-kafka-task-first", connectorId, splitInfo);
  const auto replayedAttempt = readOffsetsFromKafkaTask("native-kafka-task-replay", connectorId, splitInfo);

  EXPECT_EQ(std::vector<int64_t>({5, 6, 7}), firstAttempt);
  EXPECT_EQ(firstAttempt, replayedAttempt);

  ASSERT_EQ(2, factory->ranges.size());
  for (const auto& observedRange : factory->ranges) {
    EXPECT_EQ("native-topic", observedRange.topic);
    EXPECT_EQ(2, observedRange.partition);
    EXPECT_EQ(5, observedRange.startOffset);
    EXPECT_EQ(8, observedRange.endOffset);
    EXPECT_EQ(123, observedRange.pollTimeoutMs);
    EXPECT_TRUE(observedRange.failOnDataLoss);
  }

  ASSERT_EQ(2, factory->seeks->size());
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, int64_t{5}), factory->seeks->at(0));
  EXPECT_EQ(std::make_tuple(std::string("native-topic"), 2, int64_t{5}), factory->seeks->at(1));
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceRejectsHeadersProjectionWhenSplitExcludesHeaders) {
  const auto headersType = facebook::velox::ARRAY(
      facebook::velox::ROW({"key", "value"}, {facebook::velox::VARCHAR(), facebook::velox::VARBINARY()}));
  auto outputType = facebook::velox::ROW({"projected_headers"}, {headersType});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"projected_headers", std::make_shared<KafkaColumnHandle>("projected_headers", "headers", headersType)},
  };

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5)});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 6;
  splitInfo.includeHeaders = false;

  EXPECT_THROW(
      source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo)),
      facebook::velox::VeloxException);
  EXPECT_TRUE(factory->ranges.empty());
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceRejectsUnknownSourceColumnBeforeConsumerFactory) {
  auto outputType = facebook::velox::ROW({"projected_bad_source"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"projected_bad_source",
       std::make_shared<KafkaColumnHandle>(
           "projected_bad_source",
           "_spark_metadata",
           facebook::velox::BIGINT())},
  };

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5)});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 6;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  expectVeloxExceptionMessageContains(
      [&]() {
        source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));
      },
      "outside the Spark Kafka row schema");
  EXPECT_TRUE(factory->ranges.empty());
  EXPECT_TRUE(factory->seeks->empty());
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceRejectsMissingOutputHandleBeforeConsumerFactory) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments;

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5)});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 6;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  expectVeloxExceptionMessageContains(
      [&]() {
        source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));
      },
      "missing a column handle");
  EXPECT_TRUE(factory->ranges.empty());
  EXPECT_TRUE(factory->seeks->empty());
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceRejectsMismatchedOutputHandleTypeBeforeConsumerFactory) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::INTEGER())},
  };

  auto factory = std::make_shared<FakeKafkaConsumerFactory>(
      std::vector<std::optional<KafkaRecord>>{record(5)});
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, factory, queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 6;
  splitInfo.pollTimeoutMs = 123;
  splitInfo.failOnDataLoss = true;

  expectVeloxExceptionMessageContains(
      [&]() {
        source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));
      },
      "has type BIGINT, but its column handle has type INTEGER");
  EXPECT_TRUE(factory->ranges.empty());
  EXPECT_TRUE(factory->seeks->empty());
}

TEST_F(KafkaFiniteReaderVectorTest, kafkaDataSourceFailsClosedWithoutConsumerFactory) {
  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(
      outputType,
      assignments,
      std::make_shared<UnsupportedKafkaConsumerFactory>(),
      queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = "native-topic";
  splitInfo.partition = 2;
  splitInfo.startOffset = 5;
  splitInfo.endOffset = 6;
  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-test", splitInfo));

  auto future = facebook::velox::ContinueFuture::makeEmpty();
  expectVeloxExceptionMessageContains(
      [&]() { source.next(4096, future); },
      "Native Kafka connector fatal consumer-factory failure");
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaParamsKeepSparkAsOffsetOwner) {
  auto readRange = range(5, 7);
  readRange.params["group.id"] = "spark-owned-group";
  readRange.params["enable.auto.commit"] = "true";
  readRange.params["enable.auto.offset.store"] = "true";
  readRange.params["auto.commit.enable"] = "true";

  const auto params = makeSparkOwnedKafkaConsumerParams(readRange);

  EXPECT_EQ("localhost:9092", params.at("bootstrap.servers"));
  EXPECT_EQ("spark-owned-group", params.at("group.id"));
  EXPECT_EQ("false", params.at("enable.auto.commit"));
  EXPECT_EQ("false", params.at("enable.auto.offset.store"));
  EXPECT_EQ(0, params.count("auto.commit.enable"));
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaParamsDropJvmOnlyOptionsAndKeepSecurityOptions) {
  auto readRange = range(5, 7);
  readRange.params["key.deserializer"] = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  readRange.params["value.deserializer"] = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  readRange.params["interceptor.classes"] = "com.example.SparkOnlyInterceptor";
  readRange.params["metric.reporters"] = "com.example.SparkOnlyReporter";
  readRange.params["auto.offset.reset"] = "none";
  readRange.params["partition.assignment.strategy"] = "org.apache.kafka.clients.consumer.RangeAssignor";
  readRange.params["receive.buffer.bytes"] = "65536";
  readRange.params["spark.executor.id"] = "driver";
  readRange.params["security.protocol"] = "SASL_SSL";
  readRange.params["sasl.jaas.config"] = "org.apache.kafka.common.security.plain.PlainLoginModule required;";

  const auto params = makeSparkOwnedKafkaConsumerParams(readRange);

  EXPECT_EQ("localhost:9092", params.at("bootstrap.servers"));
  EXPECT_EQ("SASL_SSL", params.at("security.protocol"));
  EXPECT_EQ("org.apache.kafka.common.security.plain.PlainLoginModule required;", params.at("sasl.jaas.config"));
  EXPECT_EQ(0, params.count("key.deserializer"));
  EXPECT_EQ(0, params.count("value.deserializer"));
  EXPECT_EQ(0, params.count("interceptor.classes"));
  EXPECT_EQ(0, params.count("metric.reporters"));
  EXPECT_EQ(0, params.count("auto.offset.reset"));
  EXPECT_EQ(0, params.count("partition.assignment.strategy"));
  EXPECT_EQ(0, params.count("receive.buffer.bytes"));
  EXPECT_EQ(0, params.count("spark.executor.id"));
  EXPECT_EQ("false", params.at("enable.auto.commit"));
  EXPECT_EQ("false", params.at("enable.auto.offset.store"));
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaParamsRequireBootstrapServers) {
  auto readRange = range(5, 7);
  readRange.params.clear();

  expectVeloxExceptionMessageContains(
      [&]() { makeSparkOwnedKafkaConsumerParams(readRange); },
      "Native Kafka consumer fatal config failure");
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaFailureKindNamesAreStable) {
  EXPECT_STREQ("end-of-partition", kafkaConsumerFailureKindName(KafkaConsumerFailureKind::EndOfPartition));
  EXPECT_STREQ("timeout", kafkaConsumerFailureKindName(KafkaConsumerFailureKind::Timeout));
  EXPECT_STREQ("retriable", kafkaConsumerFailureKindName(KafkaConsumerFailureKind::Retriable));
  EXPECT_STREQ("data-loss", kafkaConsumerFailureKindName(KafkaConsumerFailureKind::DataLoss));
  EXPECT_STREQ("fatal", kafkaConsumerFailureKindName(KafkaConsumerFailureKind::Fatal));
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaResponseErrorClassificationPinsCommonFailures) {
  if (!rdkafkaSupportCompiled()) {
    GTEST_SKIP() << "rdkafka support is not compiled in this build.";
  }

  EXPECT_EQ(
      KafkaConsumerFailureKind::EndOfPartition,
      classifyRdkafkaResponseErrorCode(-191 /* RD_KAFKA_RESP_ERR__PARTITION_EOF */));
  EXPECT_EQ(
      KafkaConsumerFailureKind::Timeout,
      classifyRdkafkaResponseErrorCode(-185 /* RD_KAFKA_RESP_ERR__TIMED_OUT */));
  EXPECT_EQ(
      KafkaConsumerFailureKind::Timeout,
      classifyRdkafkaResponseErrorCode(-166 /* RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE */));
  EXPECT_EQ(
      KafkaConsumerFailureKind::Retriable,
      classifyRdkafkaResponseErrorCode(-195 /* RD_KAFKA_RESP_ERR__TRANSPORT */));
  EXPECT_EQ(
      KafkaConsumerFailureKind::Retriable,
      classifyRdkafkaResponseErrorCode(-187 /* RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN */));
  EXPECT_EQ(
      KafkaConsumerFailureKind::DataLoss,
      classifyRdkafkaResponseErrorCode(1 /* RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE */));
  EXPECT_EQ(KafkaConsumerFailureKind::Fatal, classifyRdkafkaResponseErrorCode(-172));
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaLiveGateRequiresCompiledSupport) {
  ASSERT_TRUE(rdkafkaSupportCompiled())
      << "The native Kafka live gate requires ENABLE_VELOX_KAFKA_CLIENT=ON. "
      << "Rebuild with dev/builddeps-veloxbe.sh --enable_velox_kafka_client=ON "
      << "or pass -DENABLE_VELOX_KAFKA_CLIENT=ON to CMake.";
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaLiveBrokerReadsSparkPlannedFiniteRangeWhenEnabled) {
  if (!rdkafkaSupportCompiled()) {
    GTEST_SKIP() << "rdkafka support is not compiled in this build.";
  }

  const auto bootstrapServers = envString("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS");
  const auto topic = envString("GLUTEN_KAFKA_TEST_TOPIC");
  if (!bootstrapServers.has_value() || !topic.has_value()) {
    GTEST_SKIP() << "Set GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS and GLUTEN_KAFKA_TEST_TOPIC to run.";
  }

  const auto partition = static_cast<int32_t>(envInt64OrDefault("GLUTEN_KAFKA_TEST_PARTITION", 0));
  const auto startOffset = envInt64OrDefault("GLUTEN_KAFKA_TEST_START_OFFSET", 0);
  const auto endOffset = envInt64OrDefault("GLUTEN_KAFKA_TEST_END_OFFSET", startOffset + 1);
  VELOX_CHECK_GT(endOffset, startOffset);

  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };

  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, createRdkafkaConsumerFactory(), queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = topic.value();
  splitInfo.partition = partition;
  splitInfo.startOffset = startOffset;
  splitInfo.endOffset = endOffset;
  splitInfo.pollTimeoutMs = envInt64OrDefault("GLUTEN_KAFKA_TEST_POLL_TIMEOUT_MS", 5000);
  splitInfo.failOnDataLoss = true;
  splitInfo.params = {{"bootstrap.servers", bootstrapServers.value()}};

  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-live-test", splitInfo));

  std::vector<int64_t> observed;
  auto future = facebook::velox::ContinueFuture::makeEmpty();
  while (true) {
    auto result = source.next(4096, future);
    ASSERT_TRUE(result.has_value());
    auto batch = result.value();
    if (batch == nullptr) {
      break;
    }

    const auto* offsets = batch->childAt(0)->as<facebook::velox::FlatVector<int64_t>>();
    ASSERT_NE(nullptr, offsets);
    for (auto i = 0; i < batch->size(); ++i) {
      observed.push_back(offsets->valueAt(i));
    }
  }

  std::vector<int64_t> expected;
  for (auto offset = startOffset; offset < endOffset; ++offset) {
    expected.push_back(offset);
  }
  EXPECT_EQ(expected, observed);
  EXPECT_EQ(static_cast<uint64_t>(expected.size()), source.getCompletedRows());
  EXPECT_EQ(1, source.getRuntimeStats().at("completedSplits").sum);
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaLiveBrokerDiscoversTopicPartitionsWhenEnabled) {
  if (!rdkafkaSupportCompiled()) {
    GTEST_SKIP() << "rdkafka support is not compiled in this build.";
  }

  const auto bootstrapServers = envString("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS");
  const auto topic = envString("GLUTEN_KAFKA_TEST_TOPIC");
  if (!bootstrapServers.has_value() || !topic.has_value()) {
    GTEST_SKIP() << "Set GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS and GLUTEN_KAFKA_TEST_TOPIC to run.";
  }

  const auto expectedPartition =
      static_cast<int32_t>(envInt64OrDefault("GLUTEN_KAFKA_TEST_PARTITION", 0));
  const auto discovered = discoverTopicPartitions(
      {topic.value()},
      {{"bootstrap.servers", bootstrapServers.value()}},
      envInt64OrDefault("GLUTEN_KAFKA_TEST_POLL_TIMEOUT_MS", 5000));

  EXPECT_TRUE(std::any_of(
      discovered.begin(),
      discovered.end(),
      [&](const auto& topicPartition) {
        return topicPartition.topic == topic.value() &&
            topicPartition.partition == expectedPartition;
      }));
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaLiveBrokerListsSubscribePatternTopicMetadataWhenEnabled) {
  if (!rdkafkaSupportCompiled()) {
    GTEST_SKIP() << "rdkafka support is not compiled in this build.";
  }

  const auto bootstrapServers = envString("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS");
  const auto topicPrefix = envString("GLUTEN_KAFKA_TEST_PATTERN_TOPIC_PREFIX");
  const auto expectedTopicsCsv = envString("GLUTEN_KAFKA_TEST_PATTERN_EXPECTED_TOPICS");
  if (!bootstrapServers.has_value() || !topicPrefix.has_value() || !expectedTopicsCsv.has_value()) {
    GTEST_SKIP()
        << "Set GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS, GLUTEN_KAFKA_TEST_PATTERN_TOPIC_PREFIX, "
        << "and GLUTEN_KAFKA_TEST_PATTERN_EXPECTED_TOPICS to run.";
  }

  const auto expectedTopics = splitCsv(expectedTopicsCsv.value());
  ASSERT_FALSE(expectedTopics.empty());

  const auto discovered = listTopicPartitionMetadata(
      {{"bootstrap.servers", bootstrapServers.value()}},
      envInt64OrDefault("GLUTEN_KAFKA_TEST_POLL_TIMEOUT_MS", 5000));

  std::set<std::string> matchingTopics;
  for (const auto& topicPartition : discovered) {
    if (!startsWith(topicPartition.topic, topicPrefix.value())) {
      continue;
    }
    ASSERT_TRUE(topicPartition.error.empty())
        << "Expected matching topic metadata to be healthy for " << topicPartition.topic
        << ", but native discovery returned " << topicPartition.failureKind << ": "
        << topicPartition.error;
    ASSERT_GE(topicPartition.partition, 0);
    matchingTopics.insert(topicPartition.topic);
  }

  for (const auto& expectedTopic : expectedTopics) {
    EXPECT_TRUE(matchingTopics.count(expectedTopic) > 0)
        << "Native all-topic metadata discovery did not include expected pattern topic "
        << expectedTopic;
  }

  const auto missTopic = envString("GLUTEN_KAFKA_TEST_PATTERN_MISS_TOPIC");
  if (missTopic.has_value()) {
    EXPECT_FALSE(startsWith(missTopic.value(), topicPrefix.value()));
    EXPECT_EQ(0, matchingTopics.count(missTopic.value()));
  }
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaLiveBrokerFailsOnDeletedStartOffsetWhenDataLossEnabled) {
  if (!rdkafkaSupportCompiled()) {
    GTEST_SKIP() << "rdkafka support is not compiled in this build.";
  }

  const auto bootstrapServers = envString("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS");
  const auto topic = envString("GLUTEN_KAFKA_TEST_DATA_LOSS_TOPIC");
  if (!bootstrapServers.has_value() || !topic.has_value()) {
    GTEST_SKIP() << "Set GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS and GLUTEN_KAFKA_TEST_DATA_LOSS_TOPIC to run.";
  }

  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, createRdkafkaConsumerFactory(), queryCtx.get());

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = topic.value();
  splitInfo.partition = static_cast<int32_t>(envInt64OrDefault("GLUTEN_KAFKA_TEST_PARTITION", 0));
  splitInfo.startOffset = envInt64OrDefault("GLUTEN_KAFKA_TEST_DATA_LOSS_START_OFFSET", 0);
  splitInfo.endOffset = envInt64OrDefault("GLUTEN_KAFKA_TEST_DATA_LOSS_END_OFFSET", splitInfo.startOffset + 2);
  splitInfo.pollTimeoutMs = envInt64OrDefault("GLUTEN_KAFKA_TEST_POLL_TIMEOUT_MS", 5000);
  splitInfo.failOnDataLoss = true;
  splitInfo.params = {{"bootstrap.servers", bootstrapServers.value()}};

  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-live-test", splitInfo));

  auto future = facebook::velox::ContinueFuture::makeEmpty();
  EXPECT_THROW(source.next(4096, future), facebook::velox::VeloxException);
  EXPECT_EQ(0, source.getCompletedRows());
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaLiveBrokerSkipsDeletedStartOffsetWhenDataLossDisabled) {
  if (!rdkafkaSupportCompiled()) {
    GTEST_SKIP() << "rdkafka support is not compiled in this build.";
  }

  const auto bootstrapServers = envString("GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS");
  const auto topic = envString("GLUTEN_KAFKA_TEST_DATA_LOSS_TOPIC");
  if (!bootstrapServers.has_value() || !topic.has_value()) {
    GTEST_SKIP() << "Set GLUTEN_KAFKA_TEST_BOOTSTRAP_SERVERS and GLUTEN_KAFKA_TEST_DATA_LOSS_TOPIC to run.";
  }

  auto outputType = facebook::velox::ROW({"offset"}, {facebook::velox::BIGINT()});
  facebook::velox::connector::ColumnHandleMap assignments{
      {"offset", std::make_shared<KafkaColumnHandle>("offset", "offset", facebook::velox::BIGINT())},
  };
  auto queryCtx = makeConnectorQueryCtx();
  KafkaDataSource source(outputType, assignments, createRdkafkaConsumerFactory(), queryCtx.get());

  const auto startOffset = envInt64OrDefault("GLUTEN_KAFKA_TEST_DATA_LOSS_START_OFFSET", 0);
  const auto endOffset = envInt64OrDefault("GLUTEN_KAFKA_TEST_DATA_LOSS_END_OFFSET", startOffset + 2);
  const auto lowWatermark = envInt64OrDefault("GLUTEN_KAFKA_TEST_DATA_LOSS_LOW_WATERMARK", startOffset + 1);

  gluten::KafkaSplitInfo splitInfo;
  splitInfo.topic = topic.value();
  splitInfo.partition = static_cast<int32_t>(envInt64OrDefault("GLUTEN_KAFKA_TEST_PARTITION", 0));
  splitInfo.startOffset = startOffset;
  splitInfo.endOffset = endOffset;
  splitInfo.pollTimeoutMs = envInt64OrDefault("GLUTEN_KAFKA_TEST_POLL_TIMEOUT_MS", 5000);
  splitInfo.failOnDataLoss = false;
  splitInfo.params = {{"bootstrap.servers", bootstrapServers.value()}};

  source.addSplit(std::make_shared<KafkaConnectorSplit>("native-kafka-live-test", splitInfo));

  std::vector<int64_t> observed;
  auto future = facebook::velox::ContinueFuture::makeEmpty();
  while (true) {
    auto result = source.next(4096, future);
    ASSERT_TRUE(result.has_value());
    auto batch = result.value();
    if (batch == nullptr) {
      break;
    }

    const auto* offsets = batch->childAt(0)->as<facebook::velox::FlatVector<int64_t>>();
    ASSERT_NE(nullptr, offsets);
    for (auto i = 0; i < batch->size(); ++i) {
      observed.push_back(offsets->valueAt(i));
    }
  }

  std::vector<int64_t> expected;
  for (auto offset = std::max(startOffset, lowWatermark); offset < endOffset; ++offset) {
    expected.push_back(offset);
  }
  EXPECT_EQ(expected, observed);
  EXPECT_EQ(static_cast<uint64_t>(expected.size()), source.getCompletedRows());
  EXPECT_EQ(1, source.getRuntimeStats().at("completedSplits").sum);
}

TEST_F(KafkaFiniteReaderVectorTest, rdkafkaFactoryFailsClosedWhenSupportIsNotCompiled) {
  if (rdkafkaSupportCompiled()) {
    GTEST_SKIP() << "rdkafka support is compiled in this build.";
  }

  auto factory = createRdkafkaConsumerFactory();
  EXPECT_THROW(factory->create(range(5, 7)), facebook::velox::VeloxException);
}

} // namespace gluten::kafka
