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

#include "compute/kafka/KafkaConnector.h"

#include "velox/common/base/Exceptions.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <utility>

namespace gluten::kafka {
namespace {

using facebook::velox::RuntimeMetric;
using facebook::velox::VectorPtr;
using facebook::velox::connector::ColumnHandleMap;
using facebook::velox::connector::ConnectorSplit;
using facebook::velox::connector::ConnectorTableHandlePtr;
using facebook::velox::connector::DataSink;
using facebook::velox::connector::DataSource;
using facebook::velox::connector::ConnectorQueryCtx;
using facebook::velox::connector::ConnectorInsertTableHandlePtr;
using facebook::velox::connector::CommitStrategy;
using facebook::velox::RowTypePtr;
using facebook::velox::RowVector;
using facebook::velox::RowVectorPtr;

std::string canonicalColumnName(std::string name) {
  std::transform(name.begin(), name.end(), name.begin(), [](unsigned char value) {
    return static_cast<char>(std::tolower(value));
  });
  return name;
}

std::unordered_map<std::string, int32_t> indexColumns(const RowTypePtr& type) {
  std::unordered_map<std::string, int32_t> indexes;
  for (int32_t idx = 0; idx < type->size(); ++idx) {
    indexes.emplace(canonicalColumnName(type->nameOf(idx)), idx);
  }
  return indexes;
}

} // namespace

KafkaConnectorSplit::KafkaConnectorSplit(std::string connectorId, KafkaSplitInfo splitInfo)
    : ConnectorSplit(std::move(connectorId)), splitInfo_(std::move(splitInfo)) {}

uint64_t KafkaConnectorSplit::size() const {
  if (splitInfo_.endOffset <= splitInfo_.startOffset) {
    return 0;
  }
  return static_cast<uint64_t>(splitInfo_.endOffset - splitInfo_.startOffset);
}

std::string KafkaConnectorSplit::toString() const {
  return fmt::format(
      "[split: connector id {}, kafka topic {}, partition {}, offsets [{}, {}), includeHeaders {}]",
      connectorId,
      splitInfo_.topic,
      splitInfo_.partition,
      splitInfo_.startOffset,
      splitInfo_.endOffset,
      splitInfo_.includeHeaders ? "true" : "false");
}

KafkaTableHandle::KafkaTableHandle(std::string connectorId)
    : ConnectorTableHandle(std::move(connectorId)) {}

const std::string& KafkaTableHandle::name() const {
  static const std::string kName = "KafkaTableHandle";
  return kName;
}

KafkaColumnHandle::KafkaColumnHandle(std::string outputName, std::string sourceName, facebook::velox::TypePtr type)
    : outputName_(std::move(outputName)), sourceName_(std::move(sourceName)), type_(std::move(type)) {}

std::unique_ptr<KafkaConsumer> UnsupportedKafkaConsumerFactory::create(const KafkaPartitionRange& range) {
  VELOX_USER_FAIL(
      "Native Kafka connector fatal consumer-factory failure: reached topic={}, partition={}, "
      "offsets=[{}, {}), but no Kafka consumer factory is registered. Keep native Kafka execution "
      "disabled until a real Kafka client binding is configured.",
      range.topic,
      range.partition,
      range.startOffset,
      range.endOffset);
  return nullptr;
}

KafkaDataSource::KafkaDataSource(
    RowTypePtr outputType,
    ColumnHandleMap columnHandles,
    std::shared_ptr<KafkaConsumerFactory> consumerFactory,
    ConnectorQueryCtx* connectorQueryCtx)
    : outputType_(std::move(outputType)),
      columnHandles_(std::move(columnHandles)),
      consumerFactory_(std::move(consumerFactory)),
      pool_(connectorQueryCtx->memoryPool()) {
  VELOX_USER_CHECK_NOT_NULL(outputType_);
  VELOX_USER_CHECK_NOT_NULL(consumerFactory_);
  VELOX_USER_CHECK_NOT_NULL(pool_);
}

void KafkaDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_USER_CHECK(
      currentSplitConsumed_,
      "Native Kafka data source fatal split failure: received a split before the previous split ended.");
  auto kafkaSplit = std::dynamic_pointer_cast<const KafkaConnectorSplit>(split);
  VELOX_USER_CHECK_NOT_NULL(
      kafkaSplit, "Native Kafka data source fatal split failure: requires KafkaConnectorSplit.");
  validateKafkaPartitionRange(kafkaPartitionRangeFromSplit(kafkaSplit->splitInfo()));
  validateSplitProjection(kafkaSplit->splitInfo());
  ++startedSplits_;
  plannedMessages_ += kafkaSplit->size();
  currentSplit_ = kafkaSplit->splitInfo();
  currentSplitConsumed_ = false;
}

std::optional<RowVectorPtr> KafkaDataSource::next(uint64_t size, facebook::velox::ContinueFuture& /*future*/) {
  if (!currentSplit_.has_value() || currentSplitConsumed_) {
    return RowVectorPtr(nullptr);
  }

  VELOX_USER_CHECK_GT(
      size,
      0,
      "Native Kafka data source fatal batch-size failure: requires a positive requested batch size.");

  if (currentSplit_->endOffset <= currentSplit_->startOffset) {
    ++emptySplits_;
    finishCurrentSplit();
    return RowVectorPtr(nullptr);
  }

  if (!currentReader_) {
    auto range = kafkaPartitionRangeFromSplit(currentSplit_.value());
    currentConsumer_ = consumerFactory_->create(range);
    ++consumerCreations_;
    currentReader_ = std::make_unique<KafkaFiniteReader>(std::move(range), currentConsumer_.get());
  }

  const auto maxRows = static_cast<size_t>(std::min<uint64_t>(size, std::numeric_limits<size_t>::max()));
  const auto skippedBeforeRead = currentReader_->skippedMessages();
  const auto watermarkLookupsBeforeRead = currentReader_->watermarkLookups();
  const auto seeksBeforeRead = currentReader_->seeks();
  const auto pollsBeforeRead = currentReader_->polls();
  auto batch = makeKafkaRowVector(
      currentReader_->readNext(maxRows), currentSplit_->includeHeaders, pool_);
  skippedMessages_ += currentReader_->skippedMessages() - skippedBeforeRead;
  watermarkLookups_ += currentReader_->watermarkLookups() - watermarkLookupsBeforeRead;
  seeks_ += currentReader_->seeks() - seeksBeforeRead;
  polls_ += currentReader_->polls() - pollsBeforeRead;
  ++readBatches_;
  auto projected = projectBatch(batch);

  completedRows_ += projected->size();
  readMessages_ += projected->size();
  completedBytes_ += projected->estimateFlatSize();
  if (currentReader_->done()) {
    finishCurrentSplit();
  }

  if (projected->size() == 0) {
    return RowVectorPtr(nullptr);
  }
  return projected;
}

void KafkaDataSource::finishCurrentSplit() {
  currentReader_.reset();
  currentConsumer_.reset();
  currentSplit_.reset();
  currentSplitConsumed_ = true;
  ++completedSplits_;
}

void KafkaDataSource::addDynamicFilter(
    facebook::velox::column_index_t /*outputChannel*/,
    const std::shared_ptr<facebook::velox::common::Filter>& /*filter*/) {
  ++dynamicFiltersRejected_;
}

std::unordered_map<std::string, RuntimeMetric> KafkaDataSource::getRuntimeStats() {
  std::unordered_map<std::string, RuntimeMetric> stats;
  stats["startedSplits"] = RuntimeMetric(startedSplits_);
  stats["completedSplits"] = RuntimeMetric(completedSplits_);
  stats["plannedMessages"] = RuntimeMetric(plannedMessages_);
  stats["readMessages"] = RuntimeMetric(readMessages_);
  stats["skippedMessages"] = RuntimeMetric(skippedMessages_);
  stats["watermarkLookups"] = RuntimeMetric(watermarkLookups_);
  stats["seeks"] = RuntimeMetric(seeks_);
  stats["polls"] = RuntimeMetric(polls_);
  stats["emptySplits"] = RuntimeMetric(emptySplits_);
  stats["consumerCreations"] = RuntimeMetric(consumerCreations_);
  stats["readBatches"] = RuntimeMetric(readBatches_);
  if (dynamicFiltersRejected_ > 0) {
    stats["dynamicFiltersRejected"] = RuntimeMetric(dynamicFiltersRejected_);
  }
  return stats;
}

void KafkaDataSource::validateSplitProjection(const KafkaSplitInfo& splitInfo) const {
  const auto sparkKafkaColumns = indexColumns(sparkKafkaRowType(splitInfo.includeHeaders));
  auto validateKafkaColumnHandle = [&](const std::string& outputName, const auto& columnHandle) {
    auto kafkaColumnHandle = std::dynamic_pointer_cast<const KafkaColumnHandle>(columnHandle);
    VELOX_USER_CHECK_NOT_NULL(
        kafkaColumnHandle,
        "Native Kafka data source fatal projection failure: output column '{}' requires a KafkaColumnHandle.",
        outputName);
    VELOX_USER_CHECK(
        kafkaColumnHandle->sourceName() != "headers" || splitInfo.includeHeaders,
        "Native Kafka data source fatal projection failure: output column '{}' requests Spark Kafka headers, but the finite split for "
        "topic={}, partition={}, offsets=[{}, {}) was planned without headers.",
        outputName,
        splitInfo.topic,
        splitInfo.partition,
        splitInfo.startOffset,
        splitInfo.endOffset);
    VELOX_USER_CHECK(
        sparkKafkaColumns.find(canonicalColumnName(kafkaColumnHandle->sourceName())) !=
            sparkKafkaColumns.end(),
        "Native Kafka data source fatal projection failure: output column '{}' maps to source column '{}', which is outside the Spark Kafka row schema for "
        "topic={}, partition={}, offsets=[{}, {}).",
        outputName,
        kafkaColumnHandle->sourceName(),
        splitInfo.topic,
        splitInfo.partition,
        splitInfo.startOffset,
        splitInfo.endOffset);
    return kafkaColumnHandle;
  };

  for (int32_t idx = 0; idx < outputType_->size(); ++idx) {
    const auto& outputName = outputType_->nameOf(idx);
    const auto handleIt = columnHandles_.find(outputName);
    VELOX_USER_CHECK(
        handleIt != columnHandles_.end(),
        "Native Kafka data source fatal projection failure: output column '{}' is missing a column handle.",
        outputName);

    auto kafkaColumnHandle = validateKafkaColumnHandle(outputName, handleIt->second);
    VELOX_USER_CHECK(
        outputType_->childAt(idx)->kindEquals(kafkaColumnHandle->type()),
        "Native Kafka data source fatal projection failure: output column '{}' has type {}, but its column handle has type {}.",
        outputName,
        outputType_->childAt(idx)->toString(),
        kafkaColumnHandle->type()->toString());
  }

  for (const auto& [outputName, columnHandle] : columnHandles_) {
    validateKafkaColumnHandle(outputName, columnHandle);
  }
}

RowVectorPtr KafkaDataSource::projectBatch(const RowVectorPtr& fullBatch) const {
  VELOX_USER_CHECK_NOT_NULL(fullBatch);
  const auto sourceIndexes = indexColumns(facebook::velox::asRowType(fullBatch->type()));

  std::vector<VectorPtr> children;
  children.reserve(outputType_->size());
  for (int32_t idx = 0; idx < outputType_->size(); ++idx) {
    const auto& outputName = outputType_->nameOf(idx);
    const auto handleIt = columnHandles_.find(outputName);
    VELOX_USER_CHECK(
        handleIt != columnHandles_.end(),
        "Native Kafka data source fatal projection failure: output column '{}' is missing a column handle.",
        outputName);
    auto kafkaColumnHandle = std::dynamic_pointer_cast<const KafkaColumnHandle>(handleIt->second);
    VELOX_USER_CHECK_NOT_NULL(
        kafkaColumnHandle,
        "Native Kafka data source fatal projection failure: output column '{}' requires a KafkaColumnHandle.",
        outputName);
    VELOX_USER_CHECK(
        outputType_->childAt(idx)->kindEquals(kafkaColumnHandle->type()),
        "Native Kafka data source fatal projection failure: output column '{}' has type {}, but its column handle has type {}.",
        outputName,
        outputType_->childAt(idx)->toString(),
        kafkaColumnHandle->type()->toString());

    const auto sourceIt = sourceIndexes.find(canonicalColumnName(kafkaColumnHandle->sourceName()));
    VELOX_USER_CHECK(
        sourceIt != sourceIndexes.end(),
        "Native Kafka data source fatal projection failure: source column '{}' is not present in the Spark Kafka row schema.",
        kafkaColumnHandle->sourceName());
    const auto sourceChild = fullBatch->childAt(sourceIt->second);
    VELOX_USER_CHECK(
        outputType_->childAt(idx)->kindEquals(sourceChild->type()),
        "Native Kafka data source fatal projection failure: output column '{}' has type {}, but source column '{}' has type {}.",
        outputName,
        outputType_->childAt(idx)->toString(),
        kafkaColumnHandle->sourceName(),
        sourceChild->type()->toString());
    children.emplace_back(sourceChild);
  }

  return std::make_shared<RowVector>(pool_, outputType_, nullptr, fullBatch->size(), std::move(children));
}

KafkaConnector::KafkaConnector(
    std::string id,
    std::shared_ptr<const facebook::velox::config::ConfigBase> config,
    std::shared_ptr<KafkaConsumerFactory> consumerFactory)
    : Connector(std::move(id), std::move(config)), consumerFactory_(std::move(consumerFactory)) {
  VELOX_USER_CHECK_NOT_NULL(consumerFactory_);
}

std::unique_ptr<DataSource> KafkaConnector::createDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  VELOX_USER_CHECK_NOT_NULL(
      std::dynamic_pointer_cast<const KafkaTableHandle>(tableHandle),
      "Native Kafka connector fatal table-handle failure: requires KafkaTableHandle.");
  return std::make_unique<KafkaDataSource>(outputType, columnHandles, consumerFactory_, connectorQueryCtx);
}

std::unique_ptr<DataSink> KafkaConnector::createDataSink(
    RowTypePtr /*inputType*/,
    ConnectorInsertTableHandlePtr /*connectorInsertTableHandle*/,
    ConnectorQueryCtx* /*connectorQueryCtx*/,
    CommitStrategy /*commitStrategy*/) {
  VELOX_USER_FAIL("Native Kafka connector fatal sink failure: data sinks are not supported.");
}

} // namespace gluten::kafka
