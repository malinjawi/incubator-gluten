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

#include "compute/kafka/KafkaSplitReader.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "velox/connectors/Connector.h"

namespace gluten::kafka {

class KafkaConnectorSplit final : public facebook::velox::connector::ConnectorSplit {
 public:
  KafkaConnectorSplit(std::string connectorId, KafkaSplitInfo splitInfo);

  const KafkaSplitInfo& splitInfo() const {
    return splitInfo_;
  }

  uint64_t size() const override;

  std::string toString() const override;

 private:
  KafkaSplitInfo splitInfo_;
};

class KafkaTableHandle final : public facebook::velox::connector::ConnectorTableHandle {
 public:
  explicit KafkaTableHandle(std::string connectorId);

  const std::string& name() const override;

  folly::dynamic serialize() const override {
    VELOX_NYI();
  }
};

class KafkaColumnHandle final : public facebook::velox::connector::ColumnHandle {
 public:
  KafkaColumnHandle(std::string outputName, std::string sourceName, facebook::velox::TypePtr type);

  const std::string& name() const override {
    return outputName_;
  }

  const std::string& sourceName() const {
    return sourceName_;
  }

  const facebook::velox::TypePtr& type() const {
    return type_;
  }

 private:
  std::string outputName_;
  std::string sourceName_;
  facebook::velox::TypePtr type_;
};

class KafkaConsumerFactory {
 public:
  virtual ~KafkaConsumerFactory() = default;

  virtual std::unique_ptr<KafkaConsumer> create(const KafkaPartitionRange& range) = 0;
};

class UnsupportedKafkaConsumerFactory final : public KafkaConsumerFactory {
 public:
  std::unique_ptr<KafkaConsumer> create(const KafkaPartitionRange& range) override;
};

class KafkaDataSource final : public facebook::velox::connector::DataSource {
 public:
  KafkaDataSource(
      facebook::velox::RowTypePtr outputType,
      facebook::velox::connector::ColumnHandleMap columnHandles,
      std::shared_ptr<KafkaConsumerFactory> consumerFactory,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx);

  void addSplit(std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) override;

  std::optional<facebook::velox::RowVectorPtr> next(
      uint64_t size,
      facebook::velox::ContinueFuture& future) override;

  const facebook::velox::common::SubfieldFilters* getFilters() const override {
    return &emptyFilters_;
  }

  void addDynamicFilter(
      facebook::velox::column_index_t outputChannel,
      const std::shared_ptr<facebook::velox::common::Filter>& filter) override;

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, facebook::velox::RuntimeMetric> getRuntimeStats() override;

 private:
  void validateSplitProjection(const KafkaSplitInfo& splitInfo) const;

  void finishCurrentSplit();

  facebook::velox::RowVectorPtr projectBatch(const facebook::velox::RowVectorPtr& fullBatch) const;

  facebook::velox::RowTypePtr outputType_;
  facebook::velox::connector::ColumnHandleMap columnHandles_;
  std::shared_ptr<KafkaConsumerFactory> consumerFactory_;
  facebook::velox::memory::MemoryPool* pool_;
  const facebook::velox::common::SubfieldFilters emptyFilters_;

  std::optional<KafkaSplitInfo> currentSplit_;
  std::unique_ptr<KafkaConsumer> currentConsumer_;
  std::unique_ptr<KafkaFiniteReader> currentReader_;
  bool currentSplitConsumed_{true};
  uint64_t completedBytes_{0};
  uint64_t completedRows_{0};
  uint64_t startedSplits_{0};
  uint64_t completedSplits_{0};
  uint64_t plannedMessages_{0};
  uint64_t readMessages_{0};
  uint64_t skippedMessages_{0};
  uint64_t watermarkLookups_{0};
  uint64_t seeks_{0};
  uint64_t polls_{0};
  uint64_t emptySplits_{0};
  uint64_t consumerCreations_{0};
  uint64_t readBatches_{0};
  uint64_t dynamicFiltersRejected_{0};
};

class KafkaConnector final : public facebook::velox::connector::Connector {
 public:
  static constexpr const char* kKafkaConnectorName = "gluten-kafka";

  KafkaConnector(
      std::string id,
      std::shared_ptr<const facebook::velox::config::ConfigBase> config,
      std::shared_ptr<KafkaConsumerFactory> consumerFactory = std::make_shared<UnsupportedKafkaConsumerFactory>());

  std::unique_ptr<facebook::velox::connector::DataSource> createDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::connector::ConnectorTableHandlePtr& tableHandle,
      const facebook::velox::connector::ColumnHandleMap& columnHandles,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<facebook::velox::connector::DataSink> createDataSink(
      facebook::velox::RowTypePtr inputType,
      facebook::velox::connector::ConnectorInsertTableHandlePtr connectorInsertTableHandle,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      facebook::velox::connector::CommitStrategy commitStrategy) override;

 private:
  std::shared_ptr<KafkaConsumerFactory> consumerFactory_;
};

} // namespace gluten::kafka
