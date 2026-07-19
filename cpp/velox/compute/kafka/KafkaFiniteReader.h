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
#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace gluten::kafka {

using KafkaBytes = std::vector<uint8_t>;

struct KafkaHeader {
  std::string key;
  std::optional<KafkaBytes> value;
};

struct KafkaRecord {
  std::optional<KafkaBytes> key;
  std::optional<KafkaBytes> value;
  std::string topic;
  int32_t partition{0};
  int64_t offset{0};
  int64_t timestamp{0};
  int32_t timestampType{0};
  std::vector<KafkaHeader> headers;
};

struct KafkaPartitionRange {
  std::string topic;
  int32_t partition{0};
  int64_t startOffset{0};
  int64_t endOffset{0};
  int64_t pollTimeoutMs{0};
  bool failOnDataLoss{false};
  bool includeHeaders{false};
  std::unordered_map<std::string, std::string> params;
};

struct KafkaTopicPartition {
  std::string topic;
  int32_t partition{0};
};

struct KafkaMicroBatchRangeRequest {
  std::string topic;
  int32_t partition{0};
  int64_t startOffset{0};
  std::optional<uint64_t> maxOffsetsPerTrigger;
  int64_t pollTimeoutMs{0};
  bool failOnDataLoss{false};
  bool includeHeaders{false};
  std::unordered_map<std::string, std::string> params;
};

struct KafkaMicroBatchRangePlan {
  KafkaPartitionRange range;
  int64_t lowWatermark{0};
  int64_t highWatermark{0};
  uint64_t skippedMessages{0};
  bool advancedStartOffset{false};
  bool resetStartToHighWatermark{false};
  bool limitedByMaxOffsetsPerTrigger{false};
};

class KafkaConsumer {
 public:
  virtual ~KafkaConsumer() = default;

  virtual void seek(const std::string& topic, int32_t partition, int64_t offset) = 0;

  virtual std::optional<KafkaRecord> poll(int64_t timeoutMs) = 0;

  virtual std::optional<std::pair<int64_t, int64_t>> watermarkOffsets(
      const std::string& /*topic*/,
      int32_t /*partition*/,
      int64_t /*timeoutMs*/) {
    return std::nullopt;
  }
};

class KafkaFiniteReader {
 public:
  KafkaFiniteReader(KafkaPartitionRange range, KafkaConsumer* consumer);

  std::vector<KafkaRecord> readNext(size_t maxRecords);

  std::vector<KafkaRecord> readAll();

  bool done() const {
    return nextOffset_ >= range_.endOffset;
  }

  uint64_t skippedMessages() const {
    return skippedMessages_;
  }

  uint64_t watermarkLookups() const {
    return watermarkLookups_;
  }

  uint64_t seeks() const {
    return seeks_;
  }

  uint64_t polls() const {
    return polls_;
  }

 private:
  KafkaRecord normalizeRecord(KafkaRecord record) const;

  void skipToOffset(int64_t offset);

  KafkaPartitionRange range_;
  KafkaConsumer* consumer_;
  int64_t nextOffset_;
  uint64_t skippedMessages_{0};
  uint64_t watermarkLookups_{0};
  uint64_t seeks_{0};
  uint64_t polls_{0};
  bool started_{false};
};

void validateKafkaPartitionRange(const KafkaPartitionRange& range);

KafkaMicroBatchRangePlan planKafkaMicroBatchRange(
    const KafkaMicroBatchRangeRequest& request,
    KafkaConsumer* consumer);

facebook::velox::RowTypePtr sparkKafkaRowType(bool includeHeaders);

facebook::velox::RowVectorPtr makeKafkaRowVector(
    const std::vector<KafkaRecord>& records,
    bool includeHeaders,
    facebook::velox::memory::MemoryPool* pool);

} // namespace gluten::kafka
