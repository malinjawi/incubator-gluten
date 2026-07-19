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

#include "compute/kafka/KafkaFiniteReader.h"

#include "velox/common/base/Exceptions.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"

#include <algorithm>
#include <limits>
#include <iterator>
#include <memory>
#include <utility>

namespace gluten::kafka {
namespace {

using facebook::velox::ARRAY;
using facebook::velox::BIGINT;
using facebook::velox::BaseVector;
using facebook::velox::FlatVector;
using facebook::velox::INTEGER;
using facebook::velox::ROW;
using facebook::velox::RowVector;
using facebook::velox::StringView;
using facebook::velox::TIMESTAMP;
using facebook::velox::Timestamp;
using facebook::velox::VARBINARY;
using facebook::velox::VARCHAR;
using facebook::velox::VectorPtr;
using facebook::velox::vector_size_t;

void setBytes(
    FlatVector<StringView>* vector,
    vector_size_t row,
    const std::optional<KafkaBytes>& value) {
  if (!value.has_value()) {
    vector->setNull(row, true);
    return;
  }

  const auto& bytes = value.value();
  vector->set(row, StringView(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
}

void setString(FlatVector<StringView>* vector, vector_size_t row, const std::string& value) {
  vector->set(row, StringView(value.data(), value.size()));
}

facebook::velox::ArrayVectorPtr makeHeadersVector(
    const std::vector<KafkaRecord>& records,
    vector_size_t rows,
    facebook::velox::memory::MemoryPool* pool) {
  vector_size_t numHeaders = 0;
  for (const auto& record : records) {
    VELOX_USER_CHECK_LE(
        record.headers.size(),
        static_cast<size_t>(std::numeric_limits<vector_size_t>::max() - numHeaders),
        "Native Kafka reader cannot materialize more than {} headers in one batch.",
        std::numeric_limits<vector_size_t>::max());
    numHeaders += static_cast<vector_size_t>(record.headers.size());
  }

  auto offsets = facebook::velox::allocateOffsets(rows, pool);
  auto sizes = facebook::velox::allocateSizes(rows, pool);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();

  auto headerKeyVector = BaseVector::create<FlatVector<StringView>>(VARCHAR(), numHeaders, pool);
  auto headerValueVector = BaseVector::create<FlatVector<StringView>>(VARBINARY(), numHeaders, pool);

  vector_size_t headerIndex = 0;
  for (vector_size_t row = 0; row < rows; ++row) {
    rawOffsets[row] = headerIndex;
    rawSizes[row] = static_cast<vector_size_t>(records[row].headers.size());
    for (const auto& header : records[row].headers) {
      setString(headerKeyVector.get(), headerIndex, header.key);
      setBytes(headerValueVector.get(), headerIndex, header.value);
      ++headerIndex;
    }
  }

  auto headerType = ROW({"key", "value"}, {VARCHAR(), VARBINARY()});
  auto elements = std::make_shared<RowVector>(
      pool,
      headerType,
      nullptr,
      numHeaders,
      std::vector<VectorPtr>{headerKeyVector, headerValueVector});
  return std::make_shared<facebook::velox::ArrayVector>(
      pool, ARRAY(headerType), nullptr, rows, offsets, sizes, elements);
}

void validateWatermarkOffsets(
    const KafkaPartitionRange& range,
    int64_t lowWatermark,
    int64_t highWatermark) {
  VELOX_USER_CHECK(
      lowWatermark >= 0 && highWatermark >= 0 && highWatermark >= lowWatermark,
      "Native Kafka reader fatal watermark lookup failure: requires non-negative monotonic "
      "watermark offsets for topic={}, partition={}, watermarks=[{}, {}).",
      range.topic,
      range.partition,
      lowWatermark,
      highWatermark);
}

KafkaPartitionRange requestAsEmptyRange(const KafkaMicroBatchRangeRequest& request) {
  return KafkaPartitionRange{
      request.topic,
      request.partition,
      request.startOffset,
      request.startOffset,
      request.pollTimeoutMs,
      request.failOnDataLoss,
      request.includeHeaders,
      request.params};
}

} // namespace

void validateKafkaPartitionRange(const KafkaPartitionRange& range) {
  VELOX_USER_CHECK(
      !range.topic.empty(),
      "Native Kafka reader fatal finite-range failure: requires a non-empty topic.");
  VELOX_USER_CHECK(
      range.partition >= 0,
      "Native Kafka reader fatal finite-range failure: requires a non-negative partition.");
  VELOX_USER_CHECK(
      range.pollTimeoutMs >= 0,
      "Native Kafka reader fatal finite-range failure: requires a non-negative poll timeout for "
      "topic={}, partition={}, timeoutMs={}.",
      range.topic,
      range.partition,
      range.pollTimeoutMs);
  VELOX_USER_CHECK(
      range.startOffset >= 0 && range.endOffset >= 0,
      "Native Kafka reader fatal finite-range failure: requires finite non-negative offsets for "
      "topic={}, partition={}, offsets=[{}, {}).",
      range.topic,
      range.partition,
      range.startOffset,
      range.endOffset);
  VELOX_USER_CHECK(
      range.endOffset >= range.startOffset,
      "Native Kafka reader fatal finite-range failure: requires endOffset >= startOffset for "
      "topic={}, partition={}, offsets=[{}, {}).",
      range.topic,
      range.partition,
      range.startOffset,
      range.endOffset);
}

KafkaMicroBatchRangePlan planKafkaMicroBatchRange(
    const KafkaMicroBatchRangeRequest& request,
    KafkaConsumer* consumer) {
  VELOX_USER_CHECK_NOT_NULL(consumer);

  auto range = requestAsEmptyRange(request);
  validateKafkaPartitionRange(range);

  auto watermarks = consumer->watermarkOffsets(request.topic, request.partition, request.pollTimeoutMs);
  VELOX_USER_CHECK(
      watermarks.has_value(),
      "Native Kafka source fatal offset-planning failure: broker watermarks are required before "
      "planning a native-owned micro-batch range for topic={}, partition={}.",
      request.topic,
      request.partition);

  const auto lowWatermark = watermarks->first;
  const auto highWatermark = watermarks->second;
  validateWatermarkOffsets(range, lowWatermark, highWatermark);

  auto plannedStartOffset = request.startOffset;
  uint64_t skippedMessages = 0;
  bool advancedStartOffset = false;
  bool resetStartToHighWatermark = false;

  if (plannedStartOffset < lowWatermark) {
    if (request.failOnDataLoss) {
      VELOX_USER_FAIL(
          "Native Kafka source data-loss offset-planning failure: checkpoint startOffset {} is "
          "before earliest available offset {} for topic={}, partition={}.",
          plannedStartOffset,
          lowWatermark,
          request.topic,
          request.partition);
    }
    skippedMessages = static_cast<uint64_t>(lowWatermark - plannedStartOffset);
    plannedStartOffset = lowWatermark;
    advancedStartOffset = true;
  }

  if (plannedStartOffset > highWatermark) {
    if (request.failOnDataLoss) {
      VELOX_USER_FAIL(
          "Native Kafka source data-loss offset-planning failure: checkpoint startOffset {} is "
          "after latest available offset {} for topic={}, partition={}.",
          plannedStartOffset,
          highWatermark,
          request.topic,
          request.partition);
    }
    plannedStartOffset = highWatermark;
    resetStartToHighWatermark = true;
  }

  auto plannedEndOffset = highWatermark;
  bool limitedByMaxOffsetsPerTrigger = false;
  const auto available = static_cast<uint64_t>(highWatermark - plannedStartOffset);
  if (request.maxOffsetsPerTrigger.has_value() &&
      request.maxOffsetsPerTrigger.value() < available) {
    plannedEndOffset = plannedStartOffset +
        static_cast<int64_t>(request.maxOffsetsPerTrigger.value());
    limitedByMaxOffsetsPerTrigger = true;
  }

  range.startOffset = plannedStartOffset;
  range.endOffset = plannedEndOffset;
  return KafkaMicroBatchRangePlan{
      range,
      lowWatermark,
      highWatermark,
      skippedMessages,
      advancedStartOffset,
      resetStartToHighWatermark,
      limitedByMaxOffsetsPerTrigger};
}

KafkaFiniteReader::KafkaFiniteReader(KafkaPartitionRange range, KafkaConsumer* consumer)
    : range_(std::move(range)), consumer_(consumer), nextOffset_(range_.startOffset) {
  validateKafkaPartitionRange(range_);
  VELOX_USER_CHECK_NOT_NULL(consumer_);
}

std::vector<KafkaRecord> KafkaFiniteReader::readNext(size_t maxRecords) {
  std::vector<KafkaRecord> records;
  if (maxRecords == 0 || done()) {
    return records;
  }

  if (!started_) {
    ++watermarkLookups_;
    if (auto watermarks = consumer_->watermarkOffsets(range_.topic, range_.partition, range_.pollTimeoutMs)) {
      const auto lowWatermark = watermarks->first;
      const auto highWatermark = watermarks->second;
      validateWatermarkOffsets(range_, lowWatermark, highWatermark);
      if (nextOffset_ < lowWatermark) {
        if (range_.failOnDataLoss) {
          VELOX_USER_FAIL(
              "Native Kafka reader data-loss finite-range failure: planned startOffset {} is before "
              "earliest available offset {} for "
              "topic={}, partition={}.",
              nextOffset_,
              lowWatermark,
              range_.topic,
              range_.partition);
        }
        skipToOffset(std::min(lowWatermark, range_.endOffset));
      }
      if (range_.endOffset > highWatermark) {
        if (range_.failOnDataLoss) {
          VELOX_USER_FAIL(
              "Native Kafka reader data-loss finite-range failure: planned endOffset {} is after "
              "latest available offset {} for "
              "topic={}, partition={}.",
              range_.endOffset,
              highWatermark,
              range_.topic,
              range_.partition);
        }
      }
    }
    if (!done()) {
      consumer_->seek(range_.topic, range_.partition, nextOffset_);
      ++seeks_;
    }
    started_ = true;
  }

  while (records.size() < maxRecords && !done()) {
    ++polls_;
    auto record = consumer_->poll(range_.pollTimeoutMs);
    if (!record.has_value()) {
      VELOX_USER_FAIL(
          "Native Kafka reader timeout poll failure before Spark planned end offset: expected "
          "offset {} before endOffset {} for topic={}, partition={}, but polling returned no record.",
          nextOffset_,
          range_.endOffset,
          range_.topic,
          range_.partition);
    }

    VELOX_USER_CHECK(
        record->topic == range_.topic && record->partition == range_.partition,
        "Native Kafka reader fatal finite-range failure: received record for topic={}, partition={} "
        "while reading topic={}, partition={}.",
        record->topic,
        record->partition,
        range_.topic,
        range_.partition);
    VELOX_USER_CHECK_GE(
        record->offset,
        0,
        "Native Kafka reader fatal finite-range failure: received negative offset {} for "
        "topic={}, partition={} while reading Spark-planned offsets=[{}, {}).",
        record->offset,
        range_.topic,
        range_.partition,
        range_.startOffset,
        range_.endOffset);

    if (record->offset < nextOffset_) {
      continue;
    }

    if (record->offset > nextOffset_ && range_.failOnDataLoss) {
      VELOX_USER_FAIL(
          "Native Kafka reader data-loss finite-range failure: detected offset gap for "
          "topic={}, partition={}: expected offset {}, got {}.",
          range_.topic,
          range_.partition,
          nextOffset_,
          record->offset);
    }

    if (record->offset >= range_.endOffset) {
      skipToOffset(range_.endOffset);
      break;
    }

    if (record->offset > nextOffset_) {
      skipToOffset(record->offset);
    }
    nextOffset_ = record->offset + 1;
    records.push_back(normalizeRecord(std::move(record.value())));
  }
  return records;
}

std::vector<KafkaRecord> KafkaFiniteReader::readAll() {
  std::vector<KafkaRecord> records;
  while (!done()) {
    auto batch = readNext(std::numeric_limits<size_t>::max());
    records.insert(
        records.end(),
        std::make_move_iterator(batch.begin()),
        std::make_move_iterator(batch.end()));
  }
  return records;
}

KafkaRecord KafkaFiniteReader::normalizeRecord(KafkaRecord record) const {
  if (!range_.includeHeaders) {
    record.headers.clear();
  }
  return record;
}

void KafkaFiniteReader::skipToOffset(int64_t offset) {
  VELOX_DCHECK_GE(offset, nextOffset_);
  VELOX_DCHECK_LE(offset, range_.endOffset);
  skippedMessages_ += static_cast<uint64_t>(offset - nextOffset_);
  nextOffset_ = offset;
}

facebook::velox::RowTypePtr sparkKafkaRowType(bool includeHeaders) {
  std::vector<std::string> names{
      "key",
      "value",
      "topic",
      "partition",
      "offset",
      "timestamp",
      "timestampType"};
  std::vector<std::shared_ptr<const facebook::velox::Type>> types{
      VARBINARY(),
      VARBINARY(),
      VARCHAR(),
      INTEGER(),
      BIGINT(),
      TIMESTAMP(),
      INTEGER()};

  if (includeHeaders) {
    names.emplace_back("headers");
    types.emplace_back(ARRAY(ROW({"key", "value"}, {VARCHAR(), VARBINARY()})));
  }
  return ROW(std::move(names), std::move(types));
}

facebook::velox::RowVectorPtr makeKafkaRowVector(
    const std::vector<KafkaRecord>& records,
    bool includeHeaders,
    facebook::velox::memory::MemoryPool* pool) {
  VELOX_USER_CHECK_NOT_NULL(pool, "Native Kafka reader requires a memory pool to materialize a batch.");
  VELOX_USER_CHECK_LE(
      records.size(),
      static_cast<size_t>(std::numeric_limits<vector_size_t>::max()),
      "Native Kafka reader cannot materialize more than {} rows in one batch.",
      std::numeric_limits<vector_size_t>::max());

  const auto rows = static_cast<vector_size_t>(records.size());
  auto keyVector = BaseVector::create<FlatVector<StringView>>(VARBINARY(), rows, pool);
  auto valueVector = BaseVector::create<FlatVector<StringView>>(VARBINARY(), rows, pool);
  auto topicVector = BaseVector::create<FlatVector<StringView>>(VARCHAR(), rows, pool);
  auto partitionVector = BaseVector::create<FlatVector<int32_t>>(INTEGER(), rows, pool);
  auto offsetVector = BaseVector::create<FlatVector<int64_t>>(BIGINT(), rows, pool);
  auto timestampVector = BaseVector::create<FlatVector<Timestamp>>(TIMESTAMP(), rows, pool);
  auto timestampTypeVector = BaseVector::create<FlatVector<int32_t>>(INTEGER(), rows, pool);

  for (vector_size_t row = 0; row < rows; ++row) {
    const auto& record = records[row];
    setBytes(keyVector.get(), row, record.key);
    setBytes(valueVector.get(), row, record.value);
    setString(topicVector.get(), row, record.topic);
    partitionVector->set(row, record.partition);
    offsetVector->set(row, record.offset);
    timestampVector->set(row, Timestamp::fromMillis(record.timestamp));
    timestampTypeVector->set(row, record.timestampType);
  }

  std::vector<VectorPtr> children{
      keyVector,
      valueVector,
      topicVector,
      partitionVector,
      offsetVector,
      timestampVector,
      timestampTypeVector};
  if (includeHeaders) {
    children.emplace_back(makeHeadersVector(records, rows, pool));
  }

  return std::make_shared<RowVector>(pool, sparkKafkaRowType(includeHeaders), nullptr, rows, std::move(children));
}

} // namespace gluten::kafka
