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

#include "compute/kafka/KafkaRdkafkaConsumer.h"

#include "velox/common/base/Exceptions.h"

#include <fmt/format.h>

#ifdef GLUTEN_ENABLE_LIBRDKAFKA
#include <librdkafka/rdkafka.h>
#endif

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

namespace gluten::kafka {
namespace {

constexpr const char* kBootstrapServers = "bootstrap.servers";
constexpr const char* kMetadataBrokerList = "metadata.broker.list";
constexpr const char* kEnableAutoCommit = "enable.auto.commit";
constexpr const char* kEnableAutoOffsetStore = "enable.auto.offset.store";
constexpr const char* kGroupId = "group.id";

bool startsWith(const std::string& value, const std::string& prefix) {
  return value.rfind(prefix, 0) == 0;
}

bool shouldForwardToRdkafka(const std::string& key) {
  static const std::unordered_set<std::string> kJvmOnlyParams{
      "interceptor.classes",
      "key.deserializer",
      "key.serializer",
      "metric.reporters",
      "auto.offset.reset",
      "auto.commit.enable",
      "partition.assignment.strategy",
      "receive.buffer.bytes",
      "value.deserializer",
      "value.serializer"};
  return !startsWith(key, "spark.") && kJvmOnlyParams.find(key) == kJvmOnlyParams.end();
}

#ifdef GLUTEN_ENABLE_LIBRDKAFKA
bool isSensitiveKafkaParam(const std::string& key) {
  return key.find("password") != std::string::npos || key.find("secret") != std::string::npos ||
      key.find("sasl.jaas.config") != std::string::npos || key.find("ssl.key") != std::string::npos;
}
#endif

std::unordered_map<std::string, std::string> makeRdkafkaDiscoveryParams(
    const std::unordered_map<std::string, std::string>& kafkaParams) {
  std::unordered_map<std::string, std::string> params;
  for (const auto& [key, value] : kafkaParams) {
    if (shouldForwardToRdkafka(key)) {
      params.emplace(key, value);
    }
  }
  VELOX_USER_CHECK(
      params.find(kBootstrapServers) != params.end() || params.find(kMetadataBrokerList) != params.end(),
      "Native Kafka partition discovery fatal config failure: requires '{}' or '{}' in Kafka params.",
      kBootstrapServers,
      kMetadataBrokerList);

  if (params.find(kGroupId) == params.end()) {
    params.emplace(kGroupId, "gluten-native-streaming-discovery");
  }

  // Discovery is metadata-only, but Spark must remain the only offset owner.
  params[kEnableAutoCommit] = "false";
  params[kEnableAutoOffsetStore] = "false";
  return params;
}

} // namespace

std::unordered_map<std::string, std::string> makeSparkOwnedKafkaConsumerParams(const KafkaPartitionRange& range) {
  std::unordered_map<std::string, std::string> params;
  for (const auto& [key, value] : range.params) {
    if (shouldForwardToRdkafka(key)) {
      params.emplace(key, value);
    }
  }
  VELOX_USER_CHECK(
      params.find(kBootstrapServers) != params.end() || params.find(kMetadataBrokerList) != params.end(),
      "Native Kafka consumer fatal config failure: requires '{}' or '{}' in Kafka params for topic={}, partition={}.",
      kBootstrapServers,
      kMetadataBrokerList,
      range.topic,
      range.partition);

  if (params.find(kGroupId) == params.end()) {
    params.emplace(kGroupId, fmt::format("gluten-native-streaming-{}-{}", range.topic, range.partition));
  }

  // Spark owns offset logs and checkpoint replay. Native Kafka must never commit
  // or store consumer offsets independently of Spark-planned finite ranges.
  params[kEnableAutoCommit] = "false";
  params[kEnableAutoOffsetStore] = "false";
  return params;
}

const char* kafkaConsumerFailureKindName(KafkaConsumerFailureKind kind) {
  switch (kind) {
    case KafkaConsumerFailureKind::EndOfPartition:
      return "end-of-partition";
    case KafkaConsumerFailureKind::Timeout:
      return "timeout";
    case KafkaConsumerFailureKind::Retriable:
      return "retriable";
    case KafkaConsumerFailureKind::DataLoss:
      return "data-loss";
    case KafkaConsumerFailureKind::Fatal:
      return "fatal";
  }
  return "unknown";
}

KafkaConsumerFailureKind classifyRdkafkaResponseErrorCode(int errorCode) {
#ifdef GLUTEN_ENABLE_LIBRDKAFKA
  switch (static_cast<rd_kafka_resp_err_t>(errorCode)) {
    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
      return KafkaConsumerFailureKind::EndOfPartition;
    case RD_KAFKA_RESP_ERR__TIMED_OUT:
    case RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE:
      return KafkaConsumerFailureKind::Timeout;
    case RD_KAFKA_RESP_ERR__TRANSPORT:
    case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
      return KafkaConsumerFailureKind::Retriable;
    case RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE:
      return KafkaConsumerFailureKind::DataLoss;
    default:
      return KafkaConsumerFailureKind::Fatal;
  }
#else
  (void)errorCode;
  return KafkaConsumerFailureKind::Fatal;
#endif
}

#ifdef GLUTEN_ENABLE_LIBRDKAFKA
namespace {

KafkaBytes copyBytes(const void* data, size_t size) {
  const auto* bytes = static_cast<const uint8_t*>(data);
  return KafkaBytes(bytes, bytes + size);
}

class RdkafkaConsumer final : public KafkaConsumer {
 public:
  explicit RdkafkaConsumer(const KafkaPartitionRange& range) {
    auto params = makeSparkOwnedKafkaConsumerParams(range);
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    for (const auto& [key, value] : params) {
      VELOX_USER_CHECK(
          rd_kafka_conf_set(conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) == RD_KAFKA_CONF_OK,
          "Native Kafka consumer fatal config failure: failed to configure option '{}': {}",
          key,
          isSensitiveKafkaParam(key) ? "<redacted>" : errstr);
    }

    consumer_ = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    VELOX_USER_CHECK_NOT_NULL(
        consumer_,
        "Native Kafka consumer fatal create failure: failed to create Kafka consumer: {}",
        errstr);
    rd_kafka_poll_set_consumer(consumer_);
  }

  ~RdkafkaConsumer() override {
    if (consumer_ != nullptr) {
      rd_kafka_consumer_close(consumer_);
      rd_kafka_destroy(consumer_);
    }
  }

  void seek(const std::string& topic, int32_t partition, int64_t offset) override {
    auto* assignment = rd_kafka_topic_partition_list_new(1);
    auto* topicPartition = rd_kafka_topic_partition_list_add(assignment, topic.c_str(), partition);
    topicPartition->offset = offset;
    const auto err = rd_kafka_assign(consumer_, assignment);
    rd_kafka_topic_partition_list_destroy(assignment);
    const auto kind = classifyRdkafkaResponseErrorCode(err);
    VELOX_USER_CHECK(
        err == RD_KAFKA_RESP_ERR_NO_ERROR,
        "Native Kafka consumer {} seek failure for topic={}, partition={}, offset={}: {}",
        kafkaConsumerFailureKindName(kind),
        topic,
        partition,
        offset,
        rd_kafka_err2str(err));
  }

  std::optional<KafkaRecord> poll(int64_t timeoutMs) override {
    auto* kafkaMessage = rd_kafka_consumer_poll(consumer_, static_cast<int>(timeoutMs));
    if (kafkaMessage == nullptr) {
      return std::nullopt;
    }
    std::unique_ptr<rd_kafka_message_t, decltype(&rd_kafka_message_destroy)> messageGuard(
        kafkaMessage, rd_kafka_message_destroy);

    if (kafkaMessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      const auto kind = classifyRdkafkaResponseErrorCode(kafkaMessage->err);
      switch (kind) {
        case KafkaConsumerFailureKind::EndOfPartition:
        case KafkaConsumerFailureKind::Timeout:
          return std::nullopt;
        case KafkaConsumerFailureKind::Retriable:
        case KafkaConsumerFailureKind::DataLoss:
        case KafkaConsumerFailureKind::Fatal:
          VELOX_USER_FAIL(
              "Native Kafka consumer {} poll failure while reading a Spark-planned finite range: {}",
              kafkaConsumerFailureKindName(kind),
              rd_kafka_message_errstr(kafkaMessage));
      }
    }

    KafkaRecord record;
    record.topic = rd_kafka_topic_name(kafkaMessage->rkt);
    record.partition = kafkaMessage->partition;
    record.offset = kafkaMessage->offset;
    record.key = kafkaMessage->key == nullptr ? std::optional<KafkaBytes>{}
                                              : copyBytes(kafkaMessage->key, kafkaMessage->key_len);
    record.value = kafkaMessage->payload == nullptr ? std::optional<KafkaBytes>{}
                                                    : copyBytes(kafkaMessage->payload, kafkaMessage->len);

    rd_kafka_timestamp_type_t timestampType = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
    const auto timestamp = rd_kafka_message_timestamp(kafkaMessage, &timestampType);
    record.timestamp = timestamp < 0 ? 0 : timestamp;
    switch (timestampType) {
      case RD_KAFKA_TIMESTAMP_CREATE_TIME:
        record.timestampType = 0;
        break;
      case RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME:
        record.timestampType = 1;
        break;
      case RD_KAFKA_TIMESTAMP_NOT_AVAILABLE:
      default:
        record.timestampType = -1;
        break;
    }

    rd_kafka_headers_t* headers = nullptr;
    if (rd_kafka_message_headers(kafkaMessage, &headers) == RD_KAFKA_RESP_ERR_NO_ERROR && headers != nullptr) {
      size_t index = 0;
      const char* name = nullptr;
      const void* value = nullptr;
      size_t size = 0;
      while (rd_kafka_header_get_all(headers, index++, &name, &value, &size) == RD_KAFKA_RESP_ERR_NO_ERROR) {
        record.headers.push_back(KafkaHeader{
            name == nullptr ? std::string{} : std::string(name),
            value == nullptr ? std::optional<KafkaBytes>{} : copyBytes(value, size)});
      }
    }

    return record;
  }

  std::optional<std::pair<int64_t, int64_t>> watermarkOffsets(
      const std::string& topic,
      int32_t partition,
      int64_t timeoutMs) override {
    int64_t low = 0;
    int64_t high = 0;
    const auto err = rd_kafka_query_watermark_offsets(
        consumer_, topic.c_str(), partition, &low, &high, static_cast<int>(timeoutMs));
    const auto kind = classifyRdkafkaResponseErrorCode(err);
    VELOX_USER_CHECK(
        err == RD_KAFKA_RESP_ERR_NO_ERROR,
        "Native Kafka consumer {} watermark lookup failure for topic={}, partition={}: {}",
        kafkaConsumerFailureKindName(kind),
        topic,
        partition,
        rd_kafka_err2str(err));
    return std::make_pair(low, high);
  }

 private:
  rd_kafka_t* consumer_{nullptr};
};

class RdkafkaConsumerFactory final : public KafkaConsumerFactory {
 public:
  std::unique_ptr<KafkaConsumer> create(const KafkaPartitionRange& range) override {
    return std::make_unique<RdkafkaConsumer>(range);
  }
};

} // namespace

bool rdkafkaSupportCompiled() {
  return true;
}

std::vector<KafkaTopicPartition> discoverTopicPartitions(
    const std::vector<std::string>& topics,
    const std::unordered_map<std::string, std::string>& kafkaParams,
    int64_t timeoutMs) {
  VELOX_USER_CHECK_GE(
      timeoutMs,
      0,
      "Native Kafka partition discovery requires a non-negative timeout, got {}.",
      timeoutMs);

  std::set<std::string> sortedTopics;
  for (const auto& topic : topics) {
    VELOX_USER_CHECK(
        !topic.empty(),
        "Native Kafka partition discovery requires non-empty topic names.");
    sortedTopics.insert(topic);
  }

  if (sortedTopics.empty()) {
    return {};
  }

  const auto params = makeRdkafkaDiscoveryParams(kafkaParams);
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  for (const auto& [key, value] : params) {
    VELOX_USER_CHECK(
        rd_kafka_conf_set(conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) == RD_KAFKA_CONF_OK,
        "Native Kafka partition discovery fatal config failure: failed to configure option '{}': {}",
        key,
        isSensitiveKafkaParam(key) ? "<redacted>" : errstr);
  }

  rd_kafka_t* rawClient = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (rawClient == nullptr) {
    rd_kafka_conf_destroy(conf);
    VELOX_USER_FAIL(
        "Native Kafka partition discovery fatal create failure: failed to create Kafka metadata client: {}",
        errstr);
  }
  std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> client(rawClient, rd_kafka_destroy);

  std::vector<KafkaTopicPartition> discovered;
  for (const auto& topic : sortedTopics) {
    std::unique_ptr<rd_kafka_topic_t, decltype(&rd_kafka_topic_destroy)> topicHandle(
        rd_kafka_topic_new(client.get(), topic.c_str(), nullptr), rd_kafka_topic_destroy);
    VELOX_USER_CHECK_NOT_NULL(
        topicHandle.get(),
        "Native Kafka partition discovery fatal topic-handle failure for topic={}.",
        topic);

    const rd_kafka_metadata_t* rawMetadata = nullptr;
    const auto err = rd_kafka_metadata(
        client.get(),
        0,
        topicHandle.get(),
        &rawMetadata,
        static_cast<int>(timeoutMs));
    const auto kind = classifyRdkafkaResponseErrorCode(err);
    VELOX_USER_CHECK(
        err == RD_KAFKA_RESP_ERR_NO_ERROR,
        "Native Kafka partition discovery {} metadata failure for topic={}: {}",
        kafkaConsumerFailureKindName(kind),
        topic,
        rd_kafka_err2str(err));

    std::unique_ptr<const rd_kafka_metadata_t, decltype(&rd_kafka_metadata_destroy)> metadata(
        rawMetadata, rd_kafka_metadata_destroy);
    bool matchedTopic = false;
    for (int topicIndex = 0; topicIndex < metadata->topic_cnt; ++topicIndex) {
      const auto& topicMetadata = metadata->topics[topicIndex];
      if (topicMetadata.topic == nullptr || topic != topicMetadata.topic) {
        continue;
      }
      matchedTopic = true;
      const auto topicErr = topicMetadata.err;
      const auto topicKind = classifyRdkafkaResponseErrorCode(topicErr);
      VELOX_USER_CHECK(
          topicErr == RD_KAFKA_RESP_ERR_NO_ERROR,
          "Native Kafka partition discovery {} topic metadata failure for topic={}: {}",
          kafkaConsumerFailureKindName(topicKind),
          topic,
          rd_kafka_err2str(topicErr));
      for (int partitionIndex = 0; partitionIndex < topicMetadata.partition_cnt; ++partitionIndex) {
        discovered.push_back(
            KafkaTopicPartition{topic, topicMetadata.partitions[partitionIndex].id});
      }
    }
    VELOX_USER_CHECK(
        matchedTopic,
        "Native Kafka partition discovery fatal metadata failure: broker response omitted topic={}.",
        topic);
  }

  std::sort(
      discovered.begin(),
      discovered.end(),
      [](const auto& left, const auto& right) {
        if (left.topic != right.topic) {
          return left.topic < right.topic;
        }
        return left.partition < right.partition;
      });
  return discovered;
}

std::vector<KafkaTopicPartition> listTopicPartitions(
    const std::unordered_map<std::string, std::string>& kafkaParams,
    int64_t timeoutMs) {
  std::vector<KafkaTopicPartition> discovered;
  for (const auto& topicPartition : listTopicPartitionMetadata(kafkaParams, timeoutMs)) {
    VELOX_USER_CHECK(
        topicPartition.error.empty(),
        "Native Kafka topic listing {} metadata failure for topic={} partition={}: {}",
        topicPartition.failureKind,
        topicPartition.topic,
        topicPartition.partition,
        topicPartition.error);
    discovered.push_back(KafkaTopicPartition{topicPartition.topic, topicPartition.partition});
  }
  return discovered;
}

std::vector<KafkaTopicPartitionMetadata> listTopicPartitionMetadata(
    const std::unordered_map<std::string, std::string>& kafkaParams,
    int64_t timeoutMs) {
  VELOX_USER_CHECK_GE(
      timeoutMs,
      0,
      "Native Kafka topic listing requires a non-negative timeout, got {}.",
      timeoutMs);

  const auto params = makeRdkafkaDiscoveryParams(kafkaParams);
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  for (const auto& [key, value] : params) {
    VELOX_USER_CHECK(
        rd_kafka_conf_set(conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) == RD_KAFKA_CONF_OK,
        "Native Kafka topic listing fatal config failure: failed to configure option '{}': {}",
        key,
        isSensitiveKafkaParam(key) ? "<redacted>" : errstr);
  }

  rd_kafka_t* rawClient = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (rawClient == nullptr) {
    rd_kafka_conf_destroy(conf);
    VELOX_USER_FAIL(
        "Native Kafka topic listing fatal create failure: failed to create Kafka metadata client: {}",
        errstr);
  }
  std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> client(rawClient, rd_kafka_destroy);

  const rd_kafka_metadata_t* rawMetadata = nullptr;
  const auto err = rd_kafka_metadata(client.get(), 1, nullptr, &rawMetadata, static_cast<int>(timeoutMs));
  const auto kind = classifyRdkafkaResponseErrorCode(err);
  VELOX_USER_CHECK(
      err == RD_KAFKA_RESP_ERR_NO_ERROR,
      "Native Kafka topic listing {} metadata failure: {}",
      kafkaConsumerFailureKindName(kind),
      rd_kafka_err2str(err));

  std::unique_ptr<const rd_kafka_metadata_t, decltype(&rd_kafka_metadata_destroy)> metadata(
      rawMetadata, rd_kafka_metadata_destroy);
  std::vector<KafkaTopicPartitionMetadata> discovered;
  for (int topicIndex = 0; topicIndex < metadata->topic_cnt; ++topicIndex) {
    const auto& topicMetadata = metadata->topics[topicIndex];
    VELOX_USER_CHECK_NOT_NULL(
        topicMetadata.topic,
        "Native Kafka topic listing fatal metadata failure: broker returned a null topic name.");
    const auto topicErr = topicMetadata.err;
    const auto topicKind = classifyRdkafkaResponseErrorCode(topicErr);
    if (topicErr != RD_KAFKA_RESP_ERR_NO_ERROR) {
      discovered.push_back(KafkaTopicPartitionMetadata{
          topicMetadata.topic,
          -1,
          kafkaConsumerFailureKindName(topicKind),
          rd_kafka_err2str(topicErr)});
      continue;
    }
    for (int partitionIndex = 0; partitionIndex < topicMetadata.partition_cnt; ++partitionIndex) {
      const auto& partitionMetadata = topicMetadata.partitions[partitionIndex];
      const auto partitionErr = partitionMetadata.err;
      const auto partitionKind = classifyRdkafkaResponseErrorCode(partitionErr);
      discovered.push_back(KafkaTopicPartitionMetadata{
          topicMetadata.topic,
          partitionMetadata.id,
          kafkaConsumerFailureKindName(partitionKind),
          partitionErr == RD_KAFKA_RESP_ERR_NO_ERROR ? "" : rd_kafka_err2str(partitionErr)});
    }
  }

  std::sort(
      discovered.begin(),
      discovered.end(),
      [](const auto& left, const auto& right) {
        if (left.topic != right.topic) {
          return left.topic < right.topic;
        }
        return left.partition < right.partition;
      });
  return discovered;
}

std::shared_ptr<KafkaConsumerFactory> createRdkafkaConsumerFactory() {
  return std::make_shared<RdkafkaConsumerFactory>();
}

#else

bool rdkafkaSupportCompiled() {
  return false;
}

std::vector<KafkaTopicPartition> discoverTopicPartitions(
    const std::vector<std::string>& topics,
    const std::unordered_map<std::string, std::string>& kafkaParams,
    int64_t timeoutMs) {
  (void)topics;
  (void)kafkaParams;
  (void)timeoutMs;
  VELOX_USER_FAIL(
      "Native Kafka partition discovery requires librdkafka support. "
      "Rebuild with ENABLE_VELOX_KAFKA_CLIENT=ON.");
}

std::vector<KafkaTopicPartition> listTopicPartitions(
    const std::unordered_map<std::string, std::string>& kafkaParams,
    int64_t timeoutMs) {
  (void)kafkaParams;
  (void)timeoutMs;
  VELOX_USER_FAIL(
      "Native Kafka topic listing requires librdkafka support. "
      "Rebuild with ENABLE_VELOX_KAFKA_CLIENT=ON.");
}

std::vector<KafkaTopicPartitionMetadata> listTopicPartitionMetadata(
    const std::unordered_map<std::string, std::string>& kafkaParams,
    int64_t timeoutMs) {
  (void)kafkaParams;
  (void)timeoutMs;
  VELOX_USER_FAIL(
      "Native Kafka topic listing requires librdkafka support. "
      "Rebuild with ENABLE_VELOX_KAFKA_CLIENT=ON.");
}

std::shared_ptr<KafkaConsumerFactory> createRdkafkaConsumerFactory() {
  return std::make_shared<UnsupportedKafkaConsumerFactory>();
}

#endif

} // namespace gluten::kafka
