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

#include "compute/kafka/KafkaConnector.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace gluten::kafka {

enum class KafkaConsumerFailureKind {
  EndOfPartition,
  Timeout,
  Retriable,
  DataLoss,
  Fatal,
};

struct KafkaTopicPartitionMetadata {
  std::string topic;
  int32_t partition;
  std::string failureKind;
  std::string error;
};

std::unordered_map<std::string, std::string> makeSparkOwnedKafkaConsumerParams(const KafkaPartitionRange& range);

const char* kafkaConsumerFailureKindName(KafkaConsumerFailureKind kind);

KafkaConsumerFailureKind classifyRdkafkaResponseErrorCode(int errorCode);

bool rdkafkaSupportCompiled();

std::vector<KafkaTopicPartition> discoverTopicPartitions(
    const std::vector<std::string>& topics,
    const std::unordered_map<std::string, std::string>& kafkaParams,
    int64_t timeoutMs);

std::vector<KafkaTopicPartition> listTopicPartitions(
    const std::unordered_map<std::string, std::string>& kafkaParams,
    int64_t timeoutMs);

std::vector<KafkaTopicPartitionMetadata> listTopicPartitionMetadata(
    const std::unordered_map<std::string, std::string>& kafkaParams,
    int64_t timeoutMs);

std::shared_ptr<KafkaConsumerFactory> createRdkafkaConsumerFactory();

} // namespace gluten::kafka
