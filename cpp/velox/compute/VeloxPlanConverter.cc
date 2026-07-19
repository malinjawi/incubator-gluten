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

#include "VeloxPlanConverter.h"
#include <filesystem>

#include "config/GlutenConfig.h"
#include "iceberg/IcebergPlanConverter.h"
#include "operators/plannodes/IteratorSplit.h"

namespace gluten {

using namespace facebook;

VeloxPlanConverter::VeloxPlanConverter(
    velox::memory::MemoryPool* veloxPool,
    const facebook::velox::config::ConfigBase* veloxCfg,
    const std::vector<std::shared_ptr<ResultIterator>>& rowVectors,
    VeloxConnectorIds connectorIds,
    const std::optional<std::string> writeFilesTempPath,
    const std::optional<std::string> writeFileName,
    bool validationMode)
    : validationMode_(validationMode),
      veloxCfg_(veloxCfg),
      substraitVeloxPlanConverter_(
          veloxPool,
          veloxCfg,
          rowVectors,
          std::move(connectorIds),
          writeFilesTempPath,
          writeFileName,
          validationMode) {
  VELOX_USER_CHECK_NOT_NULL(veloxCfg_);
}

namespace {
std::shared_ptr<SplitInfo> parseScanSplitInfo(
    const facebook::velox::config::ConfigBase* veloxCfg,
    const google::protobuf::RepeatedPtrField<substrait::ReadRel_LocalFiles_FileOrFiles>& fileList) {
  using SubstraitFileFormatCase = ::substrait::ReadRel_LocalFiles_FileOrFiles::FileFormatCase;

  auto splitInfo = std::make_shared<SplitInfo>();
  splitInfo->leafType = SplitInfo::LeafType::TABLE_SCAN;
  splitInfo->paths.reserve(fileList.size());
  splitInfo->starts.reserve(fileList.size());
  splitInfo->lengths.reserve(fileList.size());
  splitInfo->partitionColumns.reserve(fileList.size());
  splitInfo->properties.reserve(fileList.size());
  splitInfo->metadataColumns.reserve(fileList.size());
  for (const auto& file : fileList) {
    // Expect all Partitions share the same index.
    splitInfo->partitionIndex = file.partition_index();

    std::unordered_map<std::string, std::string> partitionColumnMap;
    for (const auto& partitionColumn : file.partition_columns()) {
      partitionColumnMap[partitionColumn.key()] = partitionColumn.value();
    }
    splitInfo->partitionColumns.emplace_back(partitionColumnMap);

    std::unordered_map<std::string, std::string> metadataColumnMap;
    for (const auto& metadataColumn : file.metadata_columns()) {
      metadataColumnMap[metadataColumn.key()] = metadataColumn.value();
    }
    splitInfo->metadataColumns.emplace_back(metadataColumnMap);

    splitInfo->paths.emplace_back(file.uri_file());
    splitInfo->starts.emplace_back(file.start());
    splitInfo->lengths.emplace_back(file.length());

    facebook::velox::FileProperties fileProps;
    if (file.has_properties()) {
      fileProps.fileSize = file.properties().filesize();
      fileProps.modificationTime = file.properties().modificationtime();
    }
    splitInfo->properties.emplace_back(fileProps);
    switch (file.file_format_case()) {
      case SubstraitFileFormatCase::kOrc:
        splitInfo->format = dwio::common::FileFormat::ORC;
        break;
      case SubstraitFileFormatCase::kDwrf:
        splitInfo->format = dwio::common::FileFormat::DWRF;
        break;
      case SubstraitFileFormatCase::kParquet:
        splitInfo->format = dwio::common::FileFormat::PARQUET;
        break;
      case SubstraitFileFormatCase::kText:
        splitInfo->format = dwio::common::FileFormat::TEXT;
        break;
      case SubstraitFileFormatCase::kIceberg:
        splitInfo = IcebergPlanConverter::parseIcebergSplitInfo(file, std::move(splitInfo));
        break;
      default:
        splitInfo->format = dwio::common::FileFormat::UNKNOWN;
        break;
    }

    // The schema in file represents the table schema, it is set when the TableScan requires the
    // table schema to be present, currently when the option is set to map columns by index rather
    // than by name in Parquet or ORC files. Since the table schema should be the same for all
    // files, we set it in the SplitInfo based on the first file we encounter with the schema set.
    if (!splitInfo->tableSchema && file.has_schema()) {
      const auto& schema = file.schema();

      std::vector<std::string> names;
      std::vector<TypePtr> types;
      names.reserve(schema.names().size());

      const bool asLowerCase = !veloxCfg->get<bool>(kCaseSensitive, false);
      for (const auto& name : schema.names()) {
        std::string fieldName = name;
        if (asLowerCase) {
          folly::toLowerAscii(fieldName);
        }
        names.emplace_back(std::move(fieldName));
      }
      types = SubstraitParser::parseNamedStruct(schema, asLowerCase);

      splitInfo->tableSchema = ROW(std::move(names), std::move(types));
    }
  }
  return splitInfo;
}

std::shared_ptr<SplitInfo> parseKafkaSplitInfo(const ::substrait::ReadRel_StreamKafka& split) {
  VELOX_USER_CHECK(split.has_topic_partition(), "Native Kafka split is missing topic partition.");
  VELOX_USER_CHECK(
      !split.topic_partition().topic().empty(),
      "Native Kafka split requires a non-empty topic, got partition={}, startOffset={}, endOffset={}.",
      split.topic_partition().partition(),
      split.start_offset(),
      split.end_offset());
  VELOX_USER_CHECK(
      split.topic_partition().partition() >= 0,
      "Native Kafka split requires a non-negative partition, got topic={}, partition={}.",
      split.topic_partition().topic(),
      split.topic_partition().partition());
  VELOX_USER_CHECK(
      split.poll_timeout_ms() >= 0,
      "Native Kafka split requires a non-negative poll timeout, got topic={}, partition={}, timeoutMs={}.",
      split.topic_partition().topic(),
      split.topic_partition().partition(),
      split.poll_timeout_ms());
  VELOX_USER_CHECK(
      split.start_offset() >= 0 && split.end_offset() >= 0,
      "Native Kafka split requires finite non-negative offsets, got topic={}, partition={}, startOffset={}, endOffset={}.",
      split.topic_partition().topic(),
      split.topic_partition().partition(),
      split.start_offset(),
      split.end_offset());
  VELOX_USER_CHECK(
      split.end_offset() >= split.start_offset(),
      "Native Kafka split requires endOffset >= startOffset, got topic={}, partition={}, startOffset={}, endOffset={}.",
      split.topic_partition().topic(),
      split.topic_partition().partition(),
      split.start_offset(),
      split.end_offset());

  auto splitInfo = std::make_shared<KafkaSplitInfo>();
  splitInfo->topic = split.topic_partition().topic();
  splitInfo->partition = split.topic_partition().partition();
  splitInfo->startOffset = split.start_offset();
  splitInfo->endOffset = split.end_offset();
  splitInfo->pollTimeoutMs = split.poll_timeout_ms();
  splitInfo->failOnDataLoss = split.fail_on_data_loss();
  splitInfo->includeHeaders = split.include_headers();
  for (const auto& param : split.params()) {
    splitInfo->params.emplace(param.first, param.second);
  }
  return splitInfo;
}

void parseSplitPayloads(
    SubstraitToVeloxPlanConverter* planConverter,
    const facebook::velox::config::ConfigBase* veloxCfg,
    const std::vector<SubstraitSplit>& splitPayloads) {
  std::vector<std::shared_ptr<SplitInfo>> splitInfos;
  splitInfos.reserve(splitPayloads.size());
  for (const auto& splitPayload : splitPayloads) {
    switch (splitPayload.kind) {
      case SubstraitSplit::Kind::kLocalFiles:
        splitInfos.push_back(parseScanSplitInfo(veloxCfg, splitPayload.localFiles.items()));
        break;
      case SubstraitSplit::Kind::kStreamKafka:
        splitInfos.push_back(parseKafkaSplitInfo(splitPayload.streamKafka));
        break;
    }
  }

  planConverter->setSplitInfos(std::move(splitInfos));
}
} // namespace

std::shared_ptr<const facebook::velox::core::PlanNode> VeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& substraitPlan,
    std::vector<::substrait::ReadRel_LocalFiles> localFiles) {
  std::vector<SubstraitSplit> splitPayloads;
  splitPayloads.reserve(localFiles.size());
  for (auto& localFile : localFiles) {
    splitPayloads.push_back(SubstraitSplit::makeLocalFiles(std::move(localFile)));
  }
  return toVeloxPlan(substraitPlan, std::move(splitPayloads));
}

std::shared_ptr<const facebook::velox::core::PlanNode> VeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& substraitPlan,
    std::vector<SubstraitSplit> splitPayloads) {
  if (!validationMode_) {
    parseSplitPayloads(&substraitVeloxPlanConverter_, veloxCfg_, splitPayloads);
  }

  return substraitVeloxPlanConverter_.toVeloxPlan(substraitPlan);
}

} // namespace gluten
