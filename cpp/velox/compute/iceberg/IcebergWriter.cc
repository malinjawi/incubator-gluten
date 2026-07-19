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

#include "IcebergWriter.h"

#include <algorithm>
#include <cctype>

#include <fmt/format.h>
#include <folly/json.h>

#include "IcebergPartitionSpec.pb.h"
#include "compute/ProtobufUtils.h"
#include "compute/iceberg/IcebergFormat.h"
#include "config/VeloxConfig.h"
#include "utils/ConfigExtractor.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::connector::hive::iceberg;
namespace {

std::string fileExtensionFor(dwio::common::FileFormat fileFormat) {
  std::string extension(dwio::common::toString(fileFormat));
  std::transform(extension.begin(), extension.end(), extension.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  if (extension == "dwrf") {
    return "orc";
  }
  return extension;
}

std::string sanitizeOperationId(std::string operationId) {
  for (auto& c : operationId) {
    const auto ch = static_cast<unsigned char>(c);
    if (!std::isalnum(ch) && c != '-' && c != '_' && c != '.') {
      c = '_';
    }
  }
  return operationId.empty() ? "unknown" : operationId;
}

std::string childPath(const std::string& directory, const std::string& fileName) {
  if (directory.empty() || directory.back() == '/') {
    return directory + fileName;
  }
  return directory + "/" + fileName;
}

class GlutenIcebergFileNameGenerator : public connector::hive::FileNameGenerator {
 public:
  GlutenIcebergFileNameGenerator(
      int32_t partitionId,
      int64_t taskId,
      std::string operationId,
      dwio::common::FileFormat fileFormat)
      : partitionId_(partitionId),
        taskId_(taskId),
        operationId_(sanitizeOperationId(std::move(operationId))),
        fileExtension_(fileExtensionFor(fileFormat)) {}

  std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const connector::hive::HiveInsertTableHandle> insertTableHandle,
      const connector::ConnectorQueryCtx& connectorQueryCtx,
      bool commitRequired) const override {
    auto targetFileName = insertTableHandle->locationHandle()->targetFileName();
    if (targetFileName.empty()) {
      targetFileName =
          fmt::format("{:05d}-{}-{:05d}.{}", partitionId_, operationId_, ++fileCount_, fileExtension_);
      cleanupExistingRetryFile(targetFileName, insertTableHandle);
    }
    return {targetFileName, targetFileName};
  }

  folly::dynamic serialize() const override {
    VELOX_UNREACHABLE("GlutenIcebergFileNameGenerator is local to native Iceberg writes.");
  }

  std::string toString() const override {
    return fmt::format(
        "GlutenIcebergFileNameGenerator(partitionId={}, taskId={}, operationId={})",
        partitionId_,
        taskId_,
        operationId_);
  }

 private:
  void cleanupExistingRetryFile(
      const std::string& targetFileName,
      const std::shared_ptr<const connector::hive::HiveInsertTableHandle>& insertTableHandle) const {
    const auto targetPath = childPath(insertTableHandle->locationHandle()->targetPath(), targetFileName);
    auto fileSystem = filesystems::getFileSystem(
        targetPath, std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>()));
    if (fileSystem->exists(targetPath)) {
      fileSystem->remove(targetPath);
    }
  }

  const int32_t partitionId_;
  const int64_t taskId_;
  const std::string operationId_;
  const std::string fileExtension_;
  mutable int32_t fileCount_{0};
};

parquet::ParquetFieldId convertToParquetFieldId(const gluten::IcebergNestedField& protoField) {
  std::vector<parquet::ParquetFieldId> children;
  children.reserve(protoField.children_size());
  for (const auto& protoChild : protoField.children()) {
    children.push_back(convertToParquetFieldId(protoChild));
  }

  return parquet::ParquetFieldId{protoField.id(), std::move(children)};
}

std::shared_ptr<IcebergInsertTableHandle> createIcebergInsertTableHandle(
    const RowTypePtr& outputRowType,
    const std::string& outputDirectoryPath,
    dwio::common::FileFormat fileFormat,
    facebook::velox::common::CompressionKind compressionKind,
    int32_t partitionId,
    int64_t taskId,
    const std::string& operationId,
    std::shared_ptr<const IcebergPartitionSpec> spec,
    const parquet::ParquetFieldId& nestedField) {
  std::vector<std::shared_ptr<const iceberg::IcebergColumnHandle>> columnHandles;

  std::vector<std::string> columnNames = outputRowType->names();
  columnHandles.reserve(columnNames.size());
  std::vector<TypePtr> columnTypes = outputRowType->children();
  std::vector<std::string> partitionColumns;
  if (spec != nullptr) {
    partitionColumns.reserve(spec->fields.size());
    for (const auto& field : spec->fields) {
      partitionColumns.push_back(field.name);
    }
  }
  for (auto i = 0; i < columnNames.size(); ++i) {
    if (std::find(partitionColumns.begin(), partitionColumns.end(), columnNames[i]) != partitionColumns.end()) {
      columnHandles.push_back(std::make_shared<iceberg::IcebergColumnHandle>(
          columnNames.at(i),
          connector::hive::HiveColumnHandle::ColumnType::kPartitionKey,
          columnTypes.at(i),
          nestedField.children.at(i)));
    } else {
      columnHandles.push_back(std::make_shared<iceberg::IcebergColumnHandle>(
          columnNames.at(i),
          connector::hive::HiveColumnHandle::ColumnType::kRegular,
          columnTypes.at(i),
          nestedField.children.at(i)));
    }
  }

  auto fileNameGenerator =
      std::make_shared<const GlutenIcebergFileNameGenerator>(partitionId, taskId, operationId, fileFormat);
  std::shared_ptr<const connector::hive::LocationHandle> locationHandle =
      std::make_shared<connector::hive::LocationHandle>(
          outputDirectoryPath, outputDirectoryPath, connector::hive::LocationHandle::TableType::kExisting);
  const std::unordered_map<std::string, std::string> serdeParameters;
  return std::make_shared<connector::hive::iceberg::IcebergInsertTableHandle>(
      columnHandles,
      locationHandle,
      fileFormat,
      spec,
      compressionKind,
      serdeParameters,
      fileNameGenerator);
}

} // namespace

namespace gluten {
IcebergWriter::IcebergWriter(
    const RowTypePtr& rowType,
    int32_t format,
    const std::string& outputDirectory,
    facebook::velox::common::CompressionKind compressionKind,
    int32_t partitionId,
    int64_t taskId,
    const std::string& operationId,
    std::shared_ptr<const iceberg::IcebergPartitionSpec> spec,
    const gluten::IcebergNestedField& field,
    const std::unordered_map<std::string, std::string>& sparkConfs,
    std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool,
    std::shared_ptr<facebook::velox::memory::MemoryPool> connectorPool)
    : rowType_(rowType),
      field_(convertToParquetFieldId(field)),
      pool_(memoryPool),
      connectorPool_(connectorPool),
      createTimeNs_(getCurrentTimeNano()) {
  auto veloxCfg =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(sparkConfs));
  connectorSessionProperties_ = createHiveConnectorSessionConfig(veloxCfg);
  connectorConfig_ =
      std::make_shared<facebook::velox::connector::hive::HiveConfig>(createHiveConnectorConfig(veloxCfg));
  icebergConfig_ = std::make_shared<iceberg::IcebergConfig>(veloxCfg);
  connectorQueryCtx_ = std::make_unique<connector::ConnectorQueryCtx>(
      pool_.get(),
      connectorPool_.get(),
      connectorSessionProperties_.get(),
      nullptr,
      common::PrefixSortConfig(),
      nullptr,
      nullptr,
      "query.IcebergDataSink",
      "task.IcebergDataSink",
      "planNodeId.IcebergDataSink",
      0,
      "");

  dataSink_ = std::make_unique<IcebergDataSink>(
      rowType_,
      createIcebergInsertTableHandle(
          rowType_,
          outputDirectory,
          icebergFormatToVelox(format),
          compressionKind,
          partitionId,
          taskId,
          operationId,
          spec,
          field_),
      connectorQueryCtx_.get(),
      facebook::velox::connector::CommitStrategy::kNoCommit,
      connectorConfig_,
      icebergConfig_);
}

const char* IcebergWriter::stateName() const {
  switch (state_) {
    case WriterState::kActive:
      return "active";
    case WriterState::kCommitted:
      return "committed";
    case WriterState::kAborted:
      return "aborted";
  }
  VELOX_UNREACHABLE();
}

void IcebergWriter::checkActive(std::string_view action) const {
  VELOX_CHECK(
      state_ == WriterState::kActive,
      "Cannot {} native Iceberg writer after it has been {}.",
      action,
      stateName());
}

void IcebergWriter::write(const VeloxColumnarBatch& batch) {
  checkActive("write");
  auto inputRowVector = batch.getRowVector();
  auto inputRowType = asRowType(inputRowVector->type());

  if (inputRowType->size() != rowType_->size()) {
    const auto& children = inputRowVector->children();
    std::vector<VectorPtr> dataColumns(children.begin() + 1, children.begin() + 1 + rowType_->size());

    auto filteredRowVector = std::make_shared<RowVector>(
        pool_.get(), rowType_, inputRowVector->nulls(), inputRowVector->size(), std::move(dataColumns));

    dataSink_->appendData(filteredRowVector);
  } else {
    dataSink_->appendData(inputRowVector);
  }
}

std::vector<std::string> IcebergWriter::commit() {
  checkActive("commit");
  auto finished = dataSink_->finish();
  VELOX_CHECK(finished);
  lastCommitMessages_ = dataSink_->close();
  state_ = WriterState::kCommitted;
  return lastCommitMessages_;
}

void IcebergWriter::cleanupCommittedFiles() {
  for (const auto& commitMessage : lastCommitMessages_) {
    const auto commitData = folly::parseJson(commitMessage);
    const auto& path = commitData["path"].asString();
    auto fileSystem = filesystems::getFileSystem(path, connectorSessionProperties_);
    if (fileSystem->exists(path)) {
      fileSystem->remove(path);
    }
  }
}

void IcebergWriter::abort() {
  if (state_ == WriterState::kAborted || state_ == WriterState::kCommitted) {
    return;
  }
  dataSink_->abort();
  state_ = WriterState::kAborted;
}

WriteStats IcebergWriter::writeStats() const {
  const auto currentTimeNs = getCurrentTimeNano();
  VELOX_CHECK_GE(currentTimeNs, createTimeNs_);
  const auto sinkStats = dataSink_->stats();
  return WriteStats{
      sinkStats.numWrittenBytes,
      sinkStats.numWrittenFiles,
      sinkStats.writeIOTimeUs * 1000,
      currentTimeNs - createTimeNs_};
}

std::shared_ptr<const iceberg::IcebergPartitionSpec>
parseIcebergPartitionSpec(const uint8_t* data, const int32_t length, RowTypePtr rowType) {
  gluten::IcebergPartitionSpec protoSpec;
  gluten::parseProtobuf(data, length, &protoSpec);
  std::vector<iceberg::IcebergPartitionSpec::Field> fields;
  fields.reserve(protoSpec.fields_size());

  for (const auto& protoField : protoSpec.fields()) {
    // Convert protobuf enum to C++ enum
    iceberg::TransformType transform;
    switch (protoField.transform()) {
      case gluten::IDENTITY:
        transform = iceberg::TransformType::kIdentity;
        break;
      case gluten::YEAR:
        transform = iceberg::TransformType::kYear;
        break;
      case gluten::MONTH:
        transform = iceberg::TransformType::kMonth;
        break;
      case gluten::DAY:
        transform = iceberg::TransformType::kDay;
        break;
      case gluten::HOUR:
        transform = iceberg::TransformType::kHour;
        break;
      case gluten::BUCKET:
        transform = iceberg::TransformType::kBucket;
        break;
      case gluten::TRUNCATE:
        transform = iceberg::TransformType::kTruncate;
        break;
      default:
        throw std::runtime_error("Unknown transform type");
    }

    // Handle optional parameter
    std::optional<int32_t> parameter;
    if (protoField.has_parameter()) {
      parameter = protoField.parameter();
    }

    fields.push_back({protoField.name(), rowType->findChild(protoField.name()), transform, parameter});
  }

  return std::make_shared<iceberg::IcebergPartitionSpec>(protoSpec.spec_id(), fields);
}

} // namespace gluten
