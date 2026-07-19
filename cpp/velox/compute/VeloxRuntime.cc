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

#include "VeloxRuntime.h"

#include <operators/plannodes/RowVectorStream.h>

#include <algorithm>
#include <condition_variable>
#include <filesystem>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <folly/ScopeGuard.h>

#include "VeloxBackend.h"
#include "compute/ResultIterator.h"
#include "compute/Runtime.h"
#include "compute/VeloxPlanConverter.h"
#include "compute/delta/DeltaConnector.h"
#include "compute/kafka/KafkaConnector.h"
#include "config/VeloxConfig.h"
#include "operators/plannodes/IteratorSplit.h"
#include "operators/serializer/VeloxRowToColumnarConverter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "utils/ConfigExtractor.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/VeloxWholeStageDumper.h"
#include "velox/common/process/StackTrace.h"

DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_bool(velox_memory_use_hugepages);
DECLARE_bool(velox_memory_pool_capacity_transfer_across_tasks);

#ifdef ENABLE_HDFS
#include "operators/writer/VeloxParquetDataSourceHDFS.h"
#endif

#ifdef ENABLE_S3
#include "operators/writer/VeloxParquetDataSourceS3.h"
#endif

#ifdef ENABLE_GCS
#include "operators/writer/VeloxParquetDataSourceGCS.h"
#endif

#ifdef ENABLE_ABFS
#include "operators/writer/VeloxParquetDataSourceABFS.h"
#endif

#ifdef GLUTEN_ENABLE_GPU
#include "operators/serializer/VeloxGpuColumnarBatchSerializer.h"
#endif

using namespace facebook;

namespace gluten {

namespace {

class HookedExecutor final : public folly::Executor {
 public:
  HookedExecutor(folly::Executor* parent, std::string name, bool debug, std::chrono::milliseconds joinTimeout)
      : parent_(parent),
        name_(std::move(name)),
        debug_(debug),
        joinTimeout_(joinTimeout),
        state_(std::make_shared<State>()) {}

  ~HookedExecutor() override {
    if (!join()) {
      LOG(WARNING) << "Timed out waiting for hooked executor " << name_ << " to finish after " << joinTimeout_.count()
                   << " ms.";
      if (debug_) {
        dumpOutstandingTasks();
      }
    }
  }

  uint8_t getNumPriorities() const override {
    return parent_ == nullptr ? 1 : parent_->getNumPriorities();
  }

  const std::string& name() const {
    return name_;
  }

  void dumpOutstandingTasks() const {
    if (!debug_) {
      return;
    }
    std::lock_guard<std::mutex> lock(state_->taskMutex);
    if (state_->inFlightTasks.empty()) {
      LOG(WARNING) << "Hooked executor " << name_ << " timed out with no tracked in-flight tasks.";
      return;
    }
    for (const auto& [taskId, info] : state_->inFlightTasks) {
      const auto elapsedMs =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - info.enqueueTime)
              .count();
      LOG(WARNING) << "Outstanding task in hooked executor " << name_ << ": taskId=" << taskId
                   << ", elapsedMs=" << elapsedMs << ", priority=" << static_cast<int32_t>(info.priority)
                   << ", submitStacktrace:\n"
                   << info.submitStacktrace;
    }
  }

 private:
  struct TaskInfo {
    std::chrono::steady_clock::time_point enqueueTime;
    int8_t priority;
    std::string submitStacktrace;
  };

  struct State {
    std::atomic<uint64_t> nextTaskId{0};
    std::atomic<size_t> inFlight{0};
    std::mutex mutex;
    std::condition_variable cv;
    std::mutex taskMutex;
    std::unordered_map<uint64_t, TaskInfo> inFlightTasks;
  };

  bool join() {
    std::unique_lock<std::mutex> lock(state_->mutex);
    return state_->cv.wait_for(
        lock, joinTimeout_, [&] { return state_->inFlight.load(std::memory_order_acquire) == 0; });
  }

 public:
  void add(folly::Func func) override {
    GLUTEN_CHECK(parent_ != nullptr, "Parent executor is null.");
    state_->inFlight.fetch_add(1, std::memory_order_relaxed);
    parent_->add(wrap(std::move(func), 0));
  }

  void addWithPriority(folly::Func func, int8_t priority) override {
    GLUTEN_CHECK(parent_ != nullptr, "Parent executor is null.");
    state_->inFlight.fetch_add(1, std::memory_order_relaxed);
    parent_->addWithPriority(wrap(std::move(func), priority), priority);
  }

  folly::Func wrap(folly::Func func, int8_t priority) {
    auto state = state_;
    const auto taskId = state->nextTaskId.fetch_add(1, std::memory_order_relaxed);
    if (debug_) {
      TaskInfo info{
          .enqueueTime = std::chrono::steady_clock::now(),
          .priority = priority,
          .submitStacktrace = velox::process::StackTrace().toString()};
      std::lock_guard<std::mutex> lock(state->taskMutex);
      state->inFlightTasks[taskId] = std::move(info);
    }
    const auto debug = debug_;
    return [func = std::move(func), state, debug, taskId]() mutable {
      auto markDone = folly::makeGuard([&] {
        if (debug) {
          std::lock_guard<std::mutex> lock(state->taskMutex);
          state->inFlightTasks.erase(taskId);
        }
        if (state->inFlight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
          std::lock_guard<std::mutex> lock(state->mutex);
          state->cv.notify_all();
        }
      });
      // Destroy the submitted callable and all of its captures before
      // decrementing inFlight_. Some async tasks capture AsyncLoadHolder,
      // which keeps a MemoryPool alive until the callable itself is
      // destroyed. If we decrement inFlight_ first, HookedExecutor can
      // appear drained and let VeloxRuntime teardown proceed while the
      // holder is still alive, causing MemoryManager destruction to race
      // with outstanding task-owned resources.
      auto localFunc = std::move(func);
      localFunc();
    };
  }

  folly::Executor* parent_;
  std::string name_;
  bool debug_;
  std::chrono::milliseconds joinTimeout_;
  std::shared_ptr<State> state_;
};

std::unique_ptr<folly::Executor> makeHookedExecutor(
    folly::Executor* parent,
    const std::string& name,
    bool debug,
    std::chrono::milliseconds joinTimeout) {
  if (parent == nullptr) {
    return nullptr;
  }
  return std::make_unique<HookedExecutor>(parent, name, debug, joinTimeout);
}

VeloxConnectorIds makeProcessConnectorIds() {
  return VeloxConnectorIds{
      .hive = kHiveConnectorId,
      .delta = delta::DeltaConnectorFactory::kDeltaConnectorName,
      .iterator = kIteratorConnectorId,
      .kafka = kafka::KafkaConnector::kKafkaConnectorName,
      .cudfHive = kCudfHiveConnectorId};
}

std::mutex& connectorRegistryMutex() {
  static std::mutex mutex;
  return mutex;
}

bool hasEnabledStreamKafkaRead(const google::protobuf::Message& message) {
  const auto* descriptor = message.GetDescriptor();
  const auto* reflection = message.GetReflection();
  for (int i = 0; i < descriptor->field_count(); ++i) {
    const auto* field = descriptor->field(i);
    if (field->name() == "stream_kafka" && field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_BOOL &&
        reflection->HasField(message, field) && reflection->GetBool(message, field)) {
      return true;
    }
    if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
      continue;
    }
    if (field->is_repeated()) {
      const auto size = reflection->FieldSize(message, field);
      for (int j = 0; j < size; ++j) {
        if (hasEnabledStreamKafkaRead(reflection->GetRepeatedMessage(message, field, j))) {
          return true;
        }
      }
    } else if (reflection->HasField(message, field) &&
               hasEnabledStreamKafkaRead(reflection->GetMessage(message, field))) {
      return true;
    }
  }
  return false;
}

enum class SerializedSplitKind {
  kLocalFiles = 0,
  kStreamKafka = 1,
};

struct DecodedSplitPayload {
  std::optional<SerializedSplitKind> kind;
  const uint8_t* data;
  int32_t size;
};

DecodedSplitPayload decodeSplitPayload(const uint8_t* data, int32_t size) {
  constexpr int32_t kEnvelopeSize = 6;
  if (size < kEnvelopeSize || data[0] != 'G' || data[1] != 'L' || data[2] != 'S' || data[3] != 'P') {
    return DecodedSplitPayload{.kind = std::nullopt, .data = data, .size = size};
  }

  GLUTEN_CHECK(
      data[4] == 1,
      "Unsupported Gluten split envelope version: " + std::to_string(static_cast<int32_t>(data[4])));
  switch (data[5]) {
    case static_cast<uint8_t>(SerializedSplitKind::kLocalFiles):
      return DecodedSplitPayload{
          .kind = SerializedSplitKind::kLocalFiles, .data = data + kEnvelopeSize, .size = size - kEnvelopeSize};
    case static_cast<uint8_t>(SerializedSplitKind::kStreamKafka):
      return DecodedSplitPayload{
          .kind = SerializedSplitKind::kStreamKafka, .data = data + kEnvelopeSize, .size = size - kEnvelopeSize};
    default:
      GLUTEN_CHECK(
          false, "Unsupported Gluten split envelope kind: " + std::to_string(static_cast<int32_t>(data[5])));
  }
}

const char* splitTypeName(SerializedSplitKind kind) {
  switch (kind) {
    case SerializedSplitKind::kLocalFiles:
      return "ReadRel.LocalFiles";
    case SerializedSplitKind::kStreamKafka:
      return "ReadRel.StreamKafka";
  }
}

bool isSensitiveKafkaParamForDebug(const std::string& key) {
  return key.find("password") != std::string::npos || key.find("secret") != std::string::npos ||
      key.find("sasl.jaas.config") != std::string::npos || key.find("ssl.key") != std::string::npos;
}

std::string splitPayloadToJsonForDebug(
    SerializedSplitKind splitKind,
    const char* splitType,
    const uint8_t* data,
    int32_t size) {
  if (splitKind != SerializedSplitKind::kStreamKafka) {
    return substraitFromPbToJson(splitType, data, size);
  }

  ::substrait::ReadRel_StreamKafka redacted;
  GLUTEN_CHECK(parseProtobuf(data, size, &redacted) == true, "Parse substrait Kafka split failed");
  std::vector<std::string> sensitiveKeys;
  for (const auto& param : redacted.params()) {
    const auto& key = param.first;
    if (isSensitiveKafkaParamForDebug(key)) {
      sensitiveKeys.emplace_back(key);
    }
  }
  for (const auto& key : sensitiveKeys) {
    (*redacted.mutable_params())[key] = "<redacted>";
  }

  const auto redactedBytes = redacted.SerializeAsString();
  return substraitFromPbToJson(
      splitType,
      reinterpret_cast<const uint8_t*>(redactedBytes.data()),
      static_cast<int32_t>(redactedBytes.size()));
}

} // namespace

VeloxRuntime::VeloxRuntime(
    const std::string& kind,
    VeloxMemoryManager* vmm,
    const std::unordered_map<std::string, std::string>& confMap)
    : Runtime(kind, vmm, confMap) {
  // Refresh session config.
  veloxCfg_ =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(confMap_));
  debugModeEnabled_ = veloxCfg_->get<bool>(kDebugModeEnabled, false);
  FLAGS_minloglevel = veloxCfg_->get<uint32_t>(kGlogSeverityLevel, FLAGS_minloglevel);
  FLAGS_v = veloxCfg_->get<uint32_t>(kGlogVerboseLevel, FLAGS_v);
  FLAGS_velox_exception_user_stacktrace_enabled =
      veloxCfg_->get<bool>(kEnableUserExceptionStacktrace, FLAGS_velox_exception_user_stacktrace_enabled);
  FLAGS_velox_exception_system_stacktrace_enabled =
      veloxCfg_->get<bool>(kEnableSystemExceptionStacktrace, FLAGS_velox_exception_system_stacktrace_enabled);
  FLAGS_velox_memory_use_hugepages = veloxCfg_->get<bool>(kMemoryUseHugePages, FLAGS_velox_memory_use_hugepages);
  FLAGS_velox_memory_pool_capacity_transfer_across_tasks = veloxCfg_->get<bool>(
      kMemoryPoolCapacityTransferAcrossTasks, FLAGS_velox_memory_pool_capacity_transfer_across_tasks);

  connectorIds_ = makeProcessConnectorIds();

  initializeExecutors();
  registerConnectors();
}

VeloxRuntime::~VeloxRuntime() {
  unregisterConnectors();
  executor_.reset();
  spillExecutor_.reset();
  ioExecutor_.reset();
}

void VeloxRuntime::initializeExecutors() {
  const auto timeoutMs =
      veloxCfg_->get<int32_t>(kVeloxAsyncTimeoutOnTaskStopping, kVeloxAsyncTimeoutOnTaskStoppingDefault);
  const auto timeout = std::chrono::milliseconds(timeoutMs);
  executor_ = makeHookedExecutor(VeloxBackend::get()->executor(), kind_ + ".executor", debugModeEnabled_, timeout);
  spillExecutor_ =
      makeHookedExecutor(VeloxBackend::get()->spillExecutor(), kind_ + ".spill", debugModeEnabled_, timeout);
  ioExecutor_ = makeHookedExecutor(VeloxBackend::get()->ioExecutor(), kind_ + ".io", debugModeEnabled_, timeout);
}

void VeloxRuntime::registerConnectors() {
  const std::lock_guard<std::mutex> lock(connectorRegistryMutex());
  auto* backend = VeloxBackend::get();
  if (!velox::connector::hasConnector(connectorIds_.hive)) {
    connectorIds_.hiveRegistered =
        velox::connector::registerConnector(backend->createHiveConnector(connectorIds_.hive, backend->ioExecutor()));
    GLUTEN_CHECK(connectorIds_.hiveRegistered, "Failed to register hive connector: " + connectorIds_.hive);
  }
  GLUTEN_CHECK(
      velox::connector::hasConnector(connectorIds_.hive),
      "Hive connector not found after registration: " + connectorIds_.hive);

  if (!velox::connector::hasConnector(connectorIds_.delta)) {
    connectorIds_.deltaRegistered =
        velox::connector::registerConnector(backend->createDeltaConnector(connectorIds_.delta, backend->ioExecutor()));
    GLUTEN_CHECK(connectorIds_.deltaRegistered, "Failed to register delta connector: " + connectorIds_.delta);
  }
  GLUTEN_CHECK(
      velox::connector::hasConnector(connectorIds_.delta),
      "Delta connector not found after registration: " + connectorIds_.delta);

  const auto valueStreamDynamicFilterEnabled =
      veloxCfg_->get<bool>(kValueStreamDynamicFilterEnabled, kValueStreamDynamicFilterEnabledDefault);
  if (!velox::connector::hasConnector(connectorIds_.iterator)) {
    connectorIds_.iteratorRegistered = velox::connector::registerConnector(
        backend->createValueStreamConnector(connectorIds_.iterator, valueStreamDynamicFilterEnabled));
    GLUTEN_CHECK(
        connectorIds_.iteratorRegistered, "Failed to register iterator connector: " + connectorIds_.iterator);
  }
  GLUTEN_CHECK(
      velox::connector::hasConnector(connectorIds_.iterator),
      "Iterator connector not found after registration: " + connectorIds_.iterator);

  if (!velox::connector::hasConnector(connectorIds_.kafka)) {
    connectorIds_.kafkaRegistered =
        velox::connector::registerConnector(backend->createKafkaConnector(connectorIds_.kafka));
    GLUTEN_CHECK(connectorIds_.kafkaRegistered, "Failed to register Kafka connector: " + connectorIds_.kafka);
  }
  GLUTEN_CHECK(
      velox::connector::hasConnector(connectorIds_.kafka),
      "Kafka connector not found after registration: " + connectorIds_.kafka);

#ifdef GLUTEN_ENABLE_GPU
  if (veloxCfg_->get<bool>(kCudfEnableTableScan, kCudfEnableTableScanDefault) &&
      veloxCfg_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    if (!velox::connector::hasConnector(connectorIds_.cudfHive)) {
      connectorIds_.cudfHiveRegistered = velox::connector::registerConnector(
          backend->createCudfHiveConnector(connectorIds_.cudfHive, backend->ioExecutor()));
      GLUTEN_CHECK(
          connectorIds_.cudfHiveRegistered, "Failed to register cudf hive connector: " + connectorIds_.cudfHive);
    }
    GLUTEN_CHECK(
        velox::connector::hasConnector(connectorIds_.cudfHive),
        "Cudf hive connector not found after registration: " + connectorIds_.cudfHive);
  }
#endif
}

void VeloxRuntime::unregisterConnectors() {
  // Velox's connector registry is process-global, while Gluten creates short-lived runtimes for
  // query fragments and streaming micro-batches. Keep the backend connectors registered for the
  // process lifetime to avoid unregistering a connector while another runtime or task is using it.
}

void VeloxRuntime::parsePlan(const uint8_t* data, int32_t size) {
  if (debugModeEnabled_ || dumper_ != nullptr) {
    try {
      auto planJson = substraitFromPbToJson("Plan", data, size);
      if (dumper_ != nullptr) {
        dumper_->dumpPlan(planJson);
      }

      LOG_IF(INFO, debugModeEnabled_ && taskInfo_.has_value())
          << std::string(50, '#') << " received substrait::Plan: " << taskInfo_.value() << std::endl
          << planJson;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting substrait::Plan to JSON: " << e.what();
    }
  }

  GLUTEN_CHECK(parseProtobuf(data, size, &substraitPlan_) == true, "Parse substrait plan failed");
}

void VeloxRuntime::parseSplitInfo(const uint8_t* data, int32_t size, int32_t splitIndex) {
  const auto decoded = decodeSplitPayload(data, size);
  const auto splitKind = decoded.kind.value_or(
      hasEnabledStreamKafkaRead(substraitPlan_) ? SerializedSplitKind::kStreamKafka : SerializedSplitKind::kLocalFiles);
  const auto splitType = splitTypeName(splitKind);
  if (debugModeEnabled_ || dumper_ != nullptr) {
    try {
      auto splitJson = splitPayloadToJsonForDebug(splitKind, splitType, decoded.data, decoded.size);
      if (dumper_ != nullptr) {
        dumper_->dumpInputSplit(splitIndex, splitJson);
      }
      LOG_IF(INFO, debugModeEnabled_ && taskInfo_.has_value())
          << std::string(50, '#') << " received substrait::" << splitType << ": " << taskInfo_.value()
          << std::endl
          << splitJson;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting substrait::" << splitType << " to JSON: " << e.what();
    }
  }
  if (splitKind == SerializedSplitKind::kStreamKafka) {
    ::substrait::ReadRel_StreamKafka streamKafka;
    GLUTEN_CHECK(
        parseProtobuf(decoded.data, decoded.size, &streamKafka) == true, "Parse substrait Kafka split failed");
    splitPayloads_.push_back(SubstraitSplit::makeStreamKafka(std::move(streamKafka)));
  } else {
    ::substrait::ReadRel_LocalFiles localFile;
    GLUTEN_CHECK(
        parseProtobuf(decoded.data, decoded.size, &localFile) == true, "Parse substrait local-files split failed");
    splitPayloads_.push_back(SubstraitSplit::makeLocalFiles(std::move(localFile)));
  }
}

void VeloxRuntime::getInfoAndIds(
    const std::unordered_map<velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
    const std::unordered_set<velox::core::PlanNodeId>& leafPlanNodeIds,
    std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    std::vector<velox::core::PlanNodeId>& scanIds,
    std::vector<velox::core::PlanNodeId>& streamIds) {
  int32_t streamIdx = 0;
  for (const auto& leafPlanNodeId : leafPlanNodeIds) {
    auto it = splitInfoMap.find(leafPlanNodeId);
    if (it == splitInfoMap.end()) {
      throw std::runtime_error("Could not find leafPlanNodeId.");
    }
    auto splitInfo = it->second;
    // Based on the current code, indexing of streams and files follow different orders:
    // 1. Streams follow "iterator:<idx>" in the substrait plan;
    // 2. Files follow the traversal order in the plan node tree.
    // FIXME: Why we didn't have a unified design?
    switch (splitInfo->leafType) {
      case SplitInfo::LeafType::SPLIT_AWARE_STREAM:
        streamIds.emplace_back(ValueStreamConnectorFactory::nodeIdOf(streamIdx++));
        break;
      case SplitInfo::LeafType::TABLE_SCAN:
        scanInfos.emplace_back(splitInfo);
        scanIds.emplace_back(leafPlanNodeId);
        break;
      case SplitInfo::LeafType::TRIVIAL_LEAF:
        break;
    }
  }
}

std::string VeloxRuntime::planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) {
  auto veloxMemoryPool = gluten::defaultLeafVeloxMemoryPool();
  VeloxPlanConverter veloxPlanConverter(
      veloxMemoryPool.get(), veloxCfg_.get(), {}, connectorIds_, std::nullopt, std::nullopt, true);
  auto veloxPlan = veloxPlanConverter.toVeloxPlan(substraitPlan_, splitPayloads_);
  return veloxPlan->toString(details, true);
}

VeloxMemoryManager* VeloxRuntime::memoryManager() {
  auto vmm = dynamic_cast<VeloxMemoryManager*>(memoryManager_);
  GLUTEN_CHECK(vmm != nullptr, "Not a Velox memory manager");
  return vmm;
}

std::shared_ptr<ResultIterator> VeloxRuntime::createResultIterator(
    const std::string& spillDir,
    const std::vector<std::shared_ptr<ResultIterator>>& inputs) {
  LOG_IF(INFO, debugModeEnabled_) << "VeloxRuntime session config:" << printConfig(confMap_);

  VeloxPlanConverter veloxPlanConverter(
      memoryManager()->getLeafMemoryPool().get(),
      veloxCfg_.get(),
      inputs,
      connectorIds_,
      *localWriteFilesTempPath(),
      *localWriteFileName());
  veloxPlan_ = veloxPlanConverter.toVeloxPlan(substraitPlan_, std::move(splitPayloads_));
  LOG_IF(INFO, debugModeEnabled_ && taskInfo_.has_value())
      << "############### Velox plan for task " << taskInfo_.value() << " ###############" << std::endl
      << veloxPlan_->toString(true, true);

  // Scan node can be required.
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(veloxPlanConverter.splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto wholeStageIter = std::make_unique<WholeStageResultIterator>(
      memoryManager(),
      veloxPlan_,
      scanIds,
      scanInfos,
      streamIds,
      executor_.get(),
      spillExecutor_.get(),
      connectorIds_,
      spillDir,
      veloxCfg_,
      taskInfo_.has_value() ? taskInfo_.value() : SparkTaskInfo{});

  auto remainingInputIterators = veloxPlanConverter.remainingInputIterators();
  if (!remainingInputIterators.empty()) {
    // Converts remaining input iterators to splits and add them to the task.
    wholeStageIter->addIteratorSplits(remainingInputIterators);
  }

  return std::make_shared<ResultIterator>(std::move(wholeStageIter), this);
}

void VeloxRuntime::noMoreSplits(ResultIterator* iter) {
  auto* splitAwareIter = dynamic_cast<gluten::SplitAwareColumnarBatchIterator*>(iter->getInputIter());
  if (splitAwareIter == nullptr) {
    throw GlutenException("Iterator does not support split management");
  }
  splitAwareIter->noMoreSplits();
}

void VeloxRuntime::requestBarrier(ResultIterator* iter) {
  auto* splitAwareIter = dynamic_cast<gluten::SplitAwareColumnarBatchIterator*>(iter->getInputIter());
  if (splitAwareIter == nullptr) {
    throw GlutenException("Iterator does not support split management");
  }
  splitAwareIter->requestBarrier();
}

std::shared_ptr<ColumnarToRowConverter> VeloxRuntime::createColumnar2RowConverter(int64_t column2RowMemThreshold) {
  auto veloxPool = memoryManager()->getLeafMemoryPool();
  return std::make_shared<VeloxColumnarToRowConverter>(veloxPool, column2RowMemThreshold);
}

std::shared_ptr<ColumnarBatch> VeloxRuntime::createOrGetEmptySchemaBatch(int32_t numRows) {
  auto& lookup = emptySchemaBatchLoopUp_;
  if (lookup.find(numRows) == lookup.end()) {
    auto veloxPool = memoryManager()->getLeafMemoryPool();
    const std::shared_ptr<VeloxColumnarBatch>& batch =
        VeloxColumnarBatch::from(veloxPool.get(), gluten::createZeroColumnBatch(numRows));
    lookup.emplace(numRows, batch); // the batch will be released after Spark task ends
  }
  return lookup.at(numRows);
}

std::shared_ptr<ColumnarBatch> VeloxRuntime::select(
    std::shared_ptr<ColumnarBatch> batch,
    const std::vector<int32_t>& columnIndices) {
  auto veloxPool = memoryManager()->getLeafMemoryPool();
  auto veloxBatch = gluten::VeloxColumnarBatch::from(veloxPool.get(), batch);
  auto outputBatch = veloxBatch->select(veloxPool.get(), std::move(columnIndices));
  return outputBatch;
}

std::shared_ptr<RowToColumnarConverter> VeloxRuntime::createRow2ColumnarConverter(struct ArrowSchema* cSchema) {
  auto veloxPool = memoryManager()->getLeafMemoryPool();
  return std::make_shared<VeloxRowToColumnarConverter>(cSchema, veloxPool);
}

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
std::shared_ptr<IcebergWriter> VeloxRuntime::createIcebergWriter(
    RowTypePtr rowType,
    int32_t format,
    const std::string& outputDirectory,
    facebook::velox::common::CompressionKind compressionKind,
    int32_t partitionId,
    int64_t taskId,
    const std::string& operationId,
    std::shared_ptr<const facebook::velox::connector::hive::iceberg::IcebergPartitionSpec> spec,
    const gluten::IcebergNestedField& protoField,
    const std::unordered_map<std::string, std::string>& sparkConfs) {
  auto veloxPool = memoryManager()->getLeafMemoryPool();
  auto connectorPool = memoryManager()->getAggregateMemoryPool();
  return std::make_shared<IcebergWriter>(
      rowType,
      format,
      outputDirectory,
      compressionKind,
      partitionId,
      taskId,
      operationId,
      spec,
      protoField,
      sparkConfs,
      veloxPool,
      connectorPool);
}
#endif

std::shared_ptr<ShuffleWriter> VeloxRuntime::createShuffleWriter(
    int32_t numPartitions,
    const std::shared_ptr<PartitionWriter>& partitionWriter,
    const std::shared_ptr<ShuffleWriterOptions>& options) {
  GLUTEN_ASSIGN_OR_THROW(
      std::shared_ptr<ShuffleWriter> shuffleWriter,
      VeloxShuffleWriter::create(options->shuffleWriterType, numPartitions, partitionWriter, options, memoryManager()));
  return shuffleWriter;
}

std::shared_ptr<VeloxDataSource> VeloxRuntime::createDataSource(
    const std::string& filePath,
    std::shared_ptr<arrow::Schema> schema) {
  static std::atomic_uint32_t id{0UL};
  auto veloxPool = memoryManager()->getAggregateMemoryPool()->addAggregateChild("datasource." + std::to_string(id++));
  // Pass a dedicate pool for S3 and GCS sinks as can't share veloxPool
  // with parquet writer.
  // FIXME: Check file formats?
  auto sinkPool = memoryManager()->getLeafMemoryPool();
  if (isSupportedHDFSPath(filePath)) {
#ifdef ENABLE_HDFS
    return std::make_shared<VeloxParquetDataSourceHDFS>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is hdfs path but the HDFS haven't been enabled when writing parquet data in velox runtime!");
#endif
  } else if (isSupportedS3SdkPath(filePath)) {
#ifdef ENABLE_S3
    return std::make_shared<VeloxParquetDataSourceS3>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is S3 path but the S3 haven't been enabled when writing parquet data in velox runtime!");
#endif
  } else if (isSupportedGCSPath(filePath)) {
#ifdef ENABLE_GCS
    return std::make_shared<VeloxParquetDataSourceGCS>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is GCS path but the GCS haven't been enabled when writing parquet data in velox runtime!");
#endif
  } else if (isSupportedABFSPath(filePath)) {
#ifdef ENABLE_ABFS
    return std::make_shared<VeloxParquetDataSourceABFS>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is ABFS path but the ABFS haven't been enabled when writing parquet data in velox runtime!");
#endif
  }
  return std::make_shared<VeloxParquetDataSource>(filePath, veloxPool, sinkPool, schema);
}

std::shared_ptr<ShuffleReader> VeloxRuntime::createShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    ShuffleReaderOptions options) {
  auto codec = gluten::createCompressionCodec(options.compressionType, options.codecBackend);
  const auto veloxCompressionKind = arrowCompressionTypeToVelox(options.compressionType);
  const auto rowType = facebook::velox::asRowType(gluten::fromArrowSchema(schema));

  auto deserializerFactory = std::make_unique<gluten::VeloxShuffleReaderDeserializerFactory>(
      schema,
      std::move(codec),
      veloxCompressionKind,
      rowType,
      options.batchSize,
      options.readerBufferSize,
      options.deserializerBufferSize,
      memoryManager(),
      options.shuffleWriterType,
      options.enableHashShuffleReaderStreamMerge);

  return std::make_shared<VeloxShuffleReader>(std::move(deserializerFactory));
}

std::unique_ptr<ColumnarBatchSerializer> VeloxRuntime::createColumnarBatchSerializer(struct ArrowSchema* cSchema) {
  auto arrowPool = memoryManager()->defaultArrowMemoryPool();
  auto veloxPool = memoryManager()->getLeafMemoryPool();
#ifdef GLUTEN_ENABLE_GPU
  if (veloxCfg_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    return std::make_unique<VeloxGpuColumnarBatchSerializer>(arrowPool, veloxPool, cSchema);
  }
#endif
  return std::make_unique<VeloxColumnarBatchSerializer>(arrowPool, veloxPool, cSchema);
}

void VeloxRuntime::enableDumping() {
  auto saveDir = veloxCfg_->get<std::string>(kGlutenSaveDir);
  GLUTEN_CHECK(saveDir.has_value(), kGlutenSaveDir + " is not set");

  auto taskInfo = getSparkTaskInfo();
  GLUTEN_CHECK(taskInfo.has_value(), "Task info is not set. Please set task info before enabling dumping.");

  dumper_ = std::make_shared<VeloxWholeStageDumper>(
      taskInfo.value(),
      saveDir.value(),
      veloxCfg_->get<int64_t>(kSparkBatchSize, 4096),
      memoryManager()->getAggregateMemoryPool().get());

  dumper_->dumpConf(getConfMap());
}
} // namespace gluten
