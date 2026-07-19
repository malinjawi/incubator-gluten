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

#include "state/VeloxNativeStateStore.h"
#include "state/VeloxNativeStreamingAggregation.h"
#include "utils/Exception.h"
#include "velox/common/memory/Memory.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace gluten {
namespace {

using facebook::velox::BIGINT;
using facebook::velox::BaseVector;
using facebook::velox::FlatVector;
using facebook::velox::ROW;
using facebook::velox::RowVector;
using facebook::velox::RowVectorPtr;
using facebook::velox::VectorPtr;
using facebook::velox::memory::MemoryPool;

struct BenchmarkConfig {
  int32_t rowsPerBatch{8192};
  int32_t batches{8};
  int32_t iterations{50};
  int32_t keyCardinality{65536};
};

struct PreparedBatch {
  RowVectorPtr input;
  std::vector<int64_t> keyValues;
  std::vector<StateBytes> keys;
  std::vector<int64_t> deltas;
};

struct BenchmarkResult {
  std::string name;
  int64_t numInputRows{0};
  int64_t numUpdatedStateRows{0};
  int64_t numOutputRows{0};
  int64_t numTotalStateRows{0};
  int64_t stateMemoryBytes{0};
  double elapsedMs{0.0};
};

int32_t readEnvInt(const char* name, int32_t defaultValue) {
  const auto* raw = std::getenv(name);
  if (raw == nullptr || raw[0] == '\0') {
    return defaultValue;
  }
  char* end = nullptr;
  const auto parsed = std::strtol(raw, &end, 10);
  GLUTEN_CHECK(end != raw && *end == '\0', std::string("Invalid integer env var ") + name);
  GLUTEN_CHECK(parsed > 0, std::string("Env var must be positive: ") + name);
  return static_cast<int32_t>(parsed);
}

BenchmarkConfig readConfig() {
  BenchmarkConfig config;
  config.rowsPerBatch = readEnvInt(
      "GLUTEN_NATIVE_STREAMING_EXECUTION_BENCHMARK_ROWS_PER_BATCH",
      config.rowsPerBatch);
  config.batches = readEnvInt(
      "GLUTEN_NATIVE_STREAMING_EXECUTION_BENCHMARK_BATCHES",
      config.batches);
  config.iterations = readEnvInt(
      "GLUTEN_NATIVE_STREAMING_EXECUTION_BENCHMARK_ITERATIONS",
      config.iterations);
  config.keyCardinality = readEnvInt(
      "GLUTEN_NATIVE_STREAMING_EXECUTION_BENCHMARK_KEY_CARDINALITY",
      config.keyCardinality);
  return config;
}

StateBytes unsafeRowBytes(facebook::velox::row::UnsafeRowFast& unsafeRow, int32_t row) {
  const auto rowSize = unsafeRow.rowSize(row);
  StateBytes bytes(static_cast<size_t>(rowSize));
  unsafeRow.serialize(row, reinterpret_cast<char*>(bytes.data()));
  return bytes;
}

PreparedBatch makeBatch(MemoryPool* pool, const BenchmarkConfig& config, int32_t batchIndex) {
  const auto rows = config.rowsPerBatch;
  auto key = BaseVector::create<FlatVector<int64_t>>(BIGINT(), rows, pool);
  auto delta = BaseVector::create<FlatVector<int64_t>>(BIGINT(), rows, pool);

  const auto batchSeed = batchIndex * rows;
  for (auto row = 0; row < rows; ++row) {
    key->set(row, static_cast<int64_t>((batchSeed + row) % config.keyCardinality));
    delta->set(row, static_cast<int64_t>((row % 5) + 1));
  }

  std::vector<VectorPtr> children{key, delta};
  auto input = std::make_shared<RowVector>(
      pool,
      ROW({"orderkey", "revenue_delta"}, {BIGINT(), BIGINT()}),
      nullptr,
      rows,
      std::move(children));

  auto keyInput = std::make_shared<RowVector>(
      pool,
      ROW({"orderkey"}, {BIGINT()}),
      nullptr,
      rows,
      std::vector<VectorPtr>{key});
  facebook::velox::row::UnsafeRowFast unsafeRow(keyInput);

  PreparedBatch batch;
  batch.input = std::move(input);
  batch.keyValues.reserve(rows);
  batch.keys.reserve(rows);
  batch.deltas.reserve(rows);
  for (auto row = 0; row < rows; ++row) {
    batch.keyValues.push_back(key->valueAt(row));
    batch.keys.push_back(unsafeRowBytes(unsafeRow, row));
    batch.deltas.push_back(delta->valueAt(row));
  }
  return batch;
}

std::vector<PreparedBatch> makeBatches(MemoryPool* pool, const BenchmarkConfig& config) {
  std::vector<PreparedBatch> batches;
  batches.reserve(config.batches);
  for (auto batch = 0; batch < config.batches; ++batch) {
    batches.push_back(makeBatch(pool, config, batch));
  }
  return batches;
}

template <typename Fn>
BenchmarkResult runTimed(
    const std::string& name,
    const BenchmarkConfig& config,
    Fn&& fn) {
  VeloxNativeStateStore warmStore(0, nullptr, 0);
  VeloxNativeStreamingAggregation warmAggregation(warmStore);
  fn(warmAggregation);

  VeloxNativeStateStore store(0, nullptr, 0);
  VeloxNativeStreamingAggregation aggregation(store);
  fn(aggregation);

  BenchmarkResult result;
  result.name = name;
  const auto started = std::chrono::steady_clock::now();
  for (auto iteration = 0; iteration < config.iterations; ++iteration) {
    const auto metrics = fn(aggregation);
    result.numInputRows += metrics.numInputRows;
    result.numUpdatedStateRows += metrics.numUpdatedStateRows;
    result.numOutputRows += metrics.numOutputRows;
    result.numTotalStateRows = metrics.numTotalStateRows;
    result.stateMemoryBytes = metrics.stateMemoryBytes;
  }
  const auto finished = std::chrono::steady_clock::now();
  result.elapsedMs = std::chrono::duration<double, std::milli>(finished - started).count();
  return result;
}

BenchmarkResult runRowBytesLongSum(
    const BenchmarkConfig& config,
    const std::vector<PreparedBatch>& batches) {
  return runTimed(
      "native_row_bytes_long_sum_steady",
      config,
      [&](VeloxNativeStreamingAggregation& aggregation) {
        NativeStreamingCountMetrics total;
        for (const auto& batch : batches) {
          const auto metrics = aggregation.incrementLongSum(batch.keys, batch.deltas);
          total.numInputRows += metrics.numInputRows;
          total.numUpdatedStateRows += metrics.numUpdatedStateRows;
          total.numOutputRows += metrics.numOutputRows;
          total.numTotalStateRows = metrics.numTotalStateRows;
          total.stateMemoryBytes = metrics.stateMemoryBytes;
        }
        return total;
      });
}

BenchmarkResult runInt64HashLongSumUpperBound(
    const BenchmarkConfig& config,
    const std::vector<PreparedBatch>& batches) {
  std::unordered_map<int64_t, int64_t> state;
  state.reserve(static_cast<size_t>(config.keyCardinality));

  BenchmarkResult result;
  result.name = "native_int64_hash_long_sum_upper_bound";
  const auto started = std::chrono::steady_clock::now();
  for (auto iteration = 0; iteration < config.iterations; ++iteration) {
    for (const auto& batch : batches) {
      for (size_t row = 0; row < batch.keyValues.size(); ++row) {
        state[batch.keyValues[row]] += batch.deltas[row];
      }
      result.numInputRows += static_cast<int64_t>(batch.keyValues.size());
      result.numUpdatedStateRows += static_cast<int64_t>(batch.keyValues.size());
    }
  }
  const auto finished = std::chrono::steady_clock::now();
  result.numOutputRows = static_cast<int64_t>(state.size());
  result.numTotalStateRows = static_cast<int64_t>(state.size());
  result.stateMemoryBytes = result.numTotalStateRows * static_cast<int64_t>(sizeof(int64_t) * 2);
  result.elapsedMs = std::chrono::duration<double, std::milli>(finished - started).count();
  return result;
}

BenchmarkResult runColumnarLongSum(
    const BenchmarkConfig& config,
    const std::vector<PreparedBatch>& batches) {
  return runTimed(
      "native_columnar_long_sum_update_steady",
      config,
      [&](VeloxNativeStreamingAggregation& aggregation) {
        NativeStreamingCountMetrics total;
        for (const auto& batch : batches) {
          const auto result = aggregation.incrementLongSumAndReturnKeys(batch.input, {0}, 1);
          const auto& metrics = result.metrics;
          total.numInputRows += metrics.numInputRows;
          total.numUpdatedStateRows += metrics.numUpdatedStateRows;
          total.numOutputRows += metrics.numOutputRows;
          total.numTotalStateRows = metrics.numTotalStateRows;
          total.stateMemoryBytes = metrics.stateMemoryBytes;
        }
        return total;
      });
}

BenchmarkResult runTypedColumnarLongSum(
    const BenchmarkConfig& config,
    const std::vector<PreparedBatch>& batches) {
  return runTimed(
      "native_columnar_typed_int64_long_sum_update_steady",
      config,
      [&](VeloxNativeStreamingAggregation& aggregation) {
        NativeStreamingCountMetrics total;
        for (const auto& batch : batches) {
          const auto result =
              aggregation.incrementLongSumSingleInt64KeyAndReturnKeys(batch.input, 0, 1);
          const auto& metrics = result.metrics;
          total.numInputRows += metrics.numInputRows;
          total.numUpdatedStateRows += metrics.numUpdatedStateRows;
          total.numOutputRows += metrics.numOutputRows;
          total.numTotalStateRows = metrics.numTotalStateRows;
          total.stateMemoryBytes = metrics.stateMemoryBytes;
        }
        return total;
      });
}

BenchmarkResult runColumnarCount(
    const BenchmarkConfig& config,
    const std::vector<PreparedBatch>& batches) {
  return runTimed(
      "native_columnar_count_update_steady",
      config,
      [&](VeloxNativeStreamingAggregation& aggregation) {
        NativeStreamingCountMetrics total;
        for (const auto& batch : batches) {
          const auto result = aggregation.incrementCountAndReturnKeys(batch.input, {0});
          const auto& metrics = result.metrics;
          total.numInputRows += metrics.numInputRows;
          total.numUpdatedStateRows += metrics.numUpdatedStateRows;
          total.numOutputRows += metrics.numOutputRows;
          total.numTotalStateRows = metrics.numTotalStateRows;
          total.stateMemoryBytes = metrics.stateMemoryBytes;
        }
        return total;
      });
}

BenchmarkResult runTypedColumnarCount(
    const BenchmarkConfig& config,
    const std::vector<PreparedBatch>& batches) {
  return runTimed(
      "native_columnar_typed_int64_count_update_steady",
      config,
      [&](VeloxNativeStreamingAggregation& aggregation) {
        NativeStreamingCountMetrics total;
        for (const auto& batch : batches) {
          const auto result = aggregation.incrementCountSingleInt64KeyAndReturnKeys(batch.input, 0);
          const auto& metrics = result.metrics;
          total.numInputRows += metrics.numInputRows;
          total.numUpdatedStateRows += metrics.numUpdatedStateRows;
          total.numOutputRows += metrics.numOutputRows;
          total.numTotalStateRows = metrics.numTotalStateRows;
          total.stateMemoryBytes = metrics.stateMemoryBytes;
        }
        return total;
      });
}

BenchmarkResult runTypedColumnarCountStateInput(
    const BenchmarkConfig& config,
    const std::vector<PreparedBatch>& batches) {
  return runTimed(
      "native_columnar_typed_int64_count_state_input_steady",
      config,
      [&](VeloxNativeStreamingAggregation& aggregation) {
        NativeStreamingCountMetrics total;
        for (const auto& batch : batches) {
          const auto metrics = aggregation.incrementCountSingleInt64Key(batch.input, 0);
          total.numInputRows += metrics.numInputRows;
          total.numUpdatedStateRows += metrics.numUpdatedStateRows;
          total.numOutputRows += metrics.numOutputRows;
          total.numTotalStateRows = metrics.numTotalStateRows;
          total.stateMemoryBytes = metrics.stateMemoryBytes;
        }
        return total;
      });
}

void writeTsv(
    std::ostream& out,
    const BenchmarkConfig& config,
    const std::vector<BenchmarkResult>& results) {
  out << "case\trows_per_batch\tbatches\titerations\tkey_cardinality\tinput_rows"
      << "\tupdated_state_rows\toutput_rows\ttotal_state_rows\tstate_memory_bytes"
      << "\telapsed_ms\trows_per_second\tupdated_rows_per_second\toutput_rows_per_second\n";
  out << std::fixed << std::setprecision(2);
  for (const auto& result : results) {
    const auto seconds = result.elapsedMs / 1000.0;
    const auto rowsPerSecond = seconds > 0 ? result.numInputRows / seconds : 0.0;
    const auto updatedRowsPerSecond = seconds > 0 ? result.numUpdatedStateRows / seconds : 0.0;
    const auto outputRowsPerSecond = seconds > 0 ? result.numOutputRows / seconds : 0.0;
    out << result.name << '\t'
        << config.rowsPerBatch << '\t'
        << config.batches << '\t'
        << config.iterations << '\t'
        << config.keyCardinality << '\t'
        << result.numInputRows << '\t'
        << result.numUpdatedStateRows << '\t'
        << result.numOutputRows << '\t'
        << result.numTotalStateRows << '\t'
        << result.stateMemoryBytes << '\t'
        << result.elapsedMs << '\t'
        << rowsPerSecond << '\t'
        << updatedRowsPerSecond << '\t'
        << outputRowsPerSecond << '\n';
  }
}

} // namespace
} // namespace gluten

int main() {
  using namespace gluten;

  facebook::velox::memory::MemoryManager::testingSetInstance(
      facebook::velox::memory::MemoryManager::Options{});
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool(
      "VeloxNativeStreamingStateExecutionBenchmark");

  const auto config = readConfig();
  const auto batches = makeBatches(pool.get(), config);
  const std::vector<BenchmarkResult> results{
      runInt64HashLongSumUpperBound(config, batches),
      runRowBytesLongSum(config, batches),
      runTypedColumnarLongSum(config, batches),
      runColumnarLongSum(config, batches),
      runTypedColumnarCountStateInput(config, batches),
      runTypedColumnarCount(config, batches),
      runColumnarCount(config, batches),
  };

  const auto* report = std::getenv("GLUTEN_NATIVE_STREAMING_EXECUTION_BENCHMARK_REPORT");
  if (report != nullptr && report[0] != '\0') {
    std::ofstream out(report);
    GLUTEN_CHECK(out.good(), std::string("Unable to open benchmark report: ") + report);
    writeTsv(out, config, results);
  }

  writeTsv(std::cout, config, results);
  return 0;
}
