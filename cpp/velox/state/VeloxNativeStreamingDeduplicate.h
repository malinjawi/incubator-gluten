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

#include "state/VeloxNativeStateStore.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace gluten {

struct NativeStreamingDeduplicateMetrics {
  int64_t numInputRows{0};
  int64_t numOutputRows{0};
  int64_t numUpdatedStateRows{0};
  int64_t numDroppedDuplicateRows{0};
  int64_t numRowsDroppedByWatermark{0};
  int64_t numRemovedStateRows{0};
  int64_t numTotalStateRows{0};
  int64_t stateMemoryBytes{0};
};

struct NativeStreamingDeduplicateResult {
  std::vector<size_t> outputRowIndices;
  NativeStreamingDeduplicateMetrics metrics;
};

struct NativeStreamingDeduplicateWatermarkInput {
  StateBytes key;
  int64_t eventTimeMicros;
};

class VeloxNativeStreamingDeduplicate {
 public:
  explicit VeloxNativeStreamingDeduplicate(VeloxNativeStateStore& store);

  NativeStreamingDeduplicateResult deduplicate(const std::vector<StateBytes>& keys);

  NativeStreamingDeduplicateResult deduplicateWithinWatermark(
      const std::vector<NativeStreamingDeduplicateWatermarkInput>& rows,
      std::optional<int64_t> eventTimeWatermarkForLateEventsMicros,
      int64_t delayThresholdMicros,
      std::optional<int64_t> eventTimeWatermarkForEvictionMicros);

 private:
  void refreshStateMetrics(NativeStreamingDeduplicateMetrics& metrics) const;

  VeloxNativeStateStore& store_;
};

} // namespace gluten
