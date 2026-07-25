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
#include <optional>
#include <vector>

namespace facebook::velox::exec {

/// Bit j is set when grouping key j participates in the grouping set.
using GroupingSetMask = uint64_t;

/// A derivation query stores candidate grouping sets in one machine word.
inline constexpr int32_t kMaxGroupingSets = 64;

/// The set reads the operator input rather than another grouping set's output.
constexpr int32_t kRawInputParent = -1;

/// Derivation forest ordered from finer grouping sets to coarser sets.
///
/// A prefix ROLLUP becomes a chain. Unrelated grouping sets become independent
/// roots and therefore degrade to a flat fan-out.
struct DerivationPlan {
  /// Parent set index, or kRawInputParent.
  std::vector<int32_t> parent;

  /// Inverse adjacency list for parent.
  std::vector<std::vector<int32_t>> children;

  /// Topological order in which every parent precedes its children.
  std::vector<int32_t> order;

  /// Sets that forward their intermediate states without a hash table.
  std::vector<bool> bypass;
};

/// Builds a deterministic derivation forest from pairwise-distinct masks.
/// childGroupedSet, when present, must identify a root. estimatedRows, when
/// non-empty, supplies a non-negative output-row estimate per mask and makes
/// parent selection prefer the candidate expected to emit fewer rows.
///
/// Construction is O(S * K + S log S): candidates are indexed in one 64-bit
/// bitmap per key, and each parent query intersects the bitmaps for that set's
/// active keys.
DerivationPlan buildDerivationPlan(
    const std::vector<GroupingSetMask>& masks,
    std::optional<int32_t> childGroupedSet = std::nullopt,
    const std::vector<double>& estimatedRows = {});

} // namespace facebook::velox::exec
