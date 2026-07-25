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
#include "operators/plannodes/GroupingSetLattice.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <numeric>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec {
namespace {

int32_t numActiveKeys(GroupingSetMask mask) {
  return static_cast<int32_t>(__builtin_popcountll(mask));
}

} // namespace

DerivationPlan buildDerivationPlan(
    const std::vector<GroupingSetMask>& masks,
    std::optional<int32_t> childGroupedSet,
    const std::vector<double>& estimatedRows) {
  VELOX_CHECK_LE(
      masks.size(), kMaxGroupingSets, "A grouping-set derivation plan supports at most {} sets", kMaxGroupingSets);
  const auto numSets = static_cast<int32_t>(masks.size());
  VELOX_CHECK_GT(numSets, 0, "A grouping-set aggregation needs at least one set");
  VELOX_CHECK(
      estimatedRows.empty() || estimatedRows.size() == masks.size(),
      "Grouping-set row estimates must be empty or have one entry per set");
  for (const auto estimate : estimatedRows) {
    VELOX_CHECK(std::isfinite(estimate) && estimate >= 0, "Grouping-set row estimates must be finite and non-negative");
  }

  auto sortedMasks = masks;
  std::sort(sortedMasks.begin(), sortedMasks.end());
  const auto duplicate = std::adjacent_find(sortedMasks.begin(), sortedMasks.end());
  if (duplicate != sortedMasks.end()) {
    VELOX_FAIL("Duplicate grouping-set mask {} in the derivation plan", *duplicate);
  }

  DerivationPlan plan;
  plan.parent.assign(numSets, kRawInputParent);
  plan.children.assign(numSets, {});
  plan.bypass.assign(numSets, false);

  plan.order.resize(numSets);
  std::iota(plan.order.begin(), plan.order.end(), 0);
  std::sort(plan.order.begin(), plan.order.end(), [&](int32_t left, int32_t right) {
    const auto leftKeys = numActiveKeys(masks[left]);
    const auto rightKeys = numActiveKeys(masks[right]);
    return leftKeys != rightKeys ? leftKeys > rightKeys : left < right;
  });

  // Parent desirability is independent of the child being queried. Rank all
  // candidates once, then encode that rank as a bit position: ctz() returns
  // the best surviving superset after the per-key bitmap intersections.
  std::vector<int32_t> candidatesByPreference(numSets);
  std::iota(candidatesByPreference.begin(), candidatesByPreference.end(), 0);
  std::sort(candidatesByPreference.begin(), candidatesByPreference.end(), [&](int32_t left, int32_t right) {
    if (!estimatedRows.empty() && estimatedRows[left] != estimatedRows[right]) {
      return estimatedRows[left] < estimatedRows[right];
    }
    const auto leftKeys = numActiveKeys(masks[left]);
    const auto rightKeys = numActiveKeys(masks[right]);
    return leftKeys != rightKeys ? leftKeys < rightKeys : left < right;
  });

  std::array<uint64_t, 64> candidatesContainingKey{};
  std::vector<int32_t> preferenceRank(numSets);
  for (int32_t rank = 0; rank < numSets; ++rank) {
    const auto candidate = candidatesByPreference[rank];
    preferenceRank[candidate] = rank;
    auto activeKeys = masks[candidate];
    while (activeKeys != 0) {
      const auto key = static_cast<int32_t>(__builtin_ctzll(activeKeys));
      candidatesContainingKey[key] |= uint64_t{1} << rank;
      activeKeys &= activeKeys - 1;
    }
  }

  const auto allCandidates =
      numSets == kMaxGroupingSets ? std::numeric_limits<uint64_t>::max() : (uint64_t{1} << numSets) - 1;
  for (const auto set : plan.order) {
    auto candidates = allCandidates & ~(uint64_t{1} << preferenceRank[set]);
    auto activeKeys = masks[set];
    while (activeKeys != 0 && candidates != 0) {
      const auto key = static_cast<int32_t>(__builtin_ctzll(activeKeys));
      candidates &= candidatesContainingKey[key];
      activeKeys &= activeKeys - 1;
    }

    const auto parent =
        candidates == 0 ? kRawInputParent : candidatesByPreference[static_cast<int32_t>(__builtin_ctzll(candidates))];
    plan.parent[set] = parent;
    if (parent != kRawInputParent) {
      plan.children[parent].push_back(set);
    }
  }

  if (childGroupedSet.has_value()) {
    VELOX_CHECK_GE(*childGroupedSet, 0);
    VELOX_CHECK_LT(*childGroupedSet, numSets);
    VELOX_CHECK_EQ(plan.parent[*childGroupedSet], kRawInputParent, "The bypassed grouping set must be a lattice root");
    plan.bypass[*childGroupedSet] = true;
  }

  return plan;
}

} // namespace facebook::velox::exec
