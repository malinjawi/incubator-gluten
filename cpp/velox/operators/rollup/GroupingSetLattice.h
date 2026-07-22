/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <optional>
#include <unordered_set>
#include <vector>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec {

/// Bit j set == grouping key j is ACTIVE in this grouping set. The bound of 64
/// keys is enforced by GroupingSetAggregationNode's constructor.
using GroupingSetMask = uint64_t;

/// Sentinel parent: this grouping set is computed from the operator's raw
/// input rather than from another set's output.
constexpr int32_t kRawInputParent = -1;

/// A derivation forest over the grouping-set lattice (HYBRID_DESIGN.md §2).
///
/// Grouping sets are partially ordered by mask containment. If S is a strict
/// subset of P then P's grouping refines S's, so S can be computed by
/// re-aggregating P's *output* rather than the raw input (Agarwal et al. 1996,
/// "smallest parent"). Two facts make this free rather than a tradeoff:
///
///  * MONOTONICITY. |G(P)| <= |input(P)| <= ... <= R along any derivation
///    chain, so ANY superset parent costs no more probes than raw input, for
///    any choice of parent, with no cardinality statistics at all. The plan is
///    therefore never worse than a flat fan-out in probe count.
///  * DEGENERACY. If the sets form a chain (a prefix ROLLUP) the plan is
///    exactly a cascade. If no set is a strict subset of another (e.g.
///    GROUPING SETS ((a,b),(c,d))) every set is a root and the plan is exactly
///    the flat fan-out. One mechanism produces both and everything between.
struct DerivationPlan {
  /// parent[i] is the index of the set whose output feeds set i, or
  /// kRawInputParent.
  std::vector<int32_t> parent;

  /// Inverse of parent.
  std::vector<std::vector<int32_t>> children;

  /// A topological order: every parent appears strictly before its children.
  /// This is what makes the operator's drain stack terminate structurally.
  std::vector<int32_t> order;

  /// bypass[i] == true means set i runs with no hash table: its rows are
  /// tagged with its gid and forwarded verbatim. See HYBRID_DESIGN.md §3.
  std::vector<bool> bypass;
};

namespace detail {
inline int32_t popcount(GroupingSetMask mask) {
  return static_cast<int32_t>(__builtin_popcountll(mask));
}
} // namespace detail

/// Builds the derivation forest. O(n^2) mask comparisons where n is the number
/// of GROUPING SETS -- not the number of keys. A CUBE over k keys yields
/// n = 2^k sets, so k=10 is ~10^6 integer comparisons. This runs once, in
/// Operator::initialize(), so that is acceptable.
///
/// \param masks one mask per grouping set, in node order. Must be pairwise
///        distinct: two sets at the same grain would each become a lattice root
///        redoing identical work. Checked here as well as in
///        GroupingSetAggregationNode, because this is an installed public
///        header whose callers need not go through the node.
/// \param childGroupedSet index of the set at whose grain the child operator
///        already delivers rows, if the planner proved it. That set is marked
///        for the bypass lane.
inline DerivationPlan buildDerivationPlan(
    const std::vector<GroupingSetMask>& masks,
    std::optional<int32_t> childGroupedSet = std::nullopt) {
  const auto n = static_cast<int32_t>(masks.size());
  VELOX_CHECK_GT(n, 0, "A grouping-set aggregation needs at least one set");

  DerivationPlan plan;
  plan.parent.assign(n, kRawInputParent);
  plan.children.assign(n, {});
  plan.bypass.assign(n, false);

  // (1) Rank by popcount descending. A set can only be derived from a set with
  //     STRICTLY MORE active keys, so this single order is a valid topological
  //     order for every parent assignment we could possibly make -- there is no
  //     need to topo-sort after choosing parents. Ties broken by index, via
  //     stable_sort over an iota, so the plan is deterministic across runs
  //     (plan stability matters when comparing runtime stats between runs).
  plan.order.resize(n);
  std::iota(plan.order.begin(), plan.order.end(), 0);
  std::stable_sort(
      plan.order.begin(), plan.order.end(), [&](int32_t a, int32_t b) {
        return detail::popcount(masks[a]) > detail::popcount(masks[b]);
      });

  // (2) Smallest-parent selection, scanning only sets already placed.
  for (int32_t pos = 0; pos < n; ++pos) {
    const int32_t i = plan.order[pos];
    int32_t best = kRawInputParent;
    // Cost of the raw-input option. With no statistics this is +inf:
    // monotonicity guarantees any superset parent is no worse, so preferring a
    // parent is always safe even blind.
    int32_t bestCost = std::numeric_limits<int32_t>::max();

    for (int32_t prev = 0; prev < pos; ++prev) {
      const int32_t j = plan.order[prev];
      const bool strictSubset =
          (masks[i] & ~masks[j]) == 0 && masks[i] != masks[j];
      if (!strictSubset) {
        continue;
      }
      // Proxy in the absence of statistics: fewer active keys => fewer groups
      // => cheaper to re-scan. Exact under a chain (rollup), a heuristic for
      // wide CUBEs with skewed key cardinalities -- but bounded below by
      // monotonicity, so a bad choice is never worse than no parent at all.
      const int32_t cost = detail::popcount(masks[j]);
      if (cost < bestCost) {
        bestCost = cost;
        best = j;
      }
    }
    plan.parent[i] = best;
    if (best != kRawInputParent) {
      plan.children[best].push_back(i);
    }
  }

  // (3) Duplicate masks (same grain, two gids) are a planner bug: the strict
  //     subset test leaves both as roots and we would scan the raw input twice
  //     for identical work. GroupingSetAggregationNode rejects them too, but
  //     this function is callable without the node, so enforce the precondition
  //     at its own boundary.
  {
    std::unordered_set<GroupingSetMask> seen;
    seen.reserve(n);
    for (const auto mask : masks) {
      VELOX_CHECK(
          seen.insert(mask).second,
          "Duplicate grouping-set mask {} in the derivation plan",
          mask);
    }
  }

  // (4) Bypass. The bypassed set gets no hash table: its rows are forwarded
  //     verbatim from the operator's raw input. That is only meaningful for a
  //     lattice ROOT -- a bypassed non-root would forward its parent's already
  //     aggregated rows unmerged, which is not wrong (they are still valid
  //     partial states at that grain) but silently blows up the downstream row
  //     count. The planner must not ask for it.
  if (childGroupedSet.has_value()) {
    VELOX_CHECK_LT(*childGroupedSet, n);
    VELOX_CHECK_GE(*childGroupedSet, 0);
    VELOX_CHECK_EQ(
        plan.parent[*childGroupedSet],
        kRawInputParent,
        "The bypassed grouping set must be a lattice root");
    plan.bypass[*childGroupedSet] = true;
  }

  return plan;
}

} // namespace facebook::velox::exec
