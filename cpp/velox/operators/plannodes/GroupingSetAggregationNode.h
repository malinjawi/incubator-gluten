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
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "operators/plannodes/GroupingSetLattice.h"
#include "velox/core/PlanNode.h"

namespace facebook::velox::exec {

/// Immutable description of one requested grouping set.
///
/// Bit j is set when grouping key j participates in the set. groupingId is an
/// opaque Spark literal and is deliberately not derived from activeKeysMask.
struct GroupingSetSpec {
  GroupingSetMask activeKeysMask{0};
  int64_t groupingId{0};

  bool containsKey(uint32_t key) const {
    return key < 64 && (activeKeysMask & (GroupingSetMask{1} << key)) != 0;
  }

  int32_t numActiveKeys() const;
};

/// Consumes intermediate aggregate states at the finest grouping grain and
/// emits intermediate states for all requested grouping sets.
///
/// Input:  ROW(k1..kn, acc1..accm)
/// Output: ROW(k1..kn, acc1..accm, gid)
///
/// Inactive keys are emitted as NULL. The grouping-id value is opaque
/// planner-supplied data and must not be recomputed from the key mask.
class GroupingSetAggregationNode : public core::PlanNode {
 public:
  /// Aggregate calls use base Velox function names and raw input types.
  /// childGroupedSet, when set, identifies a lattice root that may forward
  /// intermediate states without building a hash table.
  GroupingSetAggregationNode(
      core::PlanNodeId id,
      std::vector<core::FieldAccessTypedExprPtr> groupingKeys,
      std::vector<GroupingSetSpec> groupingSets,
      std::vector<std::string> aggregateNames,
      std::vector<core::AggregationNode::Aggregate> aggregates,
      std::optional<int32_t> childGroupedSet,
      core::PlanNodePtr source);

  const RowTypePtr& outputType() const override;

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override;

  const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys() const {
    return groupingKeys_;
  }

  const std::vector<GroupingSetSpec>& groupingSets() const {
    return groupingSets_;
  }

  const std::vector<std::string>& aggregateNames() const {
    return aggregateNames_;
  }

  const std::vector<core::AggregationNode::Aggregate>& aggregates() const {
    return aggregates_;
  }

  const std::optional<int32_t>& childGroupedSet() const {
    return childGroupedSet_;
  }

  const RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  column_index_t gidChannel() const {
    return static_cast<column_index_t>(outputType_->size() - 1);
  }

  /// The operator emits mergeable partial states and flushes instead of
  /// spilling.
  bool canSpill(const core::QueryConfig& /*config*/) const override {
    return false;
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  static RowTypePtr makeOutputType(
      const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys,
      const std::vector<std::string>& aggregateNames,
      const std::vector<core::AggregationNode::Aggregate>& aggregates);

  void validateSourceContract() const;

  const std::vector<core::FieldAccessTypedExprPtr> groupingKeys_;
  const std::vector<GroupingSetSpec> groupingSets_;
  const std::vector<std::string> aggregateNames_;
  const std::vector<core::AggregationNode::Aggregate> aggregates_;
  const std::optional<int32_t> childGroupedSet_;
  const std::vector<core::PlanNodePtr> sources_;
  const RowTypePtr outputType_;
};

using GroupingSetAggregationNodePtr = std::shared_ptr<const GroupingSetAggregationNode>;

} // namespace facebook::velox::exec
