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
#include "operators/plannodes/GroupingSetAggregationNode.h"

#include <algorithm>
#include <limits>
#include <sstream>
#include <unordered_set>

#include "velox/exec/AggregateFunctionRegistry.h"

namespace facebook::velox::exec {

int32_t GroupingSetSpec::numActiveKeys() const {
  return static_cast<int32_t>(__builtin_popcountll(activeKeysMask));
}

GroupingSetAggregationNode::GroupingSetAggregationNode(
    core::PlanNodeId id,
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys,
    std::vector<GroupingSetSpec> groupingSets,
    std::vector<std::string> aggregateNames,
    std::vector<core::AggregationNode::Aggregate> aggregates,
    std::optional<int32_t> childGroupedSet,
    core::PlanNodePtr source)
    : PlanNode(std::move(id)),
      groupingKeys_{std::move(groupingKeys)},
      groupingSets_{std::move(groupingSets)},
      aggregateNames_{std::move(aggregateNames)},
      aggregates_{std::move(aggregates)},
      childGroupedSet_{childGroupedSet},
      sources_{std::move(source)},
      outputType_{makeOutputType(groupingKeys_, aggregateNames_, aggregates_)} {
  validateSourceContract();
  VELOX_CHECK_LE(
      groupingSets_.size(),
      kMaxGroupingSets,
      "A grouping-set aggregation supports at most {} grouping sets",
      kMaxGroupingSets);

  const auto validMask = groupingKeys_.size() == 64 ? std::numeric_limits<GroupingSetMask>::max()
                                                    : (GroupingSetMask{1} << groupingKeys_.size()) - 1;
  std::unordered_set<GroupingSetMask> seenMasks;
  for (const auto& set : groupingSets_) {
    VELOX_CHECK(
        (set.activeKeysMask & ~validMask) == 0,
        "Grouping-set mask {} contains keys outside the {} grouping keys",
        set.activeKeysMask,
        groupingKeys_.size());
    VELOX_CHECK(
        seenMasks.insert(set.activeKeysMask).second,
        "Duplicate grouping-set mask; duplicate grouping sets must use the "
        "fallback plan");
  }

  if (childGroupedSet_.has_value()) {
    VELOX_CHECK_GE(*childGroupedSet_, 0);
    VELOX_CHECK_LT(*childGroupedSet_, static_cast<int32_t>(groupingSets_.size()));
  }
}

const RowTypePtr& GroupingSetAggregationNode::outputType() const {
  return outputType_;
}

const std::vector<core::PlanNodePtr>& GroupingSetAggregationNode::sources() const {
  return sources_;
}

std::string_view GroupingSetAggregationNode::name() const {
  return "GroupingSetAggregation";
}

folly::dynamic GroupingSetAggregationNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["groupingKeys"] = ISerializable::serialize(groupingKeys_);
  obj["aggregateNames"] = ISerializable::serialize(aggregateNames_);
  obj["aggregates"] = folly::dynamic::array;
  for (const auto& aggregate : aggregates_) {
    obj["aggregates"].push_back(aggregate.serialize());
  }

  obj["groupingSets"] = folly::dynamic::array;
  for (const auto& set : groupingSets_) {
    folly::dynamic serializedSet = folly::dynamic::object();
    serializedSet["gid"] = set.groupingId;
    serializedSet["keyIsActive"] = folly::dynamic::array;
    for (size_t key = 0; key < groupingKeys_.size(); ++key) {
      serializedSet["keyIsActive"].push_back(set.containsKey(key));
    }
    obj["groupingSets"].push_back(std::move(serializedSet));
  }

  if (childGroupedSet_.has_value()) {
    obj["childGroupedSet"] = *childGroupedSet_;
  }
  return obj;
}

core::PlanNodePtr GroupingSetAggregationNode::create(const folly::dynamic& obj, void* context) {
  auto sources = ISerializable::deserialize<std::vector<core::PlanNode>>(obj["sources"], context);
  VELOX_CHECK_EQ(sources.size(), 1, "GroupingSetAggregation must have exactly one source");

  auto groupingKeys = ISerializable::deserialize<std::vector<core::FieldAccessTypedExpr>>(obj["groupingKeys"], context);
  auto aggregateNames = ISerializable::deserialize<std::vector<std::string>>(obj["aggregateNames"]);

  std::vector<core::AggregationNode::Aggregate> aggregates;
  aggregates.reserve(obj["aggregates"].size());
  for (const auto& aggregate : obj["aggregates"]) {
    aggregates.push_back(core::AggregationNode::Aggregate::deserialize(aggregate, context));
  }

  std::vector<GroupingSetSpec> groupingSets;
  VELOX_CHECK_LE(
      obj["groupingSets"].size(),
      kMaxGroupingSets,
      "A serialized grouping-set aggregation supports at most {} grouping sets",
      kMaxGroupingSets);
  groupingSets.reserve(obj["groupingSets"].size());
  for (const auto& serializedSet : obj["groupingSets"]) {
    GroupingSetSpec set;
    set.groupingId = serializedSet["gid"].asInt();
    const auto numKeys = serializedSet["keyIsActive"].size();
    VELOX_CHECK_EQ(
        numKeys, groupingKeys.size(), "Every serialized grouping-set mask must match the grouping-key count");
    VELOX_CHECK_LE(numKeys, 64, "A grouping-set mask supports at most 64 keys");
    for (size_t key = 0; key < numKeys; ++key) {
      if (serializedSet["keyIsActive"][key].asBool()) {
        set.activeKeysMask |= GroupingSetMask{1} << key;
      }
    }
    groupingSets.push_back(std::move(set));
  }

  std::optional<int32_t> childGroupedSet;
  if (obj.count("childGroupedSet")) {
    const auto serializedChildGroupedSet = obj["childGroupedSet"].asInt();
    VELOX_CHECK_GE(serializedChildGroupedSet, 0, "Serialized childGroupedSet must be non-negative");
    VELOX_CHECK_LE(
        serializedChildGroupedSet, std::numeric_limits<int32_t>::max(), "Serialized childGroupedSet must fit in int32");
    VELOX_CHECK_LT(
        serializedChildGroupedSet,
        static_cast<int64_t>(groupingSets.size()),
        "Serialized childGroupedSet must identify an existing grouping set");
    childGroupedSet = static_cast<int32_t>(serializedChildGroupedSet);
  }

  return std::make_shared<GroupingSetAggregationNode>(
      obj["id"].asString(),
      std::move(groupingKeys),
      std::move(groupingSets),
      std::move(aggregateNames),
      std::move(aggregates),
      childGroupedSet,
      std::move(sources[0]));
}

void GroupingSetAggregationNode::addDetails(std::stringstream& stream) const {
  stream << "keys: [";
  for (size_t i = 0; i < groupingKeys_.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << groupingKeys_[i]->name();
  }

  stream << "] sets: [";
  for (size_t i = 0; i < groupingSets_.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << "0b";
    for (auto j = static_cast<int32_t>(groupingKeys_.size()) - 1; j >= 0; --j) {
      stream << (groupingSets_[i].containsKey(j) ? '1' : '0');
    }
    stream << "/gid=" << groupingSets_[i].groupingId;
  }

  stream << "] aggregates: " << aggregateNames_.size();
  if (childGroupedSet_.has_value()) {
    stream << " bypass: " << *childGroupedSet_;
  }
}

RowTypePtr GroupingSetAggregationNode::makeOutputType(
    const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys,
    const std::vector<std::string>& aggregateNames,
    const std::vector<core::AggregationNode::Aggregate>& aggregates) {
  VELOX_CHECK_GE(groupingKeys.size(), 1, "A grouping-set aggregation needs a key");
  VELOX_CHECK_LE(groupingKeys.size(), 64, "A grouping-set aggregation supports at most 64 keys");
  VELOX_CHECK(!aggregates.empty(), "A grouping-set aggregation needs at least one aggregate");
  VELOX_CHECK_EQ(aggregateNames.size(), aggregates.size(), "Aggregate names and aggregate definitions must correspond");

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(groupingKeys.size() + aggregates.size() + 1);
  types.reserve(groupingKeys.size() + aggregates.size() + 1);

  for (const auto& groupingKey : groupingKeys) {
    VELOX_CHECK_NOT_NULL(groupingKey);
    names.push_back(groupingKey->name());
    types.push_back(groupingKey->type());
  }

  for (size_t i = 0; i < aggregates.size(); ++i) {
    VELOX_CHECK_NOT_NULL(aggregates[i].call);
    names.push_back(aggregateNames[i]);
    types.push_back(resolveIntermediateType(aggregates[i].call->name(), aggregates[i].rawInputTypes));
  }

  std::unordered_set<std::string> uniqueNames;
  for (const auto& name : names) {
    VELOX_CHECK(
        uniqueNames.insert(name).second, "GroupingSetAggregation output contains duplicate column name '{}'", name);
  }

  std::string gidName{"gid"};
  for (int32_t suffix = 0; std::find(names.begin(), names.end(), gidName) != names.end();) {
    gidName = "gid_" + std::to_string(++suffix);
  }
  names.push_back(std::move(gidName));
  types.push_back(BIGINT());

  return ROW(std::move(names), std::move(types));
}

void GroupingSetAggregationNode::validateSourceContract() const {
  VELOX_CHECK(!groupingSets_.empty(), "A grouping-set aggregation needs at least one grouping set");
  VELOX_CHECK_EQ(aggregateNames_.size(), aggregates_.size());
  VELOX_CHECK_NOT_NULL(sources_[0], "GroupingSetAggregation requires a source");

  const auto& inputType = sources_[0]->outputType();
  VELOX_CHECK_EQ(
      inputType->size(),
      groupingKeys_.size() + aggregates_.size(),
      "GroupingSetAggregation input must be ROW(keys..., accumulators...)");

  for (size_t i = 0; i < groupingKeys_.size(); ++i) {
    VELOX_CHECK_EQ(
        inputType->nameOf(i),
        groupingKeys_[i]->name(),
        "Grouping key {} must be at input channel {}",
        groupingKeys_[i]->name(),
        i);
    VELOX_CHECK(
        inputType->childAt(i)->equivalent(*groupingKeys_[i]->type()),
        "Grouping key {} type disagrees with input channel {}",
        groupingKeys_[i]->name(),
        i);
  }

  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto channel = groupingKeys_.size() + i;
    const auto& expectedType = outputType_->childAt(channel);
    VELOX_CHECK(
        inputType->childAt(channel)->equivalent(*expectedType),
        "Aggregate {} expects intermediate type {} at input channel {}, but "
        "the source provides {}",
        aggregates_[i].call->name(),
        expectedType->toString(),
        channel,
        inputType->childAt(channel)->toString());
  }
}

} // namespace facebook::velox::exec
