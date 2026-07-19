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

#include "compute/kafka/KafkaSplitReader.h"

#include "substrait/SubstraitToVeloxPlan.h"

namespace gluten::kafka {

KafkaPartitionRange kafkaPartitionRangeFromSplit(const KafkaSplitInfo& splitInfo) {
  return KafkaPartitionRange{
      splitInfo.topic,
      splitInfo.partition,
      splitInfo.startOffset,
      splitInfo.endOffset,
      splitInfo.pollTimeoutMs,
      splitInfo.failOnDataLoss,
      splitInfo.includeHeaders,
      splitInfo.params};
}

facebook::velox::RowVectorPtr readKafkaSplit(
    const KafkaSplitInfo& splitInfo,
    KafkaConsumer* consumer,
    facebook::velox::memory::MemoryPool* pool) {
  auto range = kafkaPartitionRangeFromSplit(splitInfo);
  KafkaFiniteReader reader(range, consumer);
  return makeKafkaRowVector(reader.readAll(), range.includeHeaders, pool);
}

} // namespace gluten::kafka
