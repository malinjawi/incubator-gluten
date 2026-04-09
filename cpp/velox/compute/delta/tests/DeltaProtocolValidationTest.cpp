/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include "compute/delta/DeltaSplit.h"
#include "velox/dwio/common/FileSink.h"

using namespace gluten::delta;
namespace dwio = facebook::velox::dwio;

class DeltaProtocolValidationTest : public ::testing::Test {};

// ============================================================================
// DeltaProtocolInfo Tests
// ============================================================================

TEST_F(DeltaProtocolValidationTest, ProtocolSupportsDeletionVectorsValid) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 7,
      .readerFeatures = std::vector<std::string>{"deletionVectors"},
      .writerFeatures = std::vector<std::string>{"deletionVectors"}};

  EXPECT_TRUE(protocol.supportsDeletionVectors());
}

TEST_F(DeltaProtocolValidationTest, ProtocolReaderVersionTooLow) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 2,
      .minWriterVersion = 7,
      .readerFeatures = std::vector<std::string>{"deletionVectors"},
      .writerFeatures = std::vector<std::string>{"deletionVectors"}};

  EXPECT_FALSE(protocol.supportsDeletionVectors());
}

TEST_F(DeltaProtocolValidationTest, ProtocolWriterVersionTooLow) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 6,
      .readerFeatures = std::vector<std::string>{"deletionVectors"},
      .writerFeatures = std::vector<std::string>{"deletionVectors"}};

  EXPECT_FALSE(protocol.supportsDeletionVectors());
}

TEST_F(DeltaProtocolValidationTest, ProtocolMissingFeatureFlag) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 7,
      .readerFeatures = std::vector<std::string>{"someOtherFeature"},
      .writerFeatures = std::vector<std::string>{"deletionVectors"}};

  EXPECT_FALSE(protocol.supportsDeletionVectors());
}

TEST_F(DeltaProtocolValidationTest, ProtocolNoReaderFeatures) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 7,
      .readerFeatures = std::nullopt,
      .writerFeatures = std::vector<std::string>{"deletionVectors"}};

  EXPECT_FALSE(protocol.supportsDeletionVectors());
}

TEST_F(DeltaProtocolValidationTest, ProtocolEmptyReaderFeatures) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 7,
      .readerFeatures = std::vector<std::string>{},
      .writerFeatures = std::vector<std::string>{"deletionVectors"}};

  EXPECT_FALSE(protocol.supportsDeletionVectors());
}

TEST_F(DeltaProtocolValidationTest, ProtocolMultipleFeatures) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 7,
      .readerFeatures =
          std::vector<std::string>{"columnMapping", "deletionVectors"},
      .writerFeatures =
          std::vector<std::string>{"columnMapping", "deletionVectors"}};

  EXPECT_TRUE(protocol.supportsDeletionVectors());
}

// ============================================================================
// DeltaFileStatistics Tests
// ============================================================================

TEST_F(DeltaProtocolValidationTest, StatisticsLogicalRowCountNoDV) {
  DeltaFileStatistics stats{.numRecords = 1000, .tightBounds = true};

  auto logicalRows = stats.logicalRowCount(std::nullopt);
  EXPECT_EQ(logicalRows, 1000);
}

TEST_F(DeltaProtocolValidationTest, StatisticsLogicalRowCountWithDV) {
  DeltaFileStatistics stats{.numRecords = 1000, .tightBounds = true};

  auto dv = DeltaDeletionVectorDescriptor::inlineData("test", 100);
  auto logicalRows = stats.logicalRowCount(dv);
  EXPECT_EQ(logicalRows, 900);
}

TEST_F(DeltaProtocolValidationTest, StatisticsLogicalRowCountDVNoCardinality) {
  DeltaFileStatistics stats{.numRecords = 1000, .tightBounds = true};

  auto dv = DeltaDeletionVectorDescriptor::inlineData("test", std::nullopt);
  auto logicalRows = stats.logicalRowCount(dv);
  EXPECT_EQ(logicalRows, 1000); // No cardinality, so no deletions
}

TEST_F(DeltaProtocolValidationTest, StatisticsLogicalRowCountNoNumRecords) {
  DeltaFileStatistics stats{.numRecords = std::nullopt, .tightBounds = true};

  auto dv = DeltaDeletionVectorDescriptor::inlineData("test", 100);
  auto logicalRows = stats.logicalRowCount(dv);
  EXPECT_EQ(logicalRows, -1); // Unknown
}

TEST_F(DeltaProtocolValidationTest, StatisticsLogicalRowCountAllDeleted) {
  DeltaFileStatistics stats{.numRecords = 100, .tightBounds = true};

  auto dv = DeltaDeletionVectorDescriptor::inlineData("test", 100);
  auto logicalRows = stats.logicalRowCount(dv);
  EXPECT_EQ(logicalRows, 0);
}

TEST_F(DeltaProtocolValidationTest, StatisticsLogicalRowCountLargeFile) {
  DeltaFileStatistics stats{
      .numRecords = 10000000000LL, // 10 billion rows
      .tightBounds = true};

  auto dv = DeltaDeletionVectorDescriptor::inlineData("test", 1000000);
  auto logicalRows = stats.logicalRowCount(dv);
  EXPECT_EQ(logicalRows, 9999000000LL);
}

// ============================================================================
// HiveDeltaSplit Constructor Tests
// ============================================================================

TEST_F(DeltaProtocolValidationTest, SplitWithProtocolAndStatistics) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 7,
      .readerFeatures = std::vector<std::string>{"deletionVectors"},
      .writerFeatures = std::vector<std::string>{"deletionVectors"}};

  DeltaFileStatistics stats{.numRecords = 1000, .tightBounds = true};

  auto dv = DeltaDeletionVectorDescriptor::inlineData("test", 100);

  HiveDeltaSplit split(
      "test-connector",
      "file.parquet",
      dwio::common::FileFormat::PARQUET,
      0,
      1000,
      {},
      std::nullopt,
      {},
      nullptr,
      {},
      true,
      dv,
      protocol,
      stats);

  EXPECT_TRUE(split.deletionVector.has_value());
  EXPECT_TRUE(split.protocolInfo.has_value());
  EXPECT_TRUE(split.statistics.has_value());
  EXPECT_EQ(split.statistics->numRecords, 1000);
  EXPECT_TRUE(split.protocolInfo->supportsDeletionVectors());
}

TEST_F(DeltaProtocolValidationTest, SplitWithoutProtocolAndStatistics) {
  auto dv = DeltaDeletionVectorDescriptor::inlineData("test", 100);

  HiveDeltaSplit split(
      "test-connector",
      "file.parquet",
      dwio::common::FileFormat::PARQUET,
      0,
      1000,
      {},
      std::nullopt,
      {},
      nullptr,
      {},
      true,
      dv,
      std::nullopt, // No protocol
      std::nullopt); // No statistics

  EXPECT_TRUE(split.deletionVector.has_value());
  EXPECT_FALSE(split.protocolInfo.has_value());
  EXPECT_FALSE(split.statistics.has_value());
}

TEST_F(DeltaProtocolValidationTest, SplitWithoutDV) {
  HiveDeltaSplit split(
      "test-connector",
      "file.parquet",
      dwio::common::FileFormat::PARQUET,
      0,
      1000,
      {},
      std::nullopt,
      {},
      nullptr,
      {},
      true,
      std::nullopt, // No DV
      std::nullopt,
      std::nullopt);

  EXPECT_FALSE(split.deletionVector.has_value());
  EXPECT_FALSE(split.protocolInfo.has_value());
  EXPECT_FALSE(split.statistics.has_value());
}

// ============================================================================
// Edge Cases and Boundary Tests
// ============================================================================

TEST_F(DeltaProtocolValidationTest, ProtocolExactMinimumVersions) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 7,
      .readerFeatures = std::vector<std::string>{"deletionVectors"},
      .writerFeatures = std::vector<std::string>{}};

  EXPECT_TRUE(protocol.supportsDeletionVectors());
}

TEST_F(DeltaProtocolValidationTest, ProtocolHigherVersions) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 10,
      .minWriterVersion = 15,
      .readerFeatures = std::vector<std::string>{"deletionVectors"},
      .writerFeatures = std::vector<std::string>{"deletionVectors"}};

  EXPECT_TRUE(protocol.supportsDeletionVectors());
}

TEST_F(DeltaProtocolValidationTest, StatisticsTightBoundsTrue) {
  DeltaFileStatistics stats{.numRecords = 1000, .tightBounds = true};

  EXPECT_TRUE(stats.tightBounds.has_value());
  EXPECT_TRUE(*stats.tightBounds);
}

TEST_F(DeltaProtocolValidationTest, StatisticsTightBoundsFalse) {
  DeltaFileStatistics stats{.numRecords = 1000, .tightBounds = false};

  EXPECT_TRUE(stats.tightBounds.has_value());
  EXPECT_FALSE(*stats.tightBounds);
}

TEST_F(DeltaProtocolValidationTest, StatisticsTightBoundsUnspecified) {
  DeltaFileStatistics stats{.numRecords = 1000, .tightBounds = std::nullopt};

  EXPECT_FALSE(stats.tightBounds.has_value());
}

TEST_F(DeltaProtocolValidationTest, StatisticsZeroRows) {
  DeltaFileStatistics stats{.numRecords = 0, .tightBounds = true};

  auto logicalRows = stats.logicalRowCount(std::nullopt);
  EXPECT_EQ(logicalRows, 0);
}

TEST_F(DeltaProtocolValidationTest, StatisticsZeroRowsWithDV) {
  DeltaFileStatistics stats{.numRecords = 0, .tightBounds = true};

  auto dv = DeltaDeletionVectorDescriptor::inlineData("test", 0);
  auto logicalRows = stats.logicalRowCount(dv);
  EXPECT_EQ(logicalRows, 0);
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST_F(DeltaProtocolValidationTest, CompleteValidScenario) {
  // Create a complete valid scenario with all components
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 7,
      .readerFeatures =
          std::vector<std::string>{"deletionVectors", "columnMapping"},
      .writerFeatures =
          std::vector<std::string>{"deletionVectors", "columnMapping"}};

  DeltaFileStatistics stats{.numRecords = 10000, .tightBounds = true};

  auto dv = DeltaDeletionVectorDescriptor::filePath(
      "/path/to/dv.bin", 0, 1024, 500);

  HiveDeltaSplit split(
      "test-connector",
      "file.parquet",
      dwio::common::FileFormat::PARQUET,
      0,
      1000000,
      {},
      std::nullopt,
      {},
      nullptr,
      {},
      true,
      dv,
      protocol,
      stats);

  // Verify all components
  EXPECT_TRUE(split.protocolInfo->supportsDeletionVectors());
  EXPECT_EQ(split.statistics->logicalRowCount(split.deletionVector), 9500);
  EXPECT_TRUE(split.statistics->tightBounds.value());
}

TEST_F(DeltaProtocolValidationTest, CompleteScenarioLooseBounds) {
  DeltaProtocolInfo protocol{
      .minReaderVersion = 3,
      .minWriterVersion = 7,
      .readerFeatures = std::vector<std::string>{"deletionVectors"},
      .writerFeatures = std::vector<std::string>{"deletionVectors"}};

  DeltaFileStatistics stats{
      .numRecords = 10000,
      .tightBounds = false // Loose bounds
  };

  auto dv = DeltaDeletionVectorDescriptor::inlineData("test", 500);

  HiveDeltaSplit split(
      "test-connector",
      "file.parquet",
      dwio::common::FileFormat::PARQUET,
      0,
      1000000,
      {},
      std::nullopt,
      {},
      nullptr,
      {},
      true,
      dv,
      protocol,
      stats);

  EXPECT_TRUE(split.protocolInfo->supportsDeletionVectors());
  EXPECT_EQ(split.statistics->logicalRowCount(split.deletionVector), 9500);
  EXPECT_FALSE(split.statistics->tightBounds.value());
}

// Made with Bob
