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

#include "compute/delta/DeltaDeletionVectorBuilder.h"

#include <gtest/gtest.h>

using namespace gluten::delta;

class DeltaDeletionVectorBuilderTest : public testing::Test {};

TEST_F(DeltaDeletionVectorBuilderTest, AddSingleRow) {
  DeltaDeletionVectorBuilder builder;

  builder.addDeletedRow(10);
  builder.addDeletedRow(20);
  builder.addDeletedRow(30);

  EXPECT_EQ(builder.cardinality(), 3);
  EXPECT_FALSE(builder.empty());

  auto bitmap = builder.build();
  EXPECT_TRUE(bitmap.contains(10));
  EXPECT_TRUE(bitmap.contains(20));
  EXPECT_TRUE(bitmap.contains(30));
  EXPECT_FALSE(bitmap.contains(15));
}

TEST_F(DeltaDeletionVectorBuilderTest, AddRange) {
  DeltaDeletionVectorBuilder builder;

  builder.addDeletedRange(100, 199);

  EXPECT_EQ(builder.cardinality(), 100);

  auto bitmap = builder.build();
  for (uint32_t i = 100; i < 200; i++) {
    EXPECT_TRUE(bitmap.contains(i));
  }
  EXPECT_FALSE(bitmap.contains(99));
  EXPECT_FALSE(bitmap.contains(200));
}

TEST_F(DeltaDeletionVectorBuilderTest, AddMultipleRanges) {
  DeltaDeletionVectorBuilder builder;

  builder.addDeletedRange(10, 19);
  builder.addDeletedRange(30, 39);
  builder.addDeletedRange(50, 59);

  EXPECT_EQ(builder.cardinality(), 30);

  auto bitmap = builder.build();

  // Verify ranges
  for (uint32_t i = 10; i < 20; i++) {
    EXPECT_TRUE(bitmap.contains(i));
  }
  for (uint32_t i = 30; i < 40; i++) {
    EXPECT_TRUE(bitmap.contains(i));
  }
  for (uint32_t i = 50; i < 60; i++) {
    EXPECT_TRUE(bitmap.contains(i));
  }

  // Verify gaps
  for (uint32_t i = 20; i < 30; i++) {
    EXPECT_FALSE(bitmap.contains(i));
  }
  for (uint32_t i = 40; i < 50; i++) {
    EXPECT_FALSE(bitmap.contains(i));
  }
}

TEST_F(DeltaDeletionVectorBuilderTest, MixedAdditions) {
  DeltaDeletionVectorBuilder builder;

  builder.addDeletedRow(5);
  builder.addDeletedRange(10, 20);
  builder.addDeletedRow(25);
  builder.addDeletedRow(30);

  EXPECT_EQ(builder.cardinality(), 14); // 1 + 11 + 1 + 1

  auto bitmap = builder.build();
  EXPECT_TRUE(bitmap.contains(5));
  EXPECT_TRUE(bitmap.contains(15));
  EXPECT_TRUE(bitmap.contains(25));
  EXPECT_TRUE(bitmap.contains(30));
  EXPECT_FALSE(bitmap.contains(22));
}

TEST_F(DeltaDeletionVectorBuilderTest, MergeWithExisting) {
  // Create existing bitmap
  roaring::Roaring existing;
  existing.add(1);
  existing.add(2);
  existing.add(3);

  // Create builder with new deletions
  DeltaDeletionVectorBuilder builder;
  builder.addDeletedRow(4);
  builder.addDeletedRow(5);

  // Merge
  builder.mergeWith(existing);

  EXPECT_EQ(builder.cardinality(), 5);

  auto bitmap = builder.build();
  EXPECT_TRUE(bitmap.contains(1));
  EXPECT_TRUE(bitmap.contains(2));
  EXPECT_TRUE(bitmap.contains(3));
  EXPECT_TRUE(bitmap.contains(4));
  EXPECT_TRUE(bitmap.contains(5));
}

TEST_F(DeltaDeletionVectorBuilderTest, MergeOverlapping) {
  // Create existing bitmap with some values
  roaring::Roaring existing;
  existing.addRange(10, 20);

  // Create builder with overlapping values
  DeltaDeletionVectorBuilder builder;
  builder.addDeletedRange(15, 25);

  // Merge
  builder.mergeWith(existing);

  // Should have union: [10, 25)
  EXPECT_EQ(builder.cardinality(), 15);

  auto bitmap = builder.build();
  for (uint32_t i = 10; i < 25; i++) {
    EXPECT_TRUE(bitmap.contains(i));
  }
  EXPECT_FALSE(bitmap.contains(9));
  EXPECT_FALSE(bitmap.contains(25));
}

TEST_F(DeltaDeletionVectorBuilderTest, EmptyBuilder) {
  DeltaDeletionVectorBuilder builder;

  EXPECT_TRUE(builder.empty());
  EXPECT_EQ(builder.cardinality(), 0);

  auto bitmap = builder.build();
  EXPECT_TRUE(bitmap.isEmpty());
}

TEST_F(DeltaDeletionVectorBuilderTest, Clear) {
  DeltaDeletionVectorBuilder builder;

  builder.addDeletedRow(10);
  builder.addDeletedRow(20);
  EXPECT_FALSE(builder.empty());
  EXPECT_EQ(builder.cardinality(), 2);

  builder.clear();
  EXPECT_TRUE(builder.empty());
  EXPECT_EQ(builder.cardinality(), 0);
}

TEST_F(DeltaDeletionVectorBuilderTest, IsDeleted) {
  DeltaDeletionVectorBuilder builder;

  builder.addDeletedRow(10);
  builder.addDeletedRow(20);

  EXPECT_TRUE(builder.isDeleted(10));
  EXPECT_TRUE(builder.isDeleted(20));
  EXPECT_FALSE(builder.isDeleted(15));
  EXPECT_FALSE(builder.isDeleted(0));
}

TEST_F(DeltaDeletionVectorBuilderTest, InlineDecision) {
  // Small bitmap - should be inline
  DeltaDeletionVectorBuilder smallBuilder;
  for (uint32_t i = 0; i < 10; i++) {
    smallBuilder.addDeletedRow(i);
  }
  EXPECT_TRUE(smallBuilder.shouldUseInline());

  // Large bitmap - should not be inline
  DeltaDeletionVectorBuilder largeBuilder;
  for (uint32_t i = 0; i < 10000; i++) {
    largeBuilder.addDeletedRow(i);
  }
  EXPECT_FALSE(largeBuilder.shouldUseInline());
}

TEST_F(DeltaDeletionVectorBuilderTest, EstimatedSize) {
  DeltaDeletionVectorBuilder builder;

  // Empty bitmap has some overhead
  size_t emptySize = builder.estimatedSizeBytes();
  EXPECT_GT(emptySize, 0);

  // Adding rows increases size
  builder.addDeletedRow(10);
  size_t sizeWith1 = builder.estimatedSizeBytes();
  EXPECT_GT(sizeWith1, emptySize);

  // Adding more rows increases size further
  for (uint32_t i = 0; i < 1000; i++) {
    builder.addDeletedRow(i);
  }
  size_t sizeWith1000 = builder.estimatedSizeBytes();
  EXPECT_GT(sizeWith1000, sizeWith1);
}

TEST_F(DeltaDeletionVectorBuilderTest, ConsecutiveRangesCompression) {
  // Consecutive ranges should compress well with run-length encoding
  DeltaDeletionVectorBuilder builder;

  // Add large consecutive range
  builder.addDeletedRange(0, 9999);

  // Size should be relatively small due to RLE compression
  size_t size = builder.estimatedSizeBytes();

  // Compare with sparse additions (less compressible)
  DeltaDeletionVectorBuilder sparseBuilder;
  for (uint32_t i = 0; i < 10000; i += 100) {
    sparseBuilder.addDeletedRow(i);
  }
  size_t sparseSize = sparseBuilder.estimatedSizeBytes();

  // Consecutive should be smaller than sparse
  EXPECT_LT(size, sparseSize);
}

TEST_F(DeltaDeletionVectorBuilderTest, DuplicateAdditions) {
  DeltaDeletionVectorBuilder builder;

  builder.addDeletedRow(10);
  builder.addDeletedRow(10); // Duplicate
  builder.addDeletedRow(10); // Duplicate

  // Cardinality should still be 1
  EXPECT_EQ(builder.cardinality(), 1);

  auto bitmap = builder.build();
  EXPECT_TRUE(bitmap.contains(10));
}

TEST_F(DeltaDeletionVectorBuilderTest, LargeRowIndices) {
  DeltaDeletionVectorBuilder builder;

  // Add rows near the 32-bit limit
  uint64_t largeIndex = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) - 10;

  builder.addDeletedRow(largeIndex);
  builder.addDeletedRow(largeIndex + 5);

  EXPECT_EQ(builder.cardinality(), 2);
  EXPECT_TRUE(builder.isDeleted(largeIndex));
  EXPECT_TRUE(builder.isDeleted(largeIndex + 5));
}

TEST_F(DeltaDeletionVectorBuilderTest, BuildMultipleTimes) {
  DeltaDeletionVectorBuilder builder;

  builder.addDeletedRow(10);
  auto bitmap1 = builder.build();

  builder.addDeletedRow(20);
  auto bitmap2 = builder.build();

  // First build should have 1 element
  EXPECT_EQ(bitmap1.cardinality(), 1);
  EXPECT_TRUE(bitmap1.contains(10));
  EXPECT_FALSE(bitmap1.contains(20));

  // Second build should have 2 elements
  EXPECT_EQ(bitmap2.cardinality(), 2);
  EXPECT_TRUE(bitmap2.contains(10));
  EXPECT_TRUE(bitmap2.contains(20));
}

// Made with Bob
