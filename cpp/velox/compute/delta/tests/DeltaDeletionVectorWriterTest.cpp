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

#include "compute/delta/DeltaDeletionVectorWriter.h"
#include "compute/delta/DeltaDeletionVectorReader.h"

#include <gtest/gtest.h>
#include <roaring/roaring.h>

#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox;
using namespace gluten::delta;

class DeltaDeletionVectorWriterTest : public testing::Test {
 protected:
  void SetUp() override {
    tempDir_ = exec::test::TempDirectoryPath::create();
    filesystems::registerLocalFileSystem();
    pool_ = memory::memoryManager()->addLeafPool();
  }

  std::string getTempPath(const std::string& filename) {
    return tempDir_->getPath() + "/" + filename;
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(DeltaDeletionVectorWriterTest, WriteInlineDV) {
  // Create a small bitmap (should be stored inline)
  roaring::Roaring bitmap;
  bitmap.add(1);
  bitmap.add(5);
  bitmap.add(10);

  auto fs = filesystems::getFileSystem(tempDir_->getPath(), nullptr);
  DeltaDeletionVectorWriter writer(fs, pool_.get());

  // Write with force inline
  auto descriptor = writer.writeDeletionVector(
      tempDir_->getPath(), bitmap, true /* forceInline */);

  // Verify descriptor
  EXPECT_EQ(descriptor.storageType, DeltaDeletionVectorStorageType::kInlineData);
  EXPECT_FALSE(descriptor.pathOrInlineData.empty());
  EXPECT_FALSE(descriptor.offset.has_value());
  EXPECT_GT(descriptor.sizeInBytes, 0);
  EXPECT_EQ(descriptor.cardinality, 3);
}

TEST_F(DeltaDeletionVectorWriterTest, WriteOnDiskDV) {
  // Create a larger bitmap (should be stored on disk)
  roaring::Roaring bitmap;
  for (uint32_t i = 0; i < 10000; i++) {
    bitmap.add(i);
  }

  auto fs = filesystems::getFileSystem(tempDir_->getPath(), nullptr);
  DeltaDeletionVectorWriter writer(fs, pool_.get());

  // Write without forcing inline (should go to disk)
  auto descriptor = writer.writeDeletionVector(
      tempDir_->getPath(), bitmap, false /* forceInline */);

  // Verify descriptor
  EXPECT_EQ(descriptor.storageType, DeltaDeletionVectorStorageType::kFilePath);
  EXPECT_FALSE(descriptor.pathOrInlineData.empty());
  EXPECT_TRUE(descriptor.offset.has_value());
  EXPECT_GT(descriptor.sizeInBytes, 0);
  EXPECT_EQ(descriptor.cardinality, 10000);

  // Verify file exists
  auto readFile = fs->openFileForRead(descriptor.pathOrInlineData);
  EXPECT_NE(readFile, nullptr);
}

TEST_F(DeltaDeletionVectorWriterTest, WriteAndReadRoundtrip) {
  // Create a bitmap with specific pattern
  roaring::Roaring originalBitmap;
  originalBitmap.add(3);
  originalBitmap.add(7);
  originalBitmap.add(11);
  originalBitmap.add(42);
  originalBitmap.add(100);

  auto fs = filesystems::getFileSystem(tempDir_->getPath(), nullptr);
  DeltaDeletionVectorWriter writer(fs, pool_.get());

  // Write to disk
  std::string dvPath = getTempPath("test_dv.bin");
  auto descriptor = writer.writeDeletionVectorToFile(dvPath, originalBitmap, 0);

  // Read it back
  DeltaDeletionVectorReader reader(fs, pool_.get(), nullptr);
  reader.loadDeletionVector(
      dvPath,
      descriptor.offset.value(),
      descriptor.sizeInBytes,
      descriptor.cardinality);

  // Verify all original indices are marked as deleted
  EXPECT_TRUE(reader.isDeleted(3));
  EXPECT_TRUE(reader.isDeleted(7));
  EXPECT_TRUE(reader.isDeleted(11));
  EXPECT_TRUE(reader.isDeleted(42));
  EXPECT_TRUE(reader.isDeleted(100));

  // Verify non-deleted indices
  EXPECT_FALSE(reader.isDeleted(0));
  EXPECT_FALSE(reader.isDeleted(1));
  EXPECT_FALSE(reader.isDeleted(50));
  EXPECT_FALSE(reader.isDeleted(200));
}

TEST_F(DeltaDeletionVectorWriterTest, WriteLargeBitmap) {
  // Create a large bitmap with sparse deletions
  roaring::Roaring bitmap;
  for (uint32_t i = 0; i < 1000000; i += 100) {
    bitmap.add(i);
  }

  auto fs = filesystems::getFileSystem(tempDir_->getPath(), nullptr);
  DeltaDeletionVectorWriter writer(fs, pool_.get());

  std::string dvPath = getTempPath("large_dv.bin");
  auto descriptor = writer.writeDeletionVectorToFile(dvPath, bitmap, 0);

  // Verify descriptor
  EXPECT_EQ(descriptor.cardinality, 10000);
  EXPECT_GT(descriptor.sizeInBytes, 0);

  // Read it back and verify
  DeltaDeletionVectorReader reader(fs, pool_.get(), nullptr);
  reader.loadDeletionVector(
      dvPath,
      descriptor.offset.value(),
      descriptor.sizeInBytes,
      descriptor.cardinality);

  // Spot check some values
  EXPECT_TRUE(reader.isDeleted(0));
  EXPECT_TRUE(reader.isDeleted(100));
  EXPECT_TRUE(reader.isDeleted(1000));
  EXPECT_FALSE(reader.isDeleted(1));
  EXPECT_FALSE(reader.isDeleted(99));
  EXPECT_FALSE(reader.isDeleted(101));
}

TEST_F(DeltaDeletionVectorWriterTest, WriteEmptyBitmap) {
  // Create an empty bitmap
  roaring::Roaring bitmap;

  auto fs = filesystems::getFileSystem(tempDir_->getPath(), nullptr);
  DeltaDeletionVectorWriter writer(fs, pool_.get());

  // Write inline
  auto descriptor = writer.writeDeletionVector(
      tempDir_->getPath(), bitmap, true /* forceInline */);

  // Verify descriptor
  EXPECT_EQ(descriptor.cardinality, 0);
  EXPECT_GT(descriptor.sizeInBytes, 0); // Still has magic number
}

TEST_F(DeltaDeletionVectorWriterTest, WriteConsecutiveRanges) {
  // Create bitmap with consecutive ranges (good for run-length encoding)
  roaring::Roaring bitmap;
  for (uint32_t i = 100; i < 200; i++) {
    bitmap.add(i);
  }
  for (uint32_t i = 500; i < 600; i++) {
    bitmap.add(i);
  }

  auto fs = filesystems::getFileSystem(tempDir_->getPath(), nullptr);
  DeltaDeletionVectorWriter writer(fs, pool_.get());

  std::string dvPath = getTempPath("ranges_dv.bin");
  auto descriptor = writer.writeDeletionVectorToFile(dvPath, bitmap, 0);

  // Verify cardinality
  EXPECT_EQ(descriptor.cardinality, 200);

  // Read back and verify ranges
  DeltaDeletionVectorReader reader(fs, pool_.get(), nullptr);
  reader.loadDeletionVector(
      dvPath,
      descriptor.offset.value(),
      descriptor.sizeInBytes,
      descriptor.cardinality);

  // Verify first range
  for (uint32_t i = 100; i < 200; i++) {
    EXPECT_TRUE(reader.isDeleted(i));
  }

  // Verify second range
  for (uint32_t i = 500; i < 600; i++) {
    EXPECT_TRUE(reader.isDeleted(i));
  }

  // Verify gaps
  EXPECT_FALSE(reader.isDeleted(99));
  EXPECT_FALSE(reader.isDeleted(200));
  EXPECT_FALSE(reader.isDeleted(499));
  EXPECT_FALSE(reader.isDeleted(600));
}

TEST_F(DeltaDeletionVectorWriterTest, InlineThreshold) {
  auto fs = filesystems::getFileSystem(tempDir_->getPath(), nullptr);
  DeltaDeletionVectorWriter writer(fs, pool_.get());

  // Create a bitmap just under the inline threshold
  roaring::Roaring smallBitmap;
  for (uint32_t i = 0; i < 50; i++) {
    smallBitmap.add(i);
  }

  auto smallDescriptor = writer.writeDeletionVector(
      tempDir_->getPath(), smallBitmap, false);

  // Should be inline
  EXPECT_EQ(smallDescriptor.storageType, DeltaDeletionVectorStorageType::kInlineData);

  // Create a bitmap over the inline threshold
  roaring::Roaring largeBitmap;
  for (uint32_t i = 0; i < 5000; i++) {
    largeBitmap.add(i);
  }

  auto largeDescriptor = writer.writeDeletionVector(
      tempDir_->getPath(), largeBitmap, false);

  // Should be on disk
  EXPECT_EQ(largeDescriptor.storageType, DeltaDeletionVectorStorageType::kFilePath);
}

TEST_F(DeltaDeletionVectorWriterTest, MultipleWrites) {
  auto fs = filesystems::getFileSystem(tempDir_->getPath(), nullptr);
  DeltaDeletionVectorWriter writer(fs, pool_.get());

  // Write multiple DVs
  std::vector<roaring::Roaring> bitmaps;
  std::vector<DeltaDeletionVectorDescriptor> descriptors;

  for (int i = 0; i < 5; i++) {
    roaring::Roaring bitmap;
    for (uint32_t j = i * 100; j < (i + 1) * 100; j++) {
      bitmap.add(j);
    }
    bitmaps.push_back(bitmap);

    auto descriptor = writer.writeDeletionVector(
        tempDir_->getPath(), bitmap, false);
    descriptors.push_back(descriptor);
  }

  // Verify all descriptors are unique
  for (size_t i = 0; i < descriptors.size(); i++) {
    for (size_t j = i + 1; j < descriptors.size(); j++) {
      EXPECT_NE(descriptors[i].pathOrInlineData, descriptors[j].pathOrInlineData);
    }
  }

  // Verify each can be read back correctly
  for (size_t i = 0; i < descriptors.size(); i++) {
    if (descriptors[i].storageType == DeltaDeletionVectorStorageType::kFilePath) {
      DeltaDeletionVectorReader reader(fs, pool_.get(), nullptr);
      reader.loadDeletionVector(
          descriptors[i].pathOrInlineData,
          descriptors[i].offset.value(),
          descriptors[i].sizeInBytes,
          descriptors[i].cardinality);

      // Verify the range
      for (uint32_t j = i * 100; j < (i + 1) * 100; j++) {
        EXPECT_TRUE(reader.isDeleted(j));
      }
    }
  }
}

// Made with Bob
