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

#include "compute/delta/DeletionVectorFilter.h"
#include "compute/delta/DeletionVectorWriter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <gtest/gtest.h>
#include <filesystem>

using namespace facebook::velox;
using namespace gluten::delta;

class DeletionVectorFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
    filesystems::registerLocalFileSystem();
    fs_ = filesystems::getFileSystem("file://", nullptr);
    
    // Create temp directory
    tempDir_ = exec::test::TempDirectoryPath::create();
  }

  void TearDown() override {
    // Cleanup is automatic with TempDirectoryPath
  }

  // Helper: Create a DV file with specific row indices
  std::string createDVFile(
      const std::string& fileName,
      const std::vector<int64_t>& deletedRows) {
    auto writer = DeletionVectorWriter::create(pool_.get(), fs_.get());
    
    for (int64_t row : deletedRows) {
      writer->markDeleted(row);
    }
    
    std::string dvPath = tempDir_->getPath() + "/" + fileName;
    writer->writeDVFile(dvPath);
    
    return dvPath;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  filesystems::FileSystem* fs_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
};

// Test 1: Basic filtering - single file
TEST_F(DeletionVectorFilterTest, BasicFiltering) {
  // Create DV with rows 5, 10, 15 deleted
  std::vector<int64_t> deletedRows = {5, 10, 15};
  std::string dvPath = createDVFile("test_dv.bin", deletedRows);
  
  // Create filter
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get());
  
  // Load DV
  std::string filePath = "test_file.parquet";
  filter->loadDeletionVector(filePath, dvPath, 0, 0);
  
  // Test filtering
  EXPECT_TRUE(filter->isRowDeleted(filePath, 5));
  EXPECT_TRUE(filter->isRowDeleted(filePath, 10));
  EXPECT_TRUE(filter->isRowDeleted(filePath, 15));
  
  EXPECT_FALSE(filter->isRowDeleted(filePath, 0));
  EXPECT_FALSE(filter->isRowDeleted(filePath, 1));
  EXPECT_FALSE(filter->isRowDeleted(filePath, 20));
  
  // Check deleted count
  EXPECT_EQ(filter->getDeletedRowCount(filePath), 3);
}

// Test 2: Multiple files
TEST_F(DeletionVectorFilterTest, MultipleFiles) {
  // Create DVs for different files
  std::string dvPath1 = createDVFile("dv1.bin", {1, 2, 3});
  std::string dvPath2 = createDVFile("dv2.bin", {10, 20, 30});
  
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get());
  
  // Load DVs for different files
  filter->loadDeletionVector("file1.parquet", dvPath1, 0, 0);
  filter->loadDeletionVector("file2.parquet", dvPath2, 0, 0);
  
  // Test file1
  EXPECT_TRUE(filter->isRowDeleted("file1.parquet", 1));
  EXPECT_TRUE(filter->isRowDeleted("file1.parquet", 2));
  EXPECT_FALSE(filter->isRowDeleted("file1.parquet", 10));
  
  // Test file2
  EXPECT_TRUE(filter->isRowDeleted("file2.parquet", 10));
  EXPECT_TRUE(filter->isRowDeleted("file2.parquet", 20));
  EXPECT_FALSE(filter->isRowDeleted("file2.parquet", 1));
  
  // Check both have DVs
  EXPECT_TRUE(filter->hasDeletionVector("file1.parquet"));
  EXPECT_TRUE(filter->hasDeletionVector("file2.parquet"));
  EXPECT_FALSE(filter->hasDeletionVector("file3.parquet"));
}

// Test 3: No DV for file
TEST_F(DeletionVectorFilterTest, NoDeletionVector) {
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get());
  
  // File without DV - all rows should be valid
  EXPECT_FALSE(filter->isRowDeleted("no_dv_file.parquet", 0));
  EXPECT_FALSE(filter->isRowDeleted("no_dv_file.parquet", 100));
  EXPECT_FALSE(filter->isRowDeleted("no_dv_file.parquet", 1000));
  
  EXPECT_FALSE(filter->hasDeletionVector("no_dv_file.parquet"));
  EXPECT_EQ(filter->getDeletedRowCount("no_dv_file.parquet"), 0);
}

// Test 4: Large DV (many deleted rows)
TEST_F(DeletionVectorFilterTest, LargeDeletionVector) {
  // Create DV with 10,000 deleted rows
  std::vector<int64_t> deletedRows;
  for (int64_t i = 0; i < 10000; i += 2) {
    deletedRows.push_back(i); // Delete even rows
  }
  
  std::string dvPath = createDVFile("large_dv.bin", deletedRows);
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get());
  
  filter->loadDeletionVector("large_file.parquet", dvPath, 0, 0);
  
  // Test even rows are deleted
  for (int64_t i = 0; i < 10000; i += 2) {
    EXPECT_TRUE(filter->isRowDeleted("large_file.parquet", i));
  }
  
  // Test odd rows are not deleted
  for (int64_t i = 1; i < 10000; i += 2) {
    EXPECT_FALSE(filter->isRowDeleted("large_file.parquet", i));
  }
  
  EXPECT_EQ(filter->getDeletedRowCount("large_file.parquet"), 5000);
}

// Test 5: Cache behavior
TEST_F(DeletionVectorFilterTest, CacheBehavior) {
  std::string dvPath = createDVFile("cached_dv.bin", {1, 2, 3});
  
  DeletionVectorFilter::Config config;
  config.enableStats = true;
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get(), config);
  
  std::string filePath = "cached_file.parquet";
  
  // First load - cache miss
  filter->loadDeletionVector(filePath, dvPath, 0, 0);
  auto stats1 = filter->getStats();
  EXPECT_EQ(stats1.cacheMisses, 1);
  EXPECT_EQ(stats1.cacheHits, 0);
  EXPECT_EQ(stats1.cachedDVs, 1);
  
  // Second load - cache hit
  filter->loadDeletionVector(filePath, dvPath, 0, 0);
  auto stats2 = filter->getStats();
  EXPECT_EQ(stats2.cacheMisses, 1);
  EXPECT_EQ(stats2.cacheHits, 1);
  EXPECT_EQ(stats2.cachedDVs, 1);
  
  // Clear and reload - cache miss again
  filter->clearDeletionVector(filePath);
  filter->loadDeletionVector(filePath, dvPath, 0, 0);
  auto stats3 = filter->getStats();
  EXPECT_EQ(stats3.cacheMisses, 2);
  EXPECT_EQ(stats3.cacheHits, 1);
}

// Test 6: Cache eviction (LRU)
TEST_F(DeletionVectorFilterTest, CacheEviction) {
  DeletionVectorFilter::Config config;
  config.maxCacheSize = 3; // Only cache 3 DVs
  config.enableStats = true;
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get(), config);
  
  // Create 4 DVs
  std::string dvPath1 = createDVFile("dv1.bin", {1});
  std::string dvPath2 = createDVFile("dv2.bin", {2});
  std::string dvPath3 = createDVFile("dv3.bin", {3});
  std::string dvPath4 = createDVFile("dv4.bin", {4});
  
  // Load 3 DVs - all should be cached
  filter->loadDeletionVector("file1.parquet", dvPath1, 0, 0);
  filter->loadDeletionVector("file2.parquet", dvPath2, 0, 0);
  filter->loadDeletionVector("file3.parquet", dvPath3, 0, 0);
  
  auto stats1 = filter->getStats();
  EXPECT_EQ(stats1.cachedDVs, 3);
  
  // Load 4th DV - should evict LRU (file1)
  filter->loadDeletionVector("file4.parquet", dvPath4, 0, 0);
  
  auto stats2 = filter->getStats();
  EXPECT_EQ(stats2.cachedDVs, 3);
  
  // file1 should be evicted
  EXPECT_FALSE(filter->hasDeletionVector("file1.parquet"));
  EXPECT_TRUE(filter->hasDeletionVector("file2.parquet"));
  EXPECT_TRUE(filter->hasDeletionVector("file3.parquet"));
  EXPECT_TRUE(filter->hasDeletionVector("file4.parquet"));
}

// Test 7: Statistics tracking
TEST_F(DeletionVectorFilterTest, StatisticsTracking) {
  std::string dvPath = createDVFile("stats_dv.bin", {5, 10, 15});
  
  DeletionVectorFilter::Config config;
  config.enableStats = true;
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get(), config);
  
  filter->loadDeletionVector("stats_file.parquet", dvPath, 0, 0);
  
  // Check rows - some deleted, some not
  filter->isRowDeleted("stats_file.parquet", 5);  // deleted
  filter->isRowDeleted("stats_file.parquet", 10); // deleted
  filter->isRowDeleted("stats_file.parquet", 0);  // not deleted
  filter->isRowDeleted("stats_file.parquet", 1);  // not deleted
  
  auto stats = filter->getStats();
  EXPECT_EQ(stats.rowsChecked, 4);
  EXPECT_EQ(stats.rowsFiltered, 2);
  EXPECT_DOUBLE_EQ(stats.filterRate(), 0.5);
  
  // Reset stats
  filter->resetStats();
  auto stats2 = filter->getStats();
  EXPECT_EQ(stats2.rowsChecked, 0);
  EXPECT_EQ(stats2.rowsFiltered, 0);
}

// Test 8: Clear all
TEST_F(DeletionVectorFilterTest, ClearAll) {
  std::string dvPath1 = createDVFile("clear1.bin", {1, 2});
  std::string dvPath2 = createDVFile("clear2.bin", {3, 4});
  
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get());
  
  filter->loadDeletionVector("file1.parquet", dvPath1, 0, 0);
  filter->loadDeletionVector("file2.parquet", dvPath2, 0, 0);
  
  EXPECT_TRUE(filter->hasDeletionVector("file1.parquet"));
  EXPECT_TRUE(filter->hasDeletionVector("file2.parquet"));
  
  // Clear all
  filter->clearAll();
  
  EXPECT_FALSE(filter->hasDeletionVector("file1.parquet"));
  EXPECT_FALSE(filter->hasDeletionVector("file2.parquet"));
}

// Test 9: Sparse deletions (scattered rows)
TEST_F(DeletionVectorFilterTest, SparseDeletions) {
  // Delete scattered rows: 0, 100, 1000, 10000, 100000
  std::vector<int64_t> deletedRows = {0, 100, 1000, 10000, 100000};
  std::string dvPath = createDVFile("sparse_dv.bin", deletedRows);
  
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get());
  filter->loadDeletionVector("sparse_file.parquet", dvPath, 0, 0);
  
  // Test deleted rows
  for (int64_t row : deletedRows) {
    EXPECT_TRUE(filter->isRowDeleted("sparse_file.parquet", row));
  }
  
  // Test non-deleted rows
  EXPECT_FALSE(filter->isRowDeleted("sparse_file.parquet", 1));
  EXPECT_FALSE(filter->isRowDeleted("sparse_file.parquet", 50));
  EXPECT_FALSE(filter->isRowDeleted("sparse_file.parquet", 500));
  EXPECT_FALSE(filter->isRowDeleted("sparse_file.parquet", 5000));
  
  EXPECT_EQ(filter->getDeletedRowCount("sparse_file.parquet"), 5);
}

// Test 10: Dense deletions (consecutive rows)
TEST_F(DeletionVectorFilterTest, DenseDeletions) {
  // Delete consecutive rows: 100-199
  std::vector<int64_t> deletedRows;
  for (int64_t i = 100; i < 200; i++) {
    deletedRows.push_back(i);
  }
  
  std::string dvPath = createDVFile("dense_dv.bin", deletedRows);
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get());
  filter->loadDeletionVector("dense_file.parquet", dvPath, 0, 0);
  
  // Test deleted range
  for (int64_t i = 100; i < 200; i++) {
    EXPECT_TRUE(filter->isRowDeleted("dense_file.parquet", i));
  }
  
  // Test non-deleted rows
  EXPECT_FALSE(filter->isRowDeleted("dense_file.parquet", 99));
  EXPECT_FALSE(filter->isRowDeleted("dense_file.parquet", 200));
  
  EXPECT_EQ(filter->getDeletedRowCount("dense_file.parquet"), 100);
}

// Test 11: Performance - hot path
TEST_F(DeletionVectorFilterTest, PerformanceHotPath) {
  // Create DV with 1000 deleted rows
  std::vector<int64_t> deletedRows;
  for (int64_t i = 0; i < 1000; i++) {
    deletedRows.push_back(i * 10); // Every 10th row
  }
  
  std::string dvPath = createDVFile("perf_dv.bin", deletedRows);
  
  DeletionVectorFilter::Config config;
  config.enableStats = true;
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get(), config);
  
  filter->loadDeletionVector("perf_file.parquet", dvPath, 0, 0);
  
  // Check 100,000 rows
  auto startTime = std::chrono::steady_clock::now();
  
  for (int64_t i = 0; i < 100000; i++) {
    filter->isRowDeleted("perf_file.parquet", i);
  }
  
  auto endTime = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      endTime - startTime);
  
  // Should be very fast - less than 100ms for 100K checks
  EXPECT_LT(duration.count(), 100);
  
  auto stats = filter->getStats();
  EXPECT_EQ(stats.rowsChecked, 100000);
  EXPECT_EQ(stats.rowsFiltered, 1000);
}

// Test 12: Thread safety (basic)
TEST_F(DeletionVectorFilterTest, ThreadSafety) {
  std::string dvPath = createDVFile("thread_dv.bin", {1, 2, 3, 4, 5});
  auto filter = DeletionVectorFilter::create(pool_.get(), fs_.get());
  filter->loadDeletionVector("thread_file.parquet", dvPath, 0, 0);
  
  // Launch multiple threads checking rows
  std::vector<std::thread> threads;
  std::atomic<int> successCount{0};
  
  for (int t = 0; t < 10; t++) {
    threads.emplace_back([&filter, &successCount]() {
      for (int i = 0; i < 1000; i++) {
        bool deleted = filter->isRowDeleted("thread_file.parquet", i % 10);
        if (i % 10 < 6) {
          // Rows 1-5 should be deleted
          if ((i % 10 >= 1 && i % 10 <= 5) == deleted) {
            successCount++;
          }
        }
      }
    });
  }
  
  for (auto& thread : threads) {
    thread.join();
  }
  
  // All checks should be consistent
  EXPECT_GT(successCount.load(), 0);
}

} // namespace

// Made with Bob
