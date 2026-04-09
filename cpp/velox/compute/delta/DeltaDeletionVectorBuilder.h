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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include <roaring/roaring.h>
#include <roaring/roaring.hh>

namespace gluten::delta {

/// Builder for Delta Deletion Vectors.
/// Collects row indices to be deleted and builds a Roaring bitmap.
/// Supports merging with existing deletion vectors.
///
/// Usage:
///   DeltaDeletionVectorBuilder builder;
///   builder.addDeletedRow(10);
///   builder.addDeletedRow(20);
///   builder.addDeletedRange(100, 200);
///   auto bitmap = builder.build();
class DeltaDeletionVectorBuilder {
 public:
  /// Threshold for inline storage (bytes).
  /// DVs smaller than this should be stored inline in the Delta log.
  static constexpr size_t INLINE_THRESHOLD = 1024;

  DeltaDeletionVectorBuilder() = default;

  /// Add a single row index to the deletion set.
  /// @param rowIndex The 0-based row index to mark as deleted
  void addDeletedRow(uint64_t rowIndex);

  /// Add a range of row indices to the deletion set.
  /// The range is inclusive: [startRow, endRow].
  /// @param startRow First row index to mark as deleted (inclusive)
  /// @param endRow Last row index to mark as deleted (inclusive)
  void addDeletedRange(uint64_t startRow, uint64_t endRow);

  /// Merge with an existing deletion vector.
  /// All rows in the existing DV will be added to this builder.
  /// @param existingBitmap The existing Roaring bitmap to merge with
  void mergeWith(const roaring::Roaring& existingBitmap);

  /// Build the final Roaring bitmap.
  /// This can be called multiple times and will return the current state.
  /// @return The Roaring bitmap containing all deleted row indices
  roaring::Roaring build() const;

  /// Get the number of deleted rows (cardinality).
  /// @return The count of unique row indices marked as deleted
  uint64_t cardinality() const;

  /// Check if the deletion vector is empty.
  /// @return true if no rows have been marked as deleted
  bool empty() const;

  /// Decide if this DV should use inline storage.
  /// Based on the serialized size compared to INLINE_THRESHOLD.
  /// @return true if the DV should be stored inline in the Delta log
  bool shouldUseInline() const;

  /// Get the estimated serialized size in bytes.
  /// This is the size of the portable Roaring bitmap format.
  /// @return Estimated size in bytes
  size_t estimatedSizeBytes() const;

  /// Clear all deleted rows.
  /// Resets the builder to an empty state.
  void clear();

  /// Check if a specific row is marked as deleted.
  /// Useful for testing and validation.
  /// @param rowIndex The row index to check
  /// @return true if the row is marked as deleted
  bool isDeleted(uint64_t rowIndex) const;

 private:
  roaring::Roaring bitmap_;
};

} // namespace gluten::delta

// Made with Bob
