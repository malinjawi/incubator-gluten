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

#include "velox/common/base/Exceptions.h"

namespace gluten::delta {

void DeltaDeletionVectorBuilder::addDeletedRow(uint64_t rowIndex) {
  // Roaring bitmaps use 32-bit integers, but Delta supports 64-bit row indices
  // For now, we'll check that the row index fits in 32 bits
  // TODO: Implement RoaringBitmapArray for 64-bit support
  VELOX_CHECK_LE(
      rowIndex,
      std::numeric_limits<uint32_t>::max(),
      "Row index {} exceeds 32-bit limit. 64-bit support not yet implemented.",
      rowIndex);

  bitmap_.add(static_cast<uint32_t>(rowIndex));
}

void DeltaDeletionVectorBuilder::addDeletedRange(
    uint64_t startRow,
    uint64_t endRow) {
  VELOX_CHECK_LE(startRow, endRow, "Start row must be <= end row");
  VELOX_CHECK_LE(
      endRow,
      std::numeric_limits<uint32_t>::max(),
      "Row index {} exceeds 32-bit limit. 64-bit support not yet implemented.",
      endRow);

  // Add range [startRow, endRow] inclusive
  bitmap_.addRange(
      static_cast<uint32_t>(startRow), static_cast<uint32_t>(endRow) + 1);
}

void DeltaDeletionVectorBuilder::mergeWith(
    const roaring::Roaring& existingBitmap) {
  bitmap_ |= existingBitmap; // Union operation
}

roaring::Roaring DeltaDeletionVectorBuilder::build() const {
  // Return a copy of the current bitmap
  return bitmap_;
}

uint64_t DeltaDeletionVectorBuilder::cardinality() const {
  return bitmap_.cardinality();
}

bool DeltaDeletionVectorBuilder::empty() const {
  return bitmap_.isEmpty();
}

bool DeltaDeletionVectorBuilder::shouldUseInline() const {
  return estimatedSizeBytes() <= INLINE_THRESHOLD;
}

size_t DeltaDeletionVectorBuilder::estimatedSizeBytes() const {
  // Get portable serialization size
  // Add 4 bytes for magic number that will be prepended
  return 4 + bitmap_.getSizeInBytes(true /* portable */);
}

void DeltaDeletionVectorBuilder::clear() {
  bitmap_.clear();
}

bool DeltaDeletionVectorBuilder::isDeleted(uint64_t rowIndex) const {
  VELOX_CHECK_LE(
      rowIndex,
      std::numeric_limits<uint32_t>::max(),
      "Row index {} exceeds 32-bit limit",
      rowIndex);

  return bitmap_.contains(static_cast<uint32_t>(rowIndex));
}

} // namespace gluten::delta

// Made with Bob
