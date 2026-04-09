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
#include <string>
#include <vector>

#include <roaring/roaring.hh>

#include "velox/common/file/FileSystems.h"
#include "compute/delta/DeltaSplit.h"

namespace gluten::delta {

/// Writer for Delta Deletion Vectors.
/// Serializes Roaring bitmaps to Delta's on-disk format and writes them
/// to storage (inline or as separate files).
///
/// Delta DV Format (per spec):
/// - File format:
///   Bytes 0-1:   version (int16, big-endian) = 1
///   For each DV:
///     Bytes 0-3:   payloadSize (int32, big-endian)
///     Bytes 4-...: payload (RoaringBitmap in portable format)
///     Bytes ...-end: crc32 (int32, big-endian)
///
/// The payload is a RoaringBitmap serialized in portable format with:
///   Bytes 0-3:   magicNumber = 1681511377 (0x64454C54)
///   Bytes 4-end: portable Roaring bitmap data
class DeltaDeletionVectorWriter {
 public:
  /// Magic number for RoaringBitmapArray format (per Delta spec)
  static constexpr uint32_t MAGIC_NUMBER = 1681511377;

  /// File format version
  static constexpr uint16_t FORMAT_VERSION = 1;

  /// Threshold for inline storage (bytes)
  /// DVs smaller than this are stored inline in the Delta log
  static constexpr size_t INLINE_THRESHOLD = 1024;

  explicit DeltaDeletionVectorWriter(
      std::shared_ptr<filesystems::FileSystem> fileSystem,
      memory::MemoryPool* pool);

  /// Write a deletion vector to storage.
  /// Decides between inline and on-disk storage based on size.
  ///
  /// @param tablePath Base path of the Delta table
  /// @param bitmap The Roaring bitmap containing deleted row indices
  /// @param forceInline If true, always use inline storage regardless of size
  /// @return DeletionVectorDescriptor describing where the DV was written
  DeltaDeletionVectorDescriptor writeDeletionVector(
      const std::string& tablePath,
      const roaring::Roaring& bitmap,
      bool forceInline = false);

  /// Write a deletion vector to a specific file at a specific offset.
  /// Used when appending multiple DVs to a single file.
  ///
  /// @param filePath Full path to the DV file
  /// @param bitmap The Roaring bitmap containing deleted row indices
  /// @param offset Offset in the file where this DV should be written
  /// @return DeletionVectorDescriptor describing the written DV
  DeltaDeletionVectorDescriptor writeDeletionVectorToFile(
      const std::string& filePath,
      const roaring::Roaring& bitmap,
      uint64_t offset = 0);

 private:
  /// Serialize a Roaring bitmap to Delta's format.
  /// Returns the serialized payload (without framing).
  std::vector<uint8_t> serializeBitmap(const roaring::Roaring& bitmap);

  /// Compute CRC-32 checksum of data.
  /// Uses big-endian byte order as per Delta spec.
  uint32_t computeChecksum(const std::vector<uint8_t>& data);

  /// Write framed format: size + payload + CRC.
  /// This is the format used for each DV in a file.
  void writeFramedFormat(
      WriteFile* file,
      const std::vector<uint8_t>& payload);

  /// Generate a UUID-based path for a new DV file.
  /// Format: <tablePath>/<randomPrefix>/deletion_vector_<uuid>.bin
  /// Uses Z85 encoding for the UUID as per Delta spec.
  std::string generateUuidPath(const std::string& tablePath);

  /// Encode data as base64 for inline storage.
  std::string encodeBase64(const std::vector<uint8_t>& data);

  /// Write big-endian int16
  void writeInt16BE(WriteFile* file, uint16_t value);

  /// Write big-endian int32
  void writeInt32BE(WriteFile* file, uint32_t value);

  std::shared_ptr<filesystems::FileSystem> fileSystem_;
  memory::MemoryPool* pool_;
};

} // namespace gluten::delta

// Made with Bob
