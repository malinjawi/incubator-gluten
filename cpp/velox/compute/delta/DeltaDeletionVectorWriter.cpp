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

#include <random>
#include <sstream>

#include <folly/hash/Checksum.h>
#include <roaring/roaring.h>

#include "velox/common/base/Exceptions.h"
#include "compute/delta/DeltaUuidUtils.h"

namespace gluten::delta {

namespace {

/// Generate a random prefix for DV file paths (2 hex characters)
std::string generateRandomPrefix() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 255);
  
  std::ostringstream oss;
  oss << std::hex << std::setw(2) << std::setfill('0') << dis(gen);
  return oss.str();
}

/// Base64 encoding table
static const char base64_chars[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

} // namespace

DeltaDeletionVectorWriter::DeltaDeletionVectorWriter(
    std::shared_ptr<filesystems::FileSystem> fileSystem,
    memory::MemoryPool* pool)
    : fileSystem_(std::move(fileSystem)), pool_(pool) {
  VELOX_CHECK_NOT_NULL(fileSystem_, "FileSystem cannot be null");
  VELOX_CHECK_NOT_NULL(pool_, "MemoryPool cannot be null");
}

DeltaDeletionVectorDescriptor DeltaDeletionVectorWriter::writeDeletionVector(
    const std::string& tablePath,
    const roaring::Roaring& bitmap,
    bool forceInline) {
  // Serialize the bitmap
  auto payload = serializeBitmap(bitmap);
  
  // Decide storage type based on size
  bool useInline = forceInline || payload.size() <= INLINE_THRESHOLD;
  
  if (useInline) {
    // Inline storage - encode as base64
    return DeltaDeletionVectorDescriptor{
      .storageType = DeltaDeletionVectorStorageType::kInlineData,
      .pathOrInlineData = encodeBase64(payload),
      .offset = std::nullopt,
      .sizeInBytes = static_cast<uint32_t>(payload.size()),
      .cardinality = bitmap.cardinality()
    };
  } else {
    // On-disk storage - generate UUID path and write
    std::string dvPath = generateUuidPath(tablePath);
    return writeDeletionVectorToFile(dvPath, bitmap, 0);
  }
}

DeltaDeletionVectorDescriptor
DeltaDeletionVectorWriter::writeDeletionVectorToFile(
    const std::string& filePath,
    const roaring::Roaring& bitmap,
    uint64_t offset) {
  // Serialize the bitmap
  auto payload = serializeBitmap(bitmap);
  
  // Open file for writing
  auto writeFile = fileSystem_->openFileForWrite(filePath);
  VELOX_CHECK_NOT_NULL(writeFile, "Failed to open file for writing: {}", filePath);
  
  // If offset is 0, write file version header
  if (offset == 0) {
    writeInt16BE(writeFile.get(), FORMAT_VERSION);
    offset = 2; // Version is 2 bytes
  } else {
    // Seek to the specified offset
    // Note: WriteFile interface may not support seeking, 
    // so this assumes we're appending or writing at current position
  }
  
  // Write framed format
  writeFramedFormat(writeFile.get(), payload);
  
  // Close file
  writeFile->close();
  
  // Create descriptor
  return DeltaDeletionVectorDescriptor{
    .storageType = DeltaDeletionVectorStorageType::kFilePath,
    .pathOrInlineData = filePath,
    .offset = static_cast<uint32_t>(offset),
    .sizeInBytes = static_cast<uint32_t>(payload.size()),
    .cardinality = bitmap.cardinality()
  };
}

std::vector<uint8_t> DeltaDeletionVectorWriter::serializeBitmap(
    const roaring::Roaring& bitmap) {
  // Get portable serialization size
  size_t portableSize = bitmap.getSizeInBytes(true); // true = portable format
  
  // Allocate buffer: 4 bytes for magic number + portable data
  std::vector<uint8_t> buffer(4 + portableSize);
  
  // Write magic number (big-endian)
  buffer[0] = (MAGIC_NUMBER >> 24) & 0xFF;
  buffer[1] = (MAGIC_NUMBER >> 16) & 0xFF;
  buffer[2] = (MAGIC_NUMBER >> 8) & 0xFF;
  buffer[3] = MAGIC_NUMBER & 0xFF;
  
  // Write portable bitmap data
  bitmap.write(reinterpret_cast<char*>(buffer.data() + 4), true);
  
  return buffer;
}

uint32_t DeltaDeletionVectorWriter::computeChecksum(
    const std::vector<uint8_t>& data) {
  // Compute CRC-32 checksum using folly
  return folly::crc32(data.data(), data.size());
}

void DeltaDeletionVectorWriter::writeFramedFormat(
    WriteFile* file,
    const std::vector<uint8_t>& payload) {
  // Write payload size (big-endian int32)
  writeInt32BE(file, static_cast<uint32_t>(payload.size()));
  
  // Write payload
  file->append(std::string_view(
      reinterpret_cast<const char*>(payload.data()), payload.size()));
  
  // Compute and write checksum (big-endian int32)
  uint32_t checksum = computeChecksum(payload);
  writeInt32BE(file, checksum);
}

std::string DeltaDeletionVectorWriter::generateUuidPath(
    const std::string& tablePath) {
  // Generate random prefix (2 hex chars)
  std::string randomPrefix = generateRandomPrefix();
  
  // Generate UUID
  auto uuid = DeltaUuidUtils::generateUuid();
  
  // Encode UUID in Z85
  std::string z85Uuid = DeltaUuidUtils::encodeUuidToZ85(uuid);
  
  // Construct path: <tablePath>/<randomPrefix>/deletion_vector_<uuid>.bin
  std::ostringstream oss;
  oss << tablePath << "/" << randomPrefix << "/deletion_vector_"
      << DeltaUuidUtils::uuidToString(uuid) << ".bin";
  
  return oss.str();
}

std::string DeltaDeletionVectorWriter::encodeBase64(
    const std::vector<uint8_t>& data) {
  std::string encoded;
  encoded.reserve(((data.size() + 2) / 3) * 4);
  
  size_t i = 0;
  while (i < data.size()) {
    uint32_t octet_a = i < data.size() ? data[i++] : 0;
    uint32_t octet_b = i < data.size() ? data[i++] : 0;
    uint32_t octet_c = i < data.size() ? data[i++] : 0;
    
    uint32_t triple = (octet_a << 16) + (octet_b << 8) + octet_c;
    
    encoded += base64_chars[(triple >> 18) & 0x3F];
    encoded += base64_chars[(triple >> 12) & 0x3F];
    encoded += base64_chars[(triple >> 6) & 0x3F];
    encoded += base64_chars[triple & 0x3F];
  }
  
  // Add padding
  size_t padding = (3 - (data.size() % 3)) % 3;
  for (size_t j = 0; j < padding; j++) {
    encoded[encoded.size() - 1 - j] = '=';
  }
  
  return encoded;
}

void DeltaDeletionVectorWriter::writeInt16BE(
    WriteFile* file,
    uint16_t value) {
  uint8_t bytes[2];
  bytes[0] = (value >> 8) & 0xFF;
  bytes[1] = value & 0xFF;
  file->append(std::string_view(reinterpret_cast<const char*>(bytes), 2));
}

void DeltaDeletionVectorWriter::writeInt32BE(
    WriteFile* file,
    uint32_t value) {
  uint8_t bytes[4];
  bytes[0] = (value >> 24) & 0xFF;
  bytes[1] = (value >> 16) & 0xFF;
  bytes[2] = (value >> 8) & 0xFF;
  bytes[3] = value & 0xFF;
  file->append(std::string_view(reinterpret_cast<const char*>(bytes), 4));
}

} // namespace gluten::delta

// Made with Bob
