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

#include "JniDeletionVector.h"

#include <fstream>
#include <memory>

#include "compute/delta/DeltaDeletionVectorBuilder.h"
#include "compute/delta/DeltaDeletionVectorReader.h"
#include "compute/delta/DeltaDeletionVectorWriter.h"
#include "jni/JniCommon.h"

using namespace gluten::delta;

namespace gluten {

void initVeloxJniDeletionVector(JNIEnv* env) {
  // Initialize any global state if needed
  // Currently no global state required
}

void finalizeVeloxJniDeletionVector(JNIEnv* env) {
  // Cleanup any global state if needed
  // Currently no global state to clean up
}

} // namespace gluten

#ifdef __cplusplus
extern "C" {
#endif

// Helper function to throw Java exceptions
static void throwJavaException(JNIEnv* env, const char* message) {
  jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
  if (exceptionClass != nullptr) {
    env->ThrowNew(exceptionClass, message);
  }
}

// Helper function to convert jbyteArray to std::string
static std::string jbyteArrayToString(JNIEnv* env, jbyteArray array) {
  jsize len = env->GetArrayLength(array);
  jbyte* bytes = env->GetByteArrayElements(array, nullptr);
  std::string result(reinterpret_cast<char*>(bytes), len);
  env->ReleaseByteArrayElements(array, bytes, JNI_ABORT);
  return result;
}

// Helper function to convert std::string to jbyteArray
static jbyteArray stringToJbyteArray(JNIEnv* env, const std::string& str) {
  jbyteArray result = env->NewByteArray(str.size());
  if (result == nullptr) {
    throwJavaException(env, "Failed to allocate byte array");
    return nullptr;
  }
  env->SetByteArrayRegion(
      result, 0, str.size(),
      reinterpret_cast<const jbyte*>(str.data()));
  return result;
}

// ============================================================================
// DV Builder JNI Methods
// ============================================================================

JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeCreateBuilder(
    JNIEnv* env,
    jclass clazz) {
  try {
    auto builder = std::make_unique<DeltaDeletionVectorBuilder>();
    return reinterpret_cast<jlong>(builder.release());
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
    return 0;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeAddRow(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr,
    jlong rowIndex) {
  try {
    if (builderPtr == 0) {
      throwJavaException(env, "Invalid builder pointer");
      return;
    }
    auto* builder = reinterpret_cast<DeltaDeletionVectorBuilder*>(builderPtr);
    builder->addDeletedRow(static_cast<uint64_t>(rowIndex));
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
  }
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeAddRange(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr,
    jlong startRow,
    jlong endRow) {
  try {
    if (builderPtr == 0) {
      throwJavaException(env, "Invalid builder pointer");
      return;
    }
    auto* builder = reinterpret_cast<DeltaDeletionVectorBuilder*>(builderPtr);
    builder->addDeletedRange(
        static_cast<uint64_t>(startRow),
        static_cast<uint64_t>(endRow));
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
  }
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeMerge(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr,
    jbyteArray otherDVData) {
  try {
    if (builderPtr == 0) {
      throwJavaException(env, "Invalid builder pointer");
      return;
    }
    if (otherDVData == nullptr) {
      throwJavaException(env, "Null DV data");
      return;
    }
    
    auto* builder = reinterpret_cast<DeltaDeletionVectorBuilder*>(builderPtr);
    std::string dvData = jbyteArrayToString(env, otherDVData);
    
    // Deserialize the other DV and merge
    DeltaDeletionVectorReader reader;
    auto bitmap = reader.readDeletionVector(dvData);
    builder->mergeWith(bitmap);
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
  }
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeBuild(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr,
    jboolean shouldInline) {
  try {
    if (builderPtr == 0) {
      throwJavaException(env, "Invalid builder pointer");
      return nullptr;
    }
    
    auto* builder = reinterpret_cast<DeltaDeletionVectorBuilder*>(builderPtr);
    auto result = builder->build(shouldInline == JNI_TRUE);
    
    return stringToJbyteArray(env, result.serializedData);
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
    return nullptr;
  }
}

JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeGetCardinality(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr) {
  try {
    if (builderPtr == 0) {
      throwJavaException(env, "Invalid builder pointer");
      return 0;
    }
    
    auto* builder = reinterpret_cast<DeltaDeletionVectorBuilder*>(builderPtr);
    return static_cast<jlong>(builder->cardinality());
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
    return 0;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeDestroyBuilder(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr) {
  try {
    if (builderPtr == 0) {
      return; // Already destroyed or never created
    }
    
    auto* builder = reinterpret_cast<DeltaDeletionVectorBuilder*>(builderPtr);
    delete builder;
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
  }
}

// ============================================================================
// DV Writer JNI Methods
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeSerialize(
    JNIEnv* env,
    jclass clazz,
    jbyteArray bitmapData) {
  try {
    if (bitmapData == nullptr) {
      throwJavaException(env, "Null bitmap data");
      return nullptr;
    }
    
    std::string bitmap = jbyteArrayToString(env, bitmapData);
    
    DeltaDeletionVectorWriter writer;
    std::string serialized = writer.writeDeletionVector(bitmap);
    
    return stringToJbyteArray(env, serialized);
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
    return nullptr;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeWriteToFile(
    JNIEnv* env,
    jclass clazz,
    jbyteArray serializedData,
    jstring path) {
  try {
    if (serializedData == nullptr) {
      throwJavaException(env, "Null serialized data");
      return;
    }
    if (path == nullptr) {
      throwJavaException(env, "Null path");
      return;
    }
    
    std::string data = jbyteArrayToString(env, serializedData);
    std::string pathStr = jStringToCString(env, path);
    
    DeltaDeletionVectorWriter writer;
    writer.writeDeletionVectorToFile(data, pathStr);
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
  }
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeReadFromFile(
    JNIEnv* env,
    jclass clazz,
    jstring path,
    jint offset,
    jint sizeInBytes) {
  try {
    if (path == nullptr) {
      throwJavaException(env, "Null path");
      return nullptr;
    }
    
    std::string pathStr = jStringToCString(env, path);
    
    // Read the file
    std::ifstream file(pathStr, std::ios::binary);
    if (!file.is_open()) {
      std::string error = "Failed to open file: " + pathStr;
      throwJavaException(env, error.c_str());
      return nullptr;
    }
    
    // Seek to offset
    file.seekg(offset);
    if (!file.good()) {
      throwJavaException(env, "Failed to seek to offset");
      return nullptr;
    }
    
    // Read data
    std::string data(sizeInBytes, '\0');
    file.read(&data[0], sizeInBytes);
    if (!file.good() && !file.eof()) {
      throwJavaException(env, "Failed to read data");
      return nullptr;
    }
    
    file.close();
    
    // Deserialize and return bitmap
    DeltaDeletionVectorReader reader;
    auto bitmap = reader.readDeletionVector(data);
    
    return stringToJbyteArray(env, bitmap);
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
    return nullptr;
  }
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeDecodeInline(
    JNIEnv* env,
    jclass clazz,
    jstring inlineData) {
  try {
    if (inlineData == nullptr) {
      throwJavaException(env, "Null inline data");
      return nullptr;
    }
    
    std::string encoded = jStringToCString(env, inlineData);
    
    // Decode from Base85 (Z85)
    // Note: This requires a Base85 decoder implementation
    // For now, throw an error indicating this needs implementation
    throwJavaException(env, "Base85 decoding not yet implemented");
    return nullptr;
    
    // TODO: Implement Base85 decoding
    // std::string decoded = decodeBase85(encoded);
    // DeltaDeletionVectorReader reader;
    // auto bitmap = reader.readDeletionVector(decoded);
    // return stringToJbyteArray(env, bitmap);
  } catch (const std::exception& e) {
    throwJavaException(env, e.what());
    return nullptr;
  }
}

#ifdef __cplusplus
}
#endif

// Made with Bob
