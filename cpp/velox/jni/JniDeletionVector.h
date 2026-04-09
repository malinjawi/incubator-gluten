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

#pragma once

#include <jni.h>

namespace gluten {

/**
 * Initialize JNI for deletion vectors.
 * Called once during JVM startup.
 */
void initVeloxJniDeletionVector(JNIEnv* env);

/**
 * Finalize JNI for deletion vectors.
 * Called once during JVM shutdown.
 */
void finalizeVeloxJniDeletionVector(JNIEnv* env);

} // namespace gluten

#ifdef __cplusplus
extern "C" {
#endif

/**
 * JNI methods for DV Builder.
 * These are called from Scala via GlutenDeletionVectorJni.
 */

/**
 * Create a new DV builder.
 * @return Pointer to DeltaDeletionVectorBuilder as jlong
 */
JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeCreateBuilder(
    JNIEnv* env,
    jclass clazz);

/**
 * Add a single deleted row to the builder.
 * @param builderPtr Pointer to DeltaDeletionVectorBuilder
 * @param rowIndex Row index to mark as deleted
 */
JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeAddRow(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr,
    jlong rowIndex);

/**
 * Add a range of deleted rows to the builder (inclusive).
 * @param builderPtr Pointer to DeltaDeletionVectorBuilder
 * @param startRow Start row index (inclusive)
 * @param endRow End row index (inclusive)
 */
JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeAddRange(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr,
    jlong startRow,
    jlong endRow);

/**
 * Merge another DV into this builder.
 * @param builderPtr Pointer to DeltaDeletionVectorBuilder
 * @param otherDVData Serialized DV data to merge
 */
JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeMerge(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr,
    jbyteArray otherDVData);

/**
 * Build the deletion vector and return serialized data.
 * @param builderPtr Pointer to DeltaDeletionVectorBuilder
 * @param shouldInline Whether to use inline format
 * @return Serialized DV data as byte array
 */
JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeBuild(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr,
    jboolean shouldInline);

/**
 * Get the cardinality (number of deleted rows) from the builder.
 * @param builderPtr Pointer to DeltaDeletionVectorBuilder
 * @return Number of deleted rows
 */
JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeGetCardinality(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr);

/**
 * Destroy the DV builder and free memory.
 * @param builderPtr Pointer to DeltaDeletionVectorBuilder
 */
JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeDestroyBuilder(
    JNIEnv* env,
    jclass clazz,
    jlong builderPtr);

/**
 * JNI methods for DV Writer.
 */

/**
 * Serialize a roaring bitmap to Delta format.
 * @param bitmapData Raw roaring bitmap data
 * @return Serialized DV data with framing and checksum
 */
JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeSerialize(
    JNIEnv* env,
    jclass clazz,
    jbyteArray bitmapData);

/**
 * Write serialized DV data to a file.
 * @param serializedData Serialized DV data
 * @param path File path to write to
 */
JNIEXPORT void JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeWriteToFile(
    JNIEnv* env,
    jclass clazz,
    jbyteArray serializedData,
    jstring path);

/**
 * Read and deserialize a DV from a file.
 * @param path File path to read from
 * @param offset Offset in file (bytes)
 * @param sizeInBytes Size of DV data (bytes)
 * @return Deserialized roaring bitmap data
 */
JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeReadFromFile(
    JNIEnv* env,
    jclass clazz,
    jstring path,
    jint offset,
    jint sizeInBytes);

/**
 * Decode inline DV data.
 * @param inlineData Base85-encoded inline DV data
 * @return Deserialized roaring bitmap data
 */
JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_delta_GlutenDeletionVectorJni_nativeDecodeInline(
    JNIEnv* env,
    jclass clazz,
    jstring inlineData);

#ifdef __cplusplus
}
#endif

// Made with Bob
