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
package org.apache.gluten.execution.streaming.state

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.conf.Configuration

import java.io.{DataInputStream, DataOutputStream}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.Arrays

import scala.collection.mutable

/**
 * Test-scoped StateStoreProvider used to prove Spark 4.1's StateStore contract before the backing
 * implementation is replaced with a JNI/C++ Velox-native store.
 */
class ExperimentalVeloxNativeStateStoreProvider extends StateStoreProvider {
  private var storeId: StateStoreId = _
  private var keyFields: Int = _
  private var valueFields: Int = _
  private var checkpointDir: Path = _
  private var closed: Boolean = false

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean,
      stateSchemaProvider: Option[StateSchemaProvider]): Unit = {
    if (useColumnFamilies) {
      throw unsupported("column families")
    }
    if (useMultipleValuesPerKey) {
      throw unsupported("multiple values per key")
    }

    storeId = stateStoreId
    keyFields = keySchema.length
    valueFields = valueSchema.length
    checkpointDir =
      localPath(stateStoreId.storeCheckpointLocation()).resolve("gluten-native-state-poc")
    Files.createDirectories(checkpointDir)
  }

  override def stateStoreId: StateStoreId = storeId

  override def close(): Unit = {
    closed = true
  }

  override def getStore(version: Long, stateStoreCkptId: Option[String]): StateStore = {
    if (closed) {
      throw new IllegalStateException(s"StateStoreProvider $storeId has been closed")
    }
    new ExperimentalVeloxNativeStateStore(this, version, readSnapshot(version))
  }

  private[state] def commit(version: Long, state: mutable.LinkedHashMap[ByteKey, Array[Byte]])
      : Long = {
    val newVersion = version + 1
    writeSnapshot(newVersion, state)
    newVersion
  }

  private[state] def toKey(row: UnsafeRow): ByteKey = new ByteKey(row.getBytes.clone())

  private[state] def toRow(bytes: Array[Byte], fields: Int): UnsafeRow = {
    val copied = bytes.clone()
    val row = new UnsafeRow(fields)
    row.pointTo(copied, copied.length)
    row
  }

  private[state] def keyRow(bytes: Array[Byte]): UnsafeRow = toRow(bytes, keyFields)

  private[state] def valueRow(bytes: Array[Byte]): UnsafeRow = toRow(bytes, valueFields)

  private def snapshotPath(version: Long): Path = {
    checkpointDir.resolve(f"$version%020d.snapshot")
  }

  private def readSnapshot(version: Long): mutable.LinkedHashMap[ByteKey, Array[Byte]] = {
    val result = mutable.LinkedHashMap.empty[ByteKey, Array[Byte]]
    if (version == 0 && !Files.exists(snapshotPath(version))) {
      return result
    }

    val path = snapshotPath(version)
    if (!Files.exists(path)) {
      throw new IllegalStateException(
        s"Missing experimental native state snapshot for version $version")
    }

    val in = new DataInputStream(Files.newInputStream(path))
    try {
      val count = in.readInt()
      var i = 0
      while (i < count) {
        val key = new Array[Byte](in.readInt())
        in.readFully(key)
        val value = new Array[Byte](in.readInt())
        in.readFully(value)
        result.put(new ByteKey(key), value)
        i += 1
      }
      result
    } finally {
      in.close()
    }
  }

  private def writeSnapshot(
      version: Long,
      state: mutable.LinkedHashMap[ByteKey, Array[Byte]]): Unit = {
    Files.createDirectories(checkpointDir)
    val finalPath = snapshotPath(version)
    val tmpPath = checkpointDir.resolve(s"${finalPath.getFileName}.tmp")
    val out = new DataOutputStream(Files.newOutputStream(tmpPath))
    try {
      out.writeInt(state.size)
      state.foreach {
        case (key, value) =>
          out.writeInt(key.bytes.length)
          out.write(key.bytes)
          out.writeInt(value.length)
          out.write(value)
      }
    } finally {
      out.close()
    }
    Files.move(
      tmpPath,
      finalPath,
      StandardCopyOption.ATOMIC_MOVE,
      StandardCopyOption.REPLACE_EXISTING)
  }

  private def localPath(path: org.apache.hadoop.fs.Path): Path = {
    val uri = path.toUri
    if (uri.getScheme == null) {
      Paths.get(uri.getPath)
    } else {
      Paths.get(uri)
    }
  }

  private[state] def requireDefaultColumnFamily(colFamilyName: String): Unit = {
    if (colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME) {
      throw unsupported(s"column family '$colFamilyName'")
    }
  }

  private[state] def unsupported(feature: String): UnsupportedOperationException = {
    new UnsupportedOperationException(
      s"Experimental Velox native StateStore POC supports only default single-value state; " +
        s"unsupported: $feature")
  }
}

final private[state] class ExperimentalVeloxNativeStateStore(
    provider: ExperimentalVeloxNativeStateStoreProvider,
    initialVersion: Long,
    initialState: mutable.LinkedHashMap[ByteKey, Array[Byte]])
  extends StateStore {
  private val state = mutable.LinkedHashMap.empty[ByteKey, Array[Byte]]
  initialState.foreach { case (key, value) => state.put(key.copy(), value.clone()) }

  private var committed = false
  private var closed = false
  private var committedVersion = initialVersion

  override def id: StateStoreId = provider.stateStoreId

  override def version: Long = initialVersion

  override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
    provider.requireDefaultColumnFamily(colFamilyName)
    state.get(provider.toKey(key)).map(provider.valueRow).orNull
  }

  override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
    throw provider.unsupported("valuesIterator")
  }

  override def prefixScan(
      prefixKey: UnsafeRow,
      colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
    throw provider.unsupported("prefixScan")
  }

  override def iterator(colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
    provider.requireDefaultColumnFamily(colFamilyName)
    val snapshot = state.toSeq.map {
      case (key, value) =>
        new UnsafeRowPair(provider.keyRow(key.bytes), provider.valueRow(value))
    }
    new StateStoreIterator(snapshot.iterator)
  }

  override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
    provider.requireDefaultColumnFamily(colFamilyName)
    false
  }

  override def createColFamilyIfAbsent(
      colFamilyName: String,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useMultipleValuesPerKey: Boolean,
      isInternal: Boolean): Unit = {
    provider.requireDefaultColumnFamily(colFamilyName)
    if (useMultipleValuesPerKey) {
      throw provider.unsupported("multiple values per key")
    }
  }

  override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
    requireWritable()
    provider.requireDefaultColumnFamily(colFamilyName)
    state.put(provider.toKey(key), value.getBytes.clone())
  }

  override def putList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
    throw provider.unsupported("putList")
  }

  override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
    requireWritable()
    provider.requireDefaultColumnFamily(colFamilyName)
    state.remove(provider.toKey(key))
  }

  override def merge(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
    throw provider.unsupported("merge")
  }

  override def mergeList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
    throw provider.unsupported("mergeList")
  }

  override def commit(): Long = {
    requireWritable()
    committedVersion = provider.commit(initialVersion, state)
    committed = true
    closed = true
    committedVersion
  }

  override def abort(): Unit = {
    closed = true
  }

  override def release(): Unit = {
    closed = true
  }

  override def metrics: StateStoreMetrics = {
    val memoryBytes = state.map { case (key, value) => key.bytes.length + value.length }.sum
    StateStoreMetrics(state.size, memoryBytes, Map.empty)
  }

  override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
    StateStoreCheckpointInfo(id.partitionId, committedVersion, None, None)
  }

  override def hasCommitted: Boolean = committed

  private def requireWritable(): Unit = {
    if (closed) {
      throw new IllegalStateException(s"State store $id version $initialVersion is already closed")
    }
  }
}

final private[state] class ByteKey(val bytes: Array[Byte]) {
  def copy(): ByteKey = new ByteKey(bytes.clone())

  override def equals(other: Any): Boolean = other match {
    case that: ByteKey => Arrays.equals(bytes, that.bytes)
    case _ => false
  }

  override def hashCode(): Int = Arrays.hashCode(bytes)
}
