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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.ByteArrayOutputStream
import java.util.Properties
import java.util.UUID

import scala.collection.{mutable, AbstractIterator}

private object VeloxNativeStateStoreProvider {
  private val MetadataFileName = "_metadata.properties"
  private val MetadataFormatVersion = "1"
  private val SnapshotFilePattern = raw"(\d{20})\.snapshot".r
  private val DeltaFilePattern = raw"(\d{20})\.delta".r

  private val MetadataKeys = Seq(
    "metadataFormatVersion",
    "providerClass",
    "checkpointFormatVersion",
    "keySchemaJson",
    "valueSchemaJson",
    "keyStateEncoderSpecJson",
    "useColumnFamilies",
    "useMultipleValuesPerKey"
  )

  private val StreamStreamJoinStoreNames = Set(
    "left-keyToNumValues",
    "left-keyWithIndexToValue",
    "right-keyToNumValues",
    "right-keyWithIndexToValue")

  private[state] val NativeNumKeysMetric = StateStoreCustomSumMetric(
    "veloxNativeStateStoreNumKeys",
    "number of keys reported by the Velox native StateStore")
  private[state] val NativeMemoryBytesMetric = StateStoreCustomSizeMetric(
    "veloxNativeStateStoreMemoryBytes",
    "memory used by the Velox native StateStore")

  private[state] val CustomMetrics = Seq(NativeNumKeysMetric, NativeMemoryBytesMetric)
}

class VeloxNativeStateStoreProvider extends StateStoreProvider {
  import VeloxNativeStateStoreProvider._

  private var storeId: StateStoreId = _
  private var keyFields: Int = _
  private var valueFields: Int = _
  private var keySchemaJson: String = _
  private var valueSchemaJson: String = _
  private var keyStateEncoderSpecJson: String = _
  private var checkpointFormatVersion: Int = _
  private var stateStoreConf: StateStoreConf = _
  private var checkpointDir: Path = _
  private var hadoopConf: Configuration = _
  private var closed: Boolean = false
  // When true, commit() writes an O(changed-keys) <version>.delta every batch and a full
  // <version>.snapshot only every minDeltasForSnapshot versions; getStore() reconstructs a
  // version by loading the latest snapshot <= version and replaying the intervening deltas.
  // When false, the original full-snapshot-per-version lifecycle is preserved exactly.
  private var deltaCheckpointEnabled: Boolean = false
  // Write a full snapshot at least this often (in versions) when delta checkpointing is on, so
  // the cold-start replay chain stays bounded. Mirrors Spark's stateStore.minDeltasForSnapshot.
  private var minDeltasForSnapshot: Int = 10
  private val openStores = mutable.LinkedHashSet.empty[VeloxNativeStateStore]

  // Stage 1 resident-map reuse: a provider instance is per (operator, partition)
  // on an executor and drives a strictly increasing version chain. Keeping the
  // last committed native map resident lets steady-state batches skip the
  // O(total-state) snapshot read + deserialize on getStore(). residentHandle is
  // 0 when nothing is cached; residentInUse guards against handing the same
  // resident handle to two concurrent stores (e.g. a same-version commit retry).
  private val residentLock = new Object
  private var residentHandle: Long = 0L
  private var residentVersion: Long = -1L
  private var residentInUse: Boolean = false
  private var residentReuseEnabled: Boolean = false

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
    if (StreamStreamJoinStoreNames.contains(stateStoreId.storeName)) {
      throw unsupported(s"stream-stream join state store '${stateStoreId.storeName}'")
    }
    if (!GlutenConfig.get.enableNativeStreamingStateStore) {
      throw new IllegalStateException(
        s"${GlutenConfig.NATIVE_STREAMING_STATE_STORE_ENABLED.key}=true is required to use " +
          s"${classOf[VeloxNativeStateStoreProvider].getName}")
    }
    if (!VeloxConfig.get.enableVeloxNativeStreamingStateStore) {
      throw new IllegalStateException(
        s"${VeloxConfig.VELOX_NATIVE_STREAMING_STATE_STORE_ENABLED.key}=true is required to use " +
          s"${classOf[VeloxNativeStateStoreProvider].getName}")
    }
    if (useColumnFamilies) {
      throw unsupported("column families")
    }
    if (useMultipleValuesPerKey) {
      throw unsupported("multiple values per key")
    }
    keyStateEncoderSpec match {
      case _: NoPrefixKeyStateEncoderSpec =>
      case other => throw unsupported(s"key state encoder ${other.getClass.getSimpleName}")
    }

    keyFields = keySchema.length
    valueFields = valueSchema.length
    keySchemaJson = keySchema.json
    valueSchemaJson = valueSchema.json
    keyStateEncoderSpecJson = keyStateEncoderSpec.json
    checkpointFormatVersion = VeloxConfig.get.nativeStreamingStateStoreCheckpointFormatVersion
    residentReuseEnabled = VeloxConfig.get.nativeStreamingStateStoreResidentReuseEnabled
    deltaCheckpointEnabled = VeloxConfig.get.nativeStreamingStateStoreDeltaCheckpointEnabled
    minDeltasForSnapshot = math.max(1, storeConfs.minDeltasForSnapshot)
    stateStoreConf = storeConfs
    storeId = stateStoreId
    this.hadoopConf = hadoopConf
    checkpointDir = new Path(
      stateStoreId.storeCheckpointLocation(),
      s"gluten-native/v$checkpointFormatVersion")
    fileSystem.mkdirs(checkpointDir)
    validateOrWriteMetadata()
  }

  override def stateStoreId: StateStoreId = storeId

  override def close(): Unit = {
    closed = true
    val stores = openStores.synchronized {
      val stores = openStores.toSeq
      openStores.clear()
      stores
    }
    stores.foreach(_.release())
    releaseResident()
  }

  // Frees the cached resident native map, if any. Called on provider close so
  // the resident handle does not outlive the provider instance.
  private def releaseResident(): Unit = {
    val handleToClose = residentLock.synchronized {
      val handle = residentHandle
      residentHandle = 0L
      residentVersion = -1L
      residentInUse = false
      handle
    }
    if (handleToClose != 0L) {
      VeloxNativeStateStoreJniWrapper.nativeCloseStore(handleToClose)
    }
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = CustomMetrics

  override def getStore(version: Long, stateStoreCkptId: Option[String]): StateStore = {
    if (closed) {
      throw new IllegalStateException(s"StateStoreProvider $storeId has been closed")
    }
    if (version < 0) {
      throw new IllegalArgumentException(
        s"Velox native StateStore requires a non-negative version: $version")
    }

    // Stage 1: reuse the resident map if it already holds exactly this version
    // and is not currently lent out. This avoids re-reading and deserializing
    // the entire snapshot for the common steady-state case where the next batch
    // opens the version we just committed.
    val reused = residentLock.synchronized {
      if (residentReuseEnabled && residentHandle != 0L && !residentInUse &&
        residentVersion == version) {
        residentInUse = true
        Some(residentHandle)
      } else {
        None
      }
    }

    val store = reused match {
      case Some(handle) =>
        new VeloxNativeStateStore(this, handle, version, resident = true)
      case None =>
        val nativeHandle = openNativeStore(version)
        new VeloxNativeStateStore(this, nativeHandle, version, resident = false)
    }

    openStores.synchronized {
      openStores += store
    }
    Option(TaskContext.get()).foreach {
      _.addTaskCompletionListener[Unit](_ => store.release())
    }
    store
  }

  override def doMaintenance(): Unit = {
    if (!closed) {
      if (deltaCheckpointEnabled) {
        cleanupOldDeltasAndSnapshots()
      } else {
        cleanupOldSnapshots()
      }
    }
  }

  private[state] def valueRow(bytes: Array[Byte]): UnsafeRow = {
    val copied = bytes.clone()
    val row = new UnsafeRow(valueFields)
    row.pointTo(copied, copied.length)
    row
  }

  private[state] def keyRow(bytes: Array[Byte]): UnsafeRow = {
    val copied = bytes.clone()
    val row = new UnsafeRow(keyFields)
    row.pointTo(copied, copied.length)
    row
  }

  // Durably checkpoints the just-committed batch. With delta checkpointing off this writes the
  // full snapshot for the new version exactly as before. With delta checkpointing on it writes an
  // O(changed-keys) delta for the new version (from the native dirty journal) and additionally a
  // full snapshot only on snapshot-boundary versions (or when no snapshot exists yet), so the
  // replay chain stays bounded. Returns the new committed version.
  private[state] def commit(version: Long, nativeHandle: Long): Long = {
    val newVersion = version + 1
    if (!deltaCheckpointEnabled) {
      val snapshot = VeloxNativeStateStoreJniWrapper.nativeCommit(nativeHandle)
      writeSnapshot(newVersion, snapshot)
      return newVersion
    }

    val delta = VeloxNativeStateStoreJniWrapper.nativeCommitDelta(nativeHandle)
    writeDelta(newVersion, delta)
    if (needsSnapshotAt(newVersion)) {
      val snapshot = VeloxNativeStateStoreJniWrapper.nativeCommit(nativeHandle)
      writeSnapshot(newVersion, snapshot)
    }
    newVersion
  }

  // A full snapshot is required at a version when it lands on a snapshot boundary, or when no
  // snapshot at or before it exists yet (so a cold reader always has a base to replay deltas onto).
  private def needsSnapshotAt(version: Long): Boolean = {
    if (version % minDeltasForSnapshot == 0) {
      return true
    }
    latestSnapshotVersionAtOrBelow(version).isEmpty
  }

  private[state] def unregisterStore(store: VeloxNativeStateStore): Unit = {
    openStores.synchronized {
      openStores -= store
    }
  }

  private[state] def residentReuseActive: Boolean = residentReuseEnabled

  // Installs the just-committed native map as the resident store at newVersion
  // so the next getStore(newVersion) can reuse it without touching disk. If a
  // stale resident handle was cached for an older version it is closed first.
  // Returns true if the handle was adopted, false if a resident handle already
  // existed (the caller then closes its own handle to avoid a duplicate).
  private[state] def adoptResidentAfterCommit(handle: Long, newVersion: Long): Unit = {
    val staleHandle = residentLock.synchronized {
      // Only reclaim a previously cached resident handle that is not currently
      // lent out to another store; an in-use handle frees itself on its close.
      if (residentHandle != 0L && residentHandle != handle && !residentInUse) {
        val stale = residentHandle
        residentHandle = 0L
        stale
      } else {
        0L
      }
    }
    if (staleHandle != 0L) {
      VeloxNativeStateStoreJniWrapper.nativeCloseStore(staleHandle)
    }
    residentLock.synchronized {
      residentHandle = handle
      residentVersion = newVersion
      residentInUse = false
    }
  }

  // Returns a reused resident handle to the provider after an abort/release that
  // did not commit. The wrapper has already rolled the native map back to the
  // committed version, so the handle stays resident and merely becomes free.
  private[state] def returnResident(handle: Long): Unit = {
    residentLock.synchronized {
      if (residentHandle == handle) {
        residentInUse = false
      }
    }
  }

  // Drops the resident handle if it matches the given handle, closing it. Used
  // when a reused store cannot be safely returned (e.g. a commit failed midway)
  // so a corrupt resident map is never reused.
  private[state] def dropResident(handle: Long): Unit = {
    val handleToClose = residentLock.synchronized {
      if (residentHandle == handle) {
        residentHandle = 0L
        residentVersion = -1L
        residentInUse = false
        handle
      } else {
        0L
      }
    }
    if (handleToClose != 0L) {
      VeloxNativeStateStoreJniWrapper.nativeCloseStore(handleToClose)
    }
  }

  private[state] def requireDefaultColumnFamily(colFamilyName: String): Unit = {
    if (colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME) {
      throw unsupported(s"column family '$colFamilyName'")
    }
  }

  private[state] def unsupported(feature: String): UnsupportedOperationException = {
    new UnsupportedOperationException(
      s"Velox native StateStore supports only default single-value state; unsupported: $feature")
  }

  private def fileSystem: FileSystem = checkpointDir.getFileSystem(hadoopConf)

  private def snapshotPath(version: Long): Path = {
    new Path(checkpointDir, f"$version%020d.snapshot")
  }

  private def snapshotFiles(): Seq[(Long, Path)] = {
    fileSystem.listStatus(checkpointDir).flatMap {
      status =>
        status.getPath.getName match {
          case SnapshotFilePattern(version) => Some(version.toLong -> status.getPath)
          case _ => None
        }
    }.toSeq
  }

  private def deltaPath(version: Long): Path = {
    new Path(checkpointDir, f"$version%020d.delta")
  }

  private def deltaFiles(): Seq[(Long, Path)] = {
    fileSystem.listStatus(checkpointDir).flatMap {
      status =>
        status.getPath.getName match {
          case DeltaFilePattern(version) => Some(version.toLong -> status.getPath)
          case _ => None
        }
    }.toSeq
  }

  private def latestSnapshotVersionAtOrBelow(version: Long): Option[Long] = {
    val candidates = snapshotFiles().map(_._1).filter(_ <= version)
    if (candidates.isEmpty) None else Some(candidates.max)
  }

  // Opens a native store for the requested version. With delta checkpointing off this is the
  // original "read the version's full snapshot and deserialize it" path. With delta checkpointing
  // on it loads the latest snapshot at or before the version (version 0 may have none) and replays
  // each <snapshot+1..version>.delta onto the resident map, then clears the journal so the next
  // batch's commit only serializes its own mutations.
  private def openNativeStore(version: Long): Long = {
    if (!deltaCheckpointEnabled) {
      val snapshot = readSnapshot(version)
      return VeloxNativeStateStoreJniWrapper.nativeOpenStore(version, snapshot)
    }

    val baseVersion = latestSnapshotVersionAtOrBelow(version)
    if (version == 0 && baseVersion.isEmpty) {
      // A fresh stream: open an empty store at version 0 with no deltas to replay.
      return VeloxNativeStateStoreJniWrapper.nativeOpenStore(0, null)
    }
    val snapshotVersion = baseVersion.getOrElse(
      throw new IllegalStateException(
        s"Missing Velox native state snapshot to reconstruct version $version"))
    val snapshot = readSnapshot(snapshotVersion)
    val handle = VeloxNativeStateStoreJniWrapper.nativeOpenStore(snapshotVersion, snapshot)
    var ok = false
    try {
      var replayVersion = snapshotVersion + 1
      while (replayVersion <= version) {
        val delta = readDelta(replayVersion)
        VeloxNativeStateStoreJniWrapper.nativeApplyDelta(handle, delta)
        replayVersion += 1
      }
      // The replayed mutations are already durable on disk; clear the journal so the first commit
      // after this open serializes only the next batch's changes (and a rollback cannot undo
      // already-committed state).
      VeloxNativeStateStoreJniWrapper.nativeClearDirty(handle)
      VeloxNativeStateStoreJniWrapper.nativeSetVersion(handle, version)
      ok = true
      handle
    } finally {
      if (!ok) {
        VeloxNativeStateStoreJniWrapper.nativeCloseStore(handle)
      }
    }
  }

  private def metadataPath: Path = {
    new Path(checkpointDir, MetadataFileName)
  }

  private def expectedMetadata: Properties = {
    val metadata = new Properties()
    metadata.setProperty("metadataFormatVersion", MetadataFormatVersion)
    metadata.setProperty("providerClass", classOf[VeloxNativeStateStoreProvider].getName)
    metadata.setProperty("checkpointFormatVersion", checkpointFormatVersion.toString)
    metadata.setProperty("keySchemaJson", keySchemaJson)
    metadata.setProperty("valueSchemaJson", valueSchemaJson)
    metadata.setProperty("keyStateEncoderSpecJson", keyStateEncoderSpecJson)
    metadata.setProperty("useColumnFamilies", "false")
    metadata.setProperty("useMultipleValuesPerKey", "false")
    metadata
  }

  private def validateOrWriteMetadata(): Unit = {
    val expected = expectedMetadata
    val fs = fileSystem
    val path = metadataPath
    if (fs.exists(path)) {
      validateMetadata(loadMetadata(path), expected)
    } else {
      writeMetadata(path, expected)
    }
  }

  private def loadMetadata(path: Path): Properties = {
    val metadata = new Properties()
    val in = fileSystem.open(path)
    try {
      metadata.load(in)
      metadata
    } finally {
      in.close()
    }
  }

  private def validateMetadata(existing: Properties, expected: Properties): Unit = {
    val mismatches = MetadataKeys.flatMap {
      key =>
        val existingValue = Option(existing.getProperty(key)).getOrElse("<missing>")
        val expectedValue = expected.getProperty(key)
        if (existingValue == expectedValue) {
          None
        } else {
          Some(s"$key expected ${truncate(expectedValue)} but found ${truncate(existingValue)}")
        }
    }

    if (mismatches.nonEmpty) {
      throw new IllegalStateException(
        s"Velox native StateStore metadata mismatch for $storeId at $metadataPath: " +
          mismatches.mkString("; "))
    }
  }

  private def writeMetadata(finalPath: Path, metadata: Properties): Unit = {
    val fs = fileSystem
    val tmpPath = new Path(checkpointDir, s"${finalPath.getName}.${UUID.randomUUID()}.tmp")
    val out = fs.create(tmpPath, true)
    var committed = false
    try {
      out.write(metadataBytes(metadata))
      out.hflush()
      out.close()
      if (!fs.rename(tmpPath, finalPath)) {
        if (fs.exists(finalPath)) {
          validateMetadata(loadMetadata(finalPath), metadata)
          committed = true
          return
        }
        throw new IllegalStateException(
          s"Failed to rename Velox native state metadata $tmpPath to $finalPath")
      }
      committed = true
    } finally {
      if (!committed && fs.exists(tmpPath)) {
        fs.delete(tmpPath, false)
      }
    }
  }

  private def metadataBytes(metadata: Properties): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    metadata.store(out, "Gluten Velox native StateStore metadata")
    out.toByteArray
  }

  private def truncate(value: String): String = {
    val maxLength = 160
    if (value.length <= maxLength) {
      value
    } else {
      value.take(maxLength) + "..."
    }
  }

  private def readFileBytes(path: Path): Array[Byte] = {
    val in = fileSystem.open(path)
    try {
      val out = new ByteArrayOutputStream()
      val buffer = new Array[Byte](64 * 1024)
      var read = in.read(buffer)
      while (read >= 0) {
        out.write(buffer, 0, read)
        read = in.read(buffer)
      }
      out.toByteArray
    } finally {
      in.close()
    }
  }

  // Atomically writes bytes to finalPath via a temp file + rename, treating a same-version retry
  // that produces identical bytes as a no-op and a retry with divergent bytes as an error. Shared
  // by both the snapshot and delta checkpoint writers so their durability semantics are identical.
  private def writeCheckpointFile(finalPath: Path, contents: Array[Byte], kind: String): Unit = {
    val fs = fileSystem
    if (fs.exists(finalPath)) {
      val existing = readFileBytes(finalPath)
      if (!existing.sameElements(contents)) {
        throw new IllegalStateException(
          s"Velox native state $kind ${finalPath.getName} already exists with different bytes")
      }
      return
    }

    val tmpPath = new Path(checkpointDir, s"${finalPath.getName}.${UUID.randomUUID()}.tmp")
    val out = fs.create(tmpPath, true)
    var committed = false
    try {
      out.write(contents)
      out.hflush()
      out.close()
      if (!fs.rename(tmpPath, finalPath)) {
        if (fs.exists(finalPath) && readFileBytes(finalPath).sameElements(contents)) {
          committed = true
          return
        }
        throw new IllegalStateException(
          s"Failed to rename Velox native state $kind $tmpPath to $finalPath")
      }
      committed = true
    } finally {
      if (!committed && fs.exists(tmpPath)) {
        fs.delete(tmpPath, false)
      }
    }
  }

  private def readSnapshot(version: Long): Array[Byte] = {
    val path = snapshotPath(version)
    val fs = fileSystem
    if (version == 0 && !fs.exists(path)) {
      return null
    }
    if (!fs.exists(path)) {
      throw new IllegalStateException(s"Missing Velox native state snapshot for version $version")
    }
    readFileBytes(path)
  }

  private def writeSnapshot(version: Long, snapshot: Array[Byte]): Unit = {
    writeCheckpointFile(snapshotPath(version), snapshot, "snapshot")
  }

  private def readDelta(version: Long): Array[Byte] = {
    val path = deltaPath(version)
    if (!fileSystem.exists(path)) {
      throw new IllegalStateException(s"Missing Velox native state delta for version $version")
    }
    readFileBytes(path)
  }

  private def writeDelta(version: Long, delta: Array[Byte]): Unit = {
    writeCheckpointFile(deltaPath(version), delta, "delta")
  }

  private def cleanupOldSnapshots(): Unit = {
    val snapshots = snapshotFiles()
    if (snapshots.isEmpty) {
      return
    }

    val latestVersion = snapshots.map(_._1).max
    val versionsToRetain = math.max(1, stateStoreConf.minVersionsToRetain)
    val oldestVersionToRetain = math.max(0L, latestVersion - versionsToRetain + 1L)
    val openVersions = openStores.synchronized {
      openStores.map(_.version).toSet
    }

    val staleSnapshots = snapshots
      .filter {
        case (version, _) =>
          version < oldestVersionToRetain && !openVersions.contains(version)
      }
      .sortBy(_._1)
    val minVersionsToDelete = math.max(1L, stateStoreConf.minVersionsToDelete)
    if (staleSnapshots.size < minVersionsToDelete) {
      return
    }

    staleSnapshots
      .take(math.max(1, stateStoreConf.maxVersionsToDeletePerMaintenance))
      .foreach {
        case (_, path) =>
          if (fileSystem.exists(path)) {
            fileSystem.delete(path, false)
          }
      }
  }

  // Delta-aware retention. Reconstructing a version V replays the deltas after the latest snapshot
  // at or below V onto that snapshot, so we must keep, for the oldest version still retained, its
  // base snapshot and every snapshot/delta at or above it. Everything strictly older than that
  // base (and not pinned by an open store) is prunable.
  private def cleanupOldDeltasAndSnapshots(): Unit = {
    val snapshots = snapshotFiles()
    val deltas = deltaFiles()
    val allFiles = snapshots ++ deltas
    if (allFiles.isEmpty) {
      return
    }

    val latestVersion = allFiles.map(_._1).max
    val versionsToRetain = math.max(1, stateStoreConf.minVersionsToRetain)
    val oldestVersionToRetain = math.max(0L, latestVersion - versionsToRetain + 1L)
    // The base snapshot needed to reconstruct oldestVersionToRetain must survive even if it is
    // itself older than that version; nothing below it can be pruned without breaking replay.
    val baseSnapshotVersion =
      latestSnapshotVersionAtOrBelow(oldestVersionToRetain).getOrElse(0L)
    val openVersions = openStores.synchronized {
      openStores.map(_.version).toSet
    }

    val staleFiles = allFiles
      .filter {
        case (version, _) =>
          version < baseSnapshotVersion && !openVersions.contains(version)
      }
      .sortBy(_._1)
    val minVersionsToDelete = math.max(1L, stateStoreConf.minVersionsToDelete)
    if (staleFiles.size < minVersionsToDelete) {
      return
    }

    staleFiles
      .take(math.max(1, stateStoreConf.maxVersionsToDeletePerMaintenance))
      .foreach {
        case (_, path) =>
          if (fileSystem.exists(path)) {
            fileSystem.delete(path, false)
          }
      }
  }
}

object VeloxNativeStateStoreOperations {
  def incrementCount(store: StateStore, keys: Array[Array[Byte]]): Array[Long] = {
    requireVeloxStore(store).nativeIncrementCount(keys)
  }

  def incrementCount(
      store: StateStore,
      keys: Array[Array[Byte]],
      deltas: Array[Long]): Array[Long] = {
    requireVeloxStore(store).nativeIncrementCount(keys, deltas)
  }

  def incrementCountFromColumnarBatch(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinals: Array[Int]): Array[Long] = {
    requireVeloxStore(store).nativeIncrementCountFromColumnarBatch(batch, keyOrdinals)
  }

  def incrementCountFromColumnarBatchWithKeys(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinals: Array[Int]): (Array[Long], Array[Array[Byte]]) = {
    requireVeloxStore(store).nativeIncrementCountFromColumnarBatchWithKeys(batch, keyOrdinals)
  }

  def incrementCountFromColumnarBatchWithKeyValues(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinals: Array[Int]): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireVeloxStore(store).nativeIncrementCountFromColumnarBatchWithKeyValues(batch, keyOrdinals)
  }

  def incrementCountSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinal: Int,
      valueFieldCount: Int): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireVeloxStore(store).nativeIncrementCountSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
      batch,
      keyOrdinal,
      valueFieldCount)
  }

  def incrementCountSingleInt64KeyFromColumnarBatch(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinal: Int): Array[Long] = {
    requireVeloxStore(store).nativeIncrementCountSingleInt64KeyFromColumnarBatch(
      batch,
      keyOrdinal)
  }

  def incrementLongSum(
      store: StateStore,
      keys: Array[Array[Byte]],
      deltas: Array[Long]): Array[Long] = {
    requireVeloxStore(store).nativeIncrementLongSum(keys, deltas)
  }

  def incrementLongSumFromColumnarBatch(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinals: Array[Int],
      valueOrdinal: Int): Array[Long] = {
    requireVeloxStore(store).nativeIncrementLongSumFromColumnarBatch(
      batch,
      keyOrdinals,
      valueOrdinal)
  }

  def incrementLongSumFromColumnarBatchWithKeys(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinals: Array[Int],
      valueOrdinal: Int): (Array[Long], Array[Array[Byte]]) = {
    requireVeloxStore(store).nativeIncrementLongSumFromColumnarBatchWithKeys(
      batch,
      keyOrdinals,
      valueOrdinal)
  }

  def incrementLongSumFromColumnarBatchWithKeyValues(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinals: Array[Int],
      valueOrdinal: Int): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireVeloxStore(store).nativeIncrementLongSumFromColumnarBatchWithKeyValues(
      batch,
      keyOrdinals,
      valueOrdinal)
  }

  def incrementLongSumFromColumnarBatchWithFixedWidthRows(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinals: Array[Int],
      valueOrdinal: Int,
      valueFieldCount: Int): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireVeloxStore(store).nativeIncrementLongSumFromColumnarBatchWithFixedWidthRows(
      batch,
      keyOrdinals,
      valueOrdinal,
      valueFieldCount)
  }

  def incrementLongSumSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      valueFieldCount: Int): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireVeloxStore(store).nativeIncrementLongSumSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
      batch,
      keyOrdinal,
      valueOrdinal,
      valueFieldCount)
  }

  def incrementLongSumSingleInt64KeyFromColumnarBatchTypedRows(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinal: Int,
      valueOrdinal: Int): Array[Long] = {
    requireVeloxStore(store)
      .nativeIncrementLongSumSingleInt64KeyFromColumnarBatchTypedRows(
        batch,
        keyOrdinal,
        valueOrdinal)
  }

  def incrementCountSingleInt64KeyFromColumnarBatchTypedRows(
      store: StateStore,
      batch: ColumnarBatch,
      keyOrdinal: Int): Array[Long] = {
    requireVeloxStore(store)
      .nativeIncrementCountSingleInt64KeyFromColumnarBatchTypedRows(batch, keyOrdinal)
  }

  def evictCountKeys(store: StateStore, keys: Array[Array[Byte]]): Array[Long] = {
    requireVeloxStore(store).nativeEvictCountKeys(keys)
  }

  def deduplicate(store: StateStore, keys: Array[Array[Byte]]): Array[Long] = {
    requireVeloxStore(store).nativeDeduplicate(keys)
  }

  def deduplicateWithinWatermark(
      store: StateStore,
      keys: Array[Array[Byte]],
      eventTimeMicros: Array[Long],
      eventTimeWatermarkForLateEventsMicros: Option[Long],
      delayThresholdMicros: Long,
      eventTimeWatermarkForEvictionMicros: Option[Long]): Array[Long] = {
    requireVeloxStore(store).nativeDeduplicateWithinWatermark(
      keys,
      eventTimeMicros,
      eventTimeWatermarkForLateEventsMicros,
      delayThresholdMicros,
      eventTimeWatermarkForEvictionMicros)
  }

  private def requireVeloxStore(store: StateStore): VeloxNativeStateStore = {
    store match {
      case veloxStore: VeloxNativeStateStore => veloxStore
      case other =>
        throw new IllegalStateException(
          s"Velox native streaming state requires " +
            s"${classOf[VeloxNativeStateStoreProvider].getName}, but opened " +
            other.getClass.getName)
    }
  }
}

final private[state] class VeloxNativeStateStore(
    provider: VeloxNativeStateStoreProvider,
    private var nativeHandle: Long,
    initialVersion: Long,
    // true when this store reuses the provider's resident native map instead of
    // owning a freshly opened handle. A resident handle is never freed on close;
    // it is rolled back (abort/release) or re-adopted (commit) by the provider.
    resident: Boolean = false)
  extends StateStore {
  private var committed = false
  private var closed = false
  private var committedVersion = initialVersion
  private var lastMetrics = StateStoreMetrics(0L, 0L, Map.empty)
  private val iteratorHandles = mutable.LinkedHashSet.empty[Long]

  override def id: StateStoreId = provider.stateStoreId

  override def version: Long = initialVersion

  override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
    requireOpen()
    provider.requireDefaultColumnFamily(colFamilyName)
    val bytes = VeloxNativeStateStoreJniWrapper.nativeGet(nativeHandle, key.getBytes)
    if (bytes == null) {
      null
    } else {
      provider.valueRow(bytes)
    }
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
    requireOpen()
    provider.requireDefaultColumnFamily(colFamilyName)
    val iteratorHandle = VeloxNativeStateStoreJniWrapper.nativeIterator(nativeHandle)
    iteratorHandles += iteratorHandle
    new StateStoreIterator[UnsafeRowPair](
      new AbstractIterator[UnsafeRowPair] {
        override def hasNext: Boolean =
          VeloxNativeStateStoreJniWrapper.nativeIteratorHasNext(iteratorHandle)

        override def next(): UnsafeRowPair = {
          val pair = VeloxNativeStateStoreJniWrapper.nativeIteratorNext(iteratorHandle)
          new UnsafeRowPair(provider.keyRow(pair(0)), provider.valueRow(pair(1)))
        }
      },
      () => closeIterator(iteratorHandle)
    )
  }

  override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
    requireOpen()
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
    requireOpen()
    provider.requireDefaultColumnFamily(colFamilyName)
    if (useMultipleValuesPerKey) {
      throw provider.unsupported("multiple values per key")
    }
  }

  override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
    requireOpen()
    provider.requireDefaultColumnFamily(colFamilyName)
    VeloxNativeStateStoreJniWrapper.nativePut(nativeHandle, key.getBytes, value.getBytes)
  }

  override def putList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
    throw provider.unsupported("putList")
  }

  override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
    requireOpen()
    provider.requireDefaultColumnFamily(colFamilyName)
    VeloxNativeStateStoreJniWrapper.nativeRemove(nativeHandle, key.getBytes)
  }

  override def merge(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
    throw provider.unsupported("merge")
  }

  override def mergeList(key: UnsafeRow, values: Array[UnsafeRow], colFamilyName: String): Unit = {
    throw provider.unsupported("mergeList")
  }

  override def commit(): Long = {
    requireOpen()
    val handle = nativeHandle
    var durable = false
    try {
      captureMetrics()
      // The provider decides whether to write a full snapshot, an O(changed) delta, or both,
      // calling the native serializers it needs against the live (still-dirty) handle. The dirty
      // journal must therefore stay intact until after this returns; finishCommit() clears it.
      committedVersion = provider.commit(initialVersion, handle)
      // The checkpoint is now durable on disk. Everything below keeps the map resident for the
      // next batch and must not change the durable result.
      durable = true
      committed = true
      committedVersion
    } finally {
      finishCommit(handle, durable)
    }
  }

  // Completes a commit by transferring the live native map to the provider as
  // the resident store for committedVersion (so the next batch skips the disk
  // read + deserialize), or, if the commit did not become durable, discarding
  // the batch's uncommitted mutations. Either way the store ends up closed.
  private def finishCommit(handle: Long, durable: Boolean): Unit = {
    if (closed || nativeHandle == 0L) {
      return
    }
    closeOpenIterators()
    if (durable && provider.residentReuseActive) {
      // Mark the batch's mutations as durable (so a later rollback cannot undo
      // committed state), advance the resident version, and hand the handle to
      // the provider. The native map is NOT freed.
      VeloxNativeStateStoreJniWrapper.nativeClearDirty(handle)
      VeloxNativeStateStoreJniWrapper.nativeSetVersion(handle, committedVersion)
      provider.adoptResidentAfterCommit(handle, committedVersion)
    } else if (durable) {
      // Resident reuse disabled: free the committed handle, matching the
      // original read-modify-write-from-disk lifecycle exactly.
      VeloxNativeStateStoreJniWrapper.nativeCloseStore(handle)
    } else if (resident) {
      // A resident handle whose commit failed: roll back the uncommitted batch
      // and return the clean map to the provider for the next attempt.
      VeloxNativeStateStoreJniWrapper.nativeRollback(handle)
      provider.returnResident(handle)
    } else {
      // A freshly opened handle whose commit failed owns no resident slot, so
      // simply free it.
      VeloxNativeStateStoreJniWrapper.nativeCloseStore(handle)
    }
    nativeHandle = 0L
    closed = true
    provider.unregisterStore(this)
  }

  private[state] def nativeDeduplicate(keys: Array[Array[Byte]]): Array[Long] = {
    requireOpen()
    VeloxNativeStateStoreJniWrapper.nativeDeduplicate(nativeHandle, keys)
  }

  private[state] def nativeIncrementCount(keys: Array[Array[Byte]]): Array[Long] = {
    requireOpen()
    VeloxNativeStateStoreJniWrapper.nativeIncrementCount(nativeHandle, keys)
  }

  private[state] def nativeIncrementCount(
      keys: Array[Array[Byte]],
      deltas: Array[Long]): Array[Long] = {
    requireOpen()
    VeloxNativeStateStoreJniWrapper.nativeIncrementCountWithDeltas(nativeHandle, keys, deltas)
  }

  private[state] def nativeIncrementCountFromColumnarBatch(
      batch: ColumnarBatch,
      keyOrdinals: Array[Int]): Array[Long] = {
    requireOpen()
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    VeloxNativeStateStoreJniWrapper.nativeIncrementCountFromColumnarBatch(
      nativeHandle,
      batchHandle,
      keyOrdinals)
  }

  private[state] def nativeIncrementCountFromColumnarBatchWithKeys(
      batch: ColumnarBatch,
      keyOrdinals: Array[Int]): (Array[Long], Array[Array[Byte]]) = {
    requireOpen()
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    val result = VeloxNativeStateStoreJniWrapper.nativeIncrementCountFromColumnarBatchWithKeys(
      nativeHandle,
      batchHandle,
      keyOrdinals)
    require(
      result.length == 2,
      s"Invalid native columnar count update result length ${result.length}")
    (
      result(0).asInstanceOf[Array[Long]],
      result(1).asInstanceOf[Array[Array[Byte]]]
    )
  }

  private[state] def nativeIncrementCountFromColumnarBatchWithKeyValues(
      batch: ColumnarBatch,
      keyOrdinals: Array[Int]): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireOpen()
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    val result = VeloxNativeStateStoreJniWrapper.nativeIncrementCountFromColumnarBatchWithKeyValues(
      nativeHandle,
      batchHandle,
      keyOrdinals)
    require(
      result.length == 3,
      s"Invalid native columnar count update result length ${result.length}")
    val keys = result(1).asInstanceOf[Array[Array[Byte]]]
    val values = result(2).asInstanceOf[Array[Array[Byte]]]
    require(
      keys.length == values.length,
      s"Invalid native columnar count update key/value length ${keys.length}/${values.length}")
    (
      result(0).asInstanceOf[Array[Long]],
      keys,
      values
    )
  }

  private[state] def nativeIncrementCountSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
      batch: ColumnarBatch,
      keyOrdinal: Int,
      valueFieldCount: Int): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireOpen()
    require(valueFieldCount > 0, s"Invalid native fixed-width value field count $valueFieldCount")
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    val result = VeloxNativeStateStoreJniWrapper
      .nativeIncrementCountSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
        nativeHandle,
        batchHandle,
        keyOrdinal,
        valueFieldCount)
    require(
      result.length == 3,
      s"Invalid native typed count fixed-width update result length ${result.length}")
    val keys = result(1).asInstanceOf[Array[Array[Byte]]]
    val rows = result(2).asInstanceOf[Array[Array[Byte]]]
    require(
      keys.length == rows.length,
      s"Invalid native typed count update key/row length ${keys.length}/${rows.length}")
    (
      result(0).asInstanceOf[Array[Long]],
      keys,
      rows
    )
  }

  private[state] def nativeIncrementCountSingleInt64KeyFromColumnarBatch(
      batch: ColumnarBatch,
      keyOrdinal: Int): Array[Long] = {
    requireOpen()
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    VeloxNativeStateStoreJniWrapper.nativeIncrementCountSingleInt64KeyFromColumnarBatch(
      nativeHandle,
      batchHandle,
      keyOrdinal)
  }

  private[state] def nativeIncrementLongSum(
      keys: Array[Array[Byte]],
      deltas: Array[Long]): Array[Long] = {
    requireOpen()
    VeloxNativeStateStoreJniWrapper.nativeIncrementLongSumWithDeltas(nativeHandle, keys, deltas)
  }

  private[state] def nativeIncrementLongSumFromColumnarBatch(
      batch: ColumnarBatch,
      keyOrdinals: Array[Int],
      valueOrdinal: Int): Array[Long] = {
    requireOpen()
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    VeloxNativeStateStoreJniWrapper.nativeIncrementLongSumFromColumnarBatch(
      nativeHandle,
      batchHandle,
      keyOrdinals,
      valueOrdinal)
  }

  private[state] def nativeIncrementLongSumFromColumnarBatchWithKeys(
      batch: ColumnarBatch,
      keyOrdinals: Array[Int],
      valueOrdinal: Int): (Array[Long], Array[Array[Byte]]) = {
    requireOpen()
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    val result = VeloxNativeStateStoreJniWrapper.nativeIncrementLongSumFromColumnarBatchWithKeys(
      nativeHandle,
      batchHandle,
      keyOrdinals,
      valueOrdinal)
    require(
      result.length == 2,
      s"Invalid native columnar long sum update result length ${result.length}")
    (
      result(0).asInstanceOf[Array[Long]],
      result(1).asInstanceOf[Array[Array[Byte]]]
    )
  }

  private[state] def nativeIncrementLongSumFromColumnarBatchWithKeyValues(
      batch: ColumnarBatch,
      keyOrdinals: Array[Int],
      valueOrdinal: Int): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireOpen()
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    val result =
      VeloxNativeStateStoreJniWrapper.nativeIncrementLongSumFromColumnarBatchWithKeyValues(
        nativeHandle,
        batchHandle,
        keyOrdinals,
        valueOrdinal)
    require(
      result.length == 3,
      s"Invalid native columnar long sum update result length ${result.length}")
    val keys = result(1).asInstanceOf[Array[Array[Byte]]]
    val values = result(2).asInstanceOf[Array[Array[Byte]]]
    require(
      keys.length == values.length,
      s"Invalid native columnar long sum update key/value length ${keys.length}/${values.length}")
    (
      result(0).asInstanceOf[Array[Long]],
      keys,
      values
    )
  }

  private[state] def nativeIncrementLongSumFromColumnarBatchWithFixedWidthRows(
      batch: ColumnarBatch,
      keyOrdinals: Array[Int],
      valueOrdinal: Int,
      valueFieldCount: Int): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireOpen()
    require(valueFieldCount > 0, s"Invalid native fixed-width value field count $valueFieldCount")
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    val result =
      VeloxNativeStateStoreJniWrapper.nativeIncrementLongSumFromColumnarBatchWithFixedWidthRows(
        nativeHandle,
        batchHandle,
        keyOrdinals,
        valueOrdinal,
        valueFieldCount)
    require(
      result.length == 3,
      s"Invalid native columnar long sum fixed-width update result length ${result.length}")
    val keys = result(1).asInstanceOf[Array[Array[Byte]]]
    val rows = result(2).asInstanceOf[Array[Array[Byte]]]
    require(
      keys.length == rows.length,
      s"Invalid native columnar long sum update key/row length ${keys.length}/${rows.length}")
    (
      result(0).asInstanceOf[Array[Long]],
      keys,
      rows
    )
  }

  private[state] def nativeIncrementLongSumSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
      batch: ColumnarBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      valueFieldCount: Int): (Array[Long], Array[Array[Byte]], Array[Array[Byte]]) = {
    requireOpen()
    require(valueFieldCount > 0, s"Invalid native fixed-width value field count $valueFieldCount")
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    val result = VeloxNativeStateStoreJniWrapper
      .nativeIncrementLongSumSingleInt64KeyFromColumnarBatchWithFixedWidthRows(
        nativeHandle,
        batchHandle,
        keyOrdinal,
        valueOrdinal,
        valueFieldCount)
    require(
      result.length == 3,
      s"Invalid native typed long sum fixed-width update result length ${result.length}")
    val keys = result(1).asInstanceOf[Array[Array[Byte]]]
    val rows = result(2).asInstanceOf[Array[Array[Byte]]]
    require(
      keys.length == rows.length,
      s"Invalid native typed long sum update key/row length ${keys.length}/${rows.length}")
    (
      result(0).asInstanceOf[Array[Long]],
      keys,
      rows
    )
  }

  private[state] def nativeIncrementLongSumSingleInt64KeyFromColumnarBatchTypedRows(
      batch: ColumnarBatch,
      keyOrdinal: Int,
      valueOrdinal: Int): Array[Long] = {
    requireOpen()
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    VeloxNativeStateStoreJniWrapper
      .nativeIncrementLongSumSingleInt64KeyFromColumnarBatchTypedRows(
        nativeHandle,
        batchHandle,
        keyOrdinal,
        valueOrdinal)
  }

  private[state] def nativeIncrementCountSingleInt64KeyFromColumnarBatchTypedRows(
      batch: ColumnarBatch,
      keyOrdinal: Int): Array[Long] = {
    requireOpen()
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    VeloxNativeStateStoreJniWrapper
      .nativeIncrementCountSingleInt64KeyFromColumnarBatchTypedRows(
        nativeHandle,
        batchHandle,
        keyOrdinal)
  }

  private[state] def nativeEvictCountKeys(keys: Array[Array[Byte]]): Array[Long] = {
    requireOpen()
    VeloxNativeStateStoreJniWrapper.nativeEvictCountKeys(nativeHandle, keys)
  }

  private[state] def nativeDeduplicateWithinWatermark(
      keys: Array[Array[Byte]],
      eventTimeMicros: Array[Long],
      eventTimeWatermarkForLateEventsMicros: Option[Long],
      delayThresholdMicros: Long,
      eventTimeWatermarkForEvictionMicros: Option[Long]): Array[Long] = {
    requireOpen()
    VeloxNativeStateStoreJniWrapper.nativeDeduplicateWithinWatermark(
      nativeHandle,
      keys,
      eventTimeMicros,
      eventTimeWatermarkForLateEventsMicros.isDefined,
      eventTimeWatermarkForLateEventsMicros.getOrElse(0L),
      delayThresholdMicros,
      eventTimeWatermarkForEvictionMicros.isDefined,
      eventTimeWatermarkForEvictionMicros.getOrElse(0L)
    )
  }

  override def abort(): Unit = {
    closeNative()
  }

  override def release(): Unit = {
    closeNative()
  }

  override def metrics: StateStoreMetrics = {
    if (!closed && nativeHandle != 0L) {
      captureMetrics()
    }
    lastMetrics
  }

  private def captureMetrics(): Unit = {
    requireOpen()
    val metrics = VeloxNativeStateStoreJniWrapper.nativeMetrics(nativeHandle)
    lastMetrics = StateStoreMetrics(
      metrics(0),
      metrics(1),
      Map(
        VeloxNativeStateStoreProvider.NativeNumKeysMetric -> metrics(0),
        VeloxNativeStateStoreProvider.NativeMemoryBytesMetric -> metrics(1))
    )
  }

  override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
    StateStoreCheckpointInfo(id.partitionId, committedVersion, None, None)
  }

  override def hasCommitted: Boolean = committed

  private def requireOpen(): Unit = {
    if (closed || nativeHandle == 0L) {
      throw new IllegalStateException(s"State store $id version $initialVersion is already closed")
    }
  }

  private def closeNative(): Unit = {
    if (!closed && nativeHandle != 0L) {
      val handle = nativeHandle
      closeOpenIterators()
      if (resident) {
        // Discard this batch's uncommitted writes and return the clean resident
        // map to the provider; the native handle stays alive for reuse.
        VeloxNativeStateStoreJniWrapper.nativeRollback(handle)
        provider.returnResident(handle)
      } else {
        // A freshly opened, uncommitted handle owns no resident slot: free it.
        VeloxNativeStateStoreJniWrapper.nativeCloseStore(handle)
      }
      nativeHandle = 0L
      closed = true
      provider.unregisterStore(this)
    }
  }

  private def closeIterator(iteratorHandle: Long): Unit = {
    if (iteratorHandles.remove(iteratorHandle)) {
      VeloxNativeStateStoreJniWrapper.nativeCloseIterator(iteratorHandle)
    }
  }

  private def closeOpenIterators(): Unit = {
    val handles = iteratorHandles.toSeq
    iteratorHandles.clear()
    handles.foreach(VeloxNativeStateStoreJniWrapper.nativeCloseIterator)
  }
}
