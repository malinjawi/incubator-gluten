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
package org.apache.gluten.execution

import org.apache.iceberg.{DataFiles, FileFormat, PartitionSpec, Snapshot, Table}
import org.apache.iceberg.io.{FileIO, InputFile}
import org.scalatest.funsuite.AnyFunSuite

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util

import scala.jdk.CollectionConverters._

class IcebergStreamingEpochAuditSuite extends AnyFunSuite {

  test("native Iceberg streaming epoch audit finds exact committed snapshot") {
    val epoch0Path = "file:/warehouse/table/data/00000-query-0-00000.parquet"
    val epoch1Path = "file:/warehouse/table/data/00000-query-1-00000.parquet"
    val snapshot0 = snapshot(
      id = 10L,
      parentId = null,
      queryId = "query",
      epochId = 0L,
      paths = Seq(epoch0Path))
    val snapshot1 = snapshot(
      id = 11L,
      parentId = 10L,
      queryId = "query",
      epochId = 1L,
      paths = Seq(epoch1Path))
    val icebergTable = table(
      current = snapshot1,
      snapshotsById = Map(10L -> snapshot0, 11L -> snapshot1),
      existingPaths = Set(epoch0Path, epoch1Path))

    assert(IcebergStreamingEpochAudit.findLastCommittedStreamingEpochId(
      icebergTable,
      "query") == Some(1L))
    assert(IcebergStreamingEpochAudit
      .findCommittedStreamingEpochSnapshot(icebergTable, "query", 0L)
      .map(_.snapshotId()) == Some(10L))

    IcebergStreamingEpochAudit.auditCommittedStreamingEpochFiles(
      icebergTable,
      "query",
      0L,
      snapshot0)
  }

  test("native Iceberg streaming epoch audit fails closed when object-store file is missing") {
    val missingPath = "file:/warehouse/table/data/00000-query-2-00000.parquet"
    val presentPath = "file:/warehouse/table/data/00001-query-2-00000.parquet"
    val committedSnapshot = snapshot(
      id = 20L,
      parentId = null,
      queryId = "query",
      epochId = 2L,
      paths = Seq(missingPath, presentPath))
    val icebergTable = table(
      current = committedSnapshot,
      snapshotsById = Map(20L -> committedSnapshot),
      existingPaths = Set(presentPath))

    val auditError = intercept[IllegalStateException] {
      IcebergStreamingEpochAudit.auditCommittedStreamingEpochFiles(
        icebergTable,
        "query",
        2L,
        committedSnapshot)
    }

    assert(auditError.getMessage.contains("data file(s) are missing from object store"))
    assert(auditError.getMessage.contains(missingPath))
    assert(!auditError.getMessage.contains(presentPath))
  }

  private def snapshot(
      id: Long,
      parentId: java.lang.Long,
      queryId: String,
      epochId: Long,
      paths: Seq[String]): Snapshot = {
    val dataFiles = paths.map {
      path =>
        DataFiles
          .builder(PartitionSpec.unpartitioned())
          .withPath(path)
          .withFormat(FileFormat.PARQUET)
          .withRecordCount(1L)
          .withFileSizeInBytes(1L)
          .build()
    }
    proxy[Snapshot] {
      (method, _) =>
        method.getName match {
          case "snapshotId" => id
          case "parentId" => parentId
          case "summary" =>
            Map(
              "spark.sql.streaming.queryId" -> queryId,
              "spark.sql.streaming.epochId" -> epochId.toString).asJava
          case "addedDataFiles" => dataFiles.asJava
          case other => unsupported(other)
        }
    }
  }

  private def table(
      current: Snapshot,
      snapshotsById: Map[Long, Snapshot],
      existingPaths: Set[String]): Table = {
    val fileIo = fileIO(existingPaths)
    proxy[Table] {
      (method, args) =>
        method.getName match {
          case "currentSnapshot" => current
          case "snapshot" =>
            method.getParameterTypes.toSeq match {
              case Seq(java.lang.Long.TYPE) =>
                snapshotsById(args.head.asInstanceOf[java.lang.Long].longValue())
              case _ => null
            }
          case "io" => fileIo
          case "refresh" => null
          case other => unsupported(other)
        }
    }
  }

  private def fileIO(existingPaths: Set[String]): FileIO = {
    proxy[FileIO] {
      (method, args) =>
        method.getName match {
          case "newInputFile" => inputFile(args.head.toString, existingPaths)
          case "properties" => util.Collections.emptyMap[String, String]()
          case "close" => null
          case other => unsupported(other)
        }
    }
  }

  private def inputFile(path: String, existingPaths: Set[String]): InputFile = {
    proxy[InputFile] {
      (method, _) =>
        method.getName match {
          case "exists" => existingPaths.contains(path)
          case "location" => path
          case "getLength" => 1L
          case other => unsupported(other)
        }
    }
  }

  private def proxy[T: reflect.ClassTag](answer: (Method, Array[AnyRef]) => Any): T = {
    val iface = implicitly[reflect.ClassTag[T]].runtimeClass
    Proxy
      .newProxyInstance(
        iface.getClassLoader,
        Array(iface),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            answer(method, Option(args).getOrElse(Array.empty[AnyRef])).asInstanceOf[AnyRef]
          }
        }
      )
      .asInstanceOf[T]
  }

  private def unsupported(methodName: String): Nothing = {
    throw new UnsupportedOperationException(methodName)
  }
}
