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
package org.apache.gluten.connector.write;

import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Locale;
import java.util.Objects;

/**
 * ColumnarMicroBatchWriterFactory is used to create ColumnarMicroBatchWriter.
 *
 * <p>It is used in micro-batch mode.
 */
public class ColumnarMicroBatchWriterFactory implements ColumnarBatchDataWriterFactory {

  private final long epochId;
  private final ColumnarStreamingDataWriterFactory streamingWriterFactory;
  private final boolean failTaskAfterWriteEnabled;
  private final String failTaskAfterWriteAction;
  private final int failTaskAfterWritePartitionId;
  private final boolean failTaskAfterCommitEnabled;
  private final String failTaskAfterCommitAction;
  private final int failTaskAfterCommitPartitionId;

  public ColumnarMicroBatchWriterFactory(
      long epochId, ColumnarStreamingDataWriterFactory streamingWriterFactory) {
    this(epochId, streamingWriterFactory, false, "throw", 0, false, "throw", 0);
  }

  public ColumnarMicroBatchWriterFactory(
      long epochId,
      ColumnarStreamingDataWriterFactory streamingWriterFactory,
      boolean failTaskAfterWriteEnabled,
      String failTaskAfterWriteAction,
      int failTaskAfterWritePartitionId) {
    this(
        epochId,
        streamingWriterFactory,
        failTaskAfterWriteEnabled,
        failTaskAfterWriteAction,
        failTaskAfterWritePartitionId,
        false,
        "throw",
        0);
  }

  public ColumnarMicroBatchWriterFactory(
      long epochId,
      ColumnarStreamingDataWriterFactory streamingWriterFactory,
      boolean failTaskAfterWriteEnabled,
      String failTaskAfterWriteAction,
      int failTaskAfterWritePartitionId,
      boolean failTaskAfterCommitEnabled,
      String failTaskAfterCommitAction,
      int failTaskAfterCommitPartitionId) {
    if (epochId < 0) {
      throw new IllegalArgumentException(
          "Native streaming sink writer requires a non-negative epoch id: " + epochId);
    }
    if (failTaskAfterWritePartitionId < 0) {
      throw new IllegalArgumentException(
          "Native streaming sink task failure target partition id must be non-negative: "
              + failTaskAfterWritePartitionId);
    }
    if (failTaskAfterCommitPartitionId < 0) {
      throw new IllegalArgumentException(
          "Native streaming sink post-commit task failure target partition id "
              + "must be non-negative: "
              + failTaskAfterCommitPartitionId);
    }
    this.epochId = epochId;
    this.streamingWriterFactory =
        Objects.requireNonNull(streamingWriterFactory, "streamingWriterFactory");
    this.failTaskAfterWriteEnabled = failTaskAfterWriteEnabled;
    this.failTaskAfterWriteAction =
        validateFailureAction("failTaskAfterWriteAction", failTaskAfterWriteAction);
    this.failTaskAfterWritePartitionId = failTaskAfterWritePartitionId;
    this.failTaskAfterCommitEnabled = failTaskAfterCommitEnabled;
    this.failTaskAfterCommitAction =
        validateFailureAction("failTaskAfterCommitAction", failTaskAfterCommitAction);
    this.failTaskAfterCommitPartitionId = failTaskAfterCommitPartitionId;
  }

  @Override
  public DataWriter<ColumnarBatch> createWriter(int partitionId, long taskId) {
    return streamingWriterFactory.createWriter(partitionId, taskId, epochId);
  }

  public boolean failTaskAfterWriteEnabled() {
    return failTaskAfterWriteEnabled;
  }

  public String failTaskAfterWriteAction() {
    return failTaskAfterWriteAction;
  }

  public int failTaskAfterWritePartitionId() {
    return failTaskAfterWritePartitionId;
  }

  public boolean failTaskAfterCommitEnabled() {
    return failTaskAfterCommitEnabled;
  }

  public String failTaskAfterCommitAction() {
    return failTaskAfterCommitAction;
  }

  public int failTaskAfterCommitPartitionId() {
    return failTaskAfterCommitPartitionId;
  }

  private static String validateFailureAction(String name, String action) {
    String normalized = Objects.requireNonNull(action, name).toLowerCase(Locale.ROOT);
    if (!"throw".equals(normalized) && !"halt".equals(normalized)) {
      throw new IllegalArgumentException(
          name + " must be one of [throw, halt], but was: " + action);
    }
    return normalized;
  }
}
