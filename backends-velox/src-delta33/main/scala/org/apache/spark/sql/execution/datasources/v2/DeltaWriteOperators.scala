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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.{GlutenOptimisticTransaction, OptimisticTransaction, TransactionExecutionObserver}
import org.apache.spark.sql.delta.commands.GlutenDeltaDeleteTiming
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric

case class GlutenDeltaLeafV2CommandExec(delegate: LeafV2CommandExec) extends LeafV2CommandExec {

  override def metrics: Map[String, SQLMetric] = delegate.metrics

  override protected def run(): Seq[InternalRow] = {
    TransactionExecutionObserver.withObserver(
      DeltaV2WriteOperators.UseColumnarDeltaTransactionLog) {
      delegate.executeCollect()
    }
  }

  override def output: Seq[Attribute] = {
    delegate.output
  }

  override def nodeName: String = "GlutenDelta " + delegate.nodeName
}

case class GlutenDeltaLeafRunnableCommand(delegate: LeafRunnableCommand)
  extends LeafRunnableCommand {
  override lazy val metrics: Map[String, SQLMetric] = delegate.metrics

  override def output: Seq[Attribute] = {
    delegate.output
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val timingEnabled = GlutenDeltaDeleteTiming.isEnabled(sparkSession)
    val start = GlutenDeltaDeleteTiming.now()
    try {
      val result = TransactionExecutionObserver.withObserver(
        DeltaV2WriteOperators.createTransactionObserver(timingEnabled, delegate.nodeName)) {
        delegate.run(sparkSession)
      }
      GlutenDeltaDeleteTiming.logIfEnabled(
        timingEnabled,
        s"command totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} command=${delegate.nodeName}")
      result
    } catch {
      case t: Throwable =>
        GlutenDeltaDeleteTiming.logIfEnabled(
          timingEnabled,
          s"command failed totalMs=${GlutenDeltaDeleteTiming.elapsedMs(start)} " +
            s"command=${delegate.nodeName} error=${t.getClass.getName}: ${t.getMessage}"
        )
        throw t
    }
  }

  override def nodeName: String = "GlutenDelta " + delegate.nodeName
}

object DeltaV2WriteOperators {
  def createTransactionObserver(
      timingEnabled: Boolean,
      commandName: String): TransactionExecutionObserver = {
    if (timingEnabled) {
      new TimingTransactionExecutionObserver(commandName)
    } else {
      UseColumnarDeltaTransactionLog
    }
  }

  object UseColumnarDeltaTransactionLog extends TransactionExecutionObserver {
    override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction = {
      val delegate = f
      new GlutenOptimisticTransaction(delegate)
    }

    override def preparingCommit[T](f: => T): T = f

    override def beginDoCommit(): Unit = ()

    override def beginBackfill(): Unit = ()

    override def beginPostCommit(): Unit = ()

    override def transactionCommitted(): Unit = ()

    override def transactionAborted(): Unit = ()

    override def createChild(): TransactionExecutionObserver = {
      TransactionExecutionObserver.getObserver
    }
  }

  private class TimingTransactionExecutionObserver(commandName: String)
    extends TransactionExecutionObserver {
    private var startingTransactionNs: Long = 0
    private var prepareCommitNs: Long = 0
    private var doCommitStart: Long = 0
    private var doCommitNs: Long = 0
    private var backfillStart: Long = 0
    private var backfillNs: Long = 0
    private var postCommitStart: Long = 0
    private var postCommitNs: Long = 0
    private val transactionStart = GlutenDeltaDeleteTiming.now()

    override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction = {
      val start = GlutenDeltaDeleteTiming.now()
      val delegate = f
      startingTransactionNs += GlutenDeltaDeleteTiming.now() - start
      new GlutenOptimisticTransaction(delegate)
    }

    override def preparingCommit[T](f: => T): T = {
      val start = GlutenDeltaDeleteTiming.now()
      try {
        f
      } finally {
        prepareCommitNs += GlutenDeltaDeleteTiming.now() - start
      }
    }

    override def beginDoCommit(): Unit = {
      doCommitStart = GlutenDeltaDeleteTiming.now()
    }

    override def beginBackfill(): Unit = {
      val now = GlutenDeltaDeleteTiming.now()
      if (doCommitStart != 0) {
        doCommitNs += now - doCommitStart
        doCommitStart = 0
      }
      backfillStart = now
    }

    override def beginPostCommit(): Unit = {
      val now = GlutenDeltaDeleteTiming.now()
      if (backfillStart != 0) {
        backfillNs += now - backfillStart
        backfillStart = 0
      } else if (doCommitStart != 0) {
        doCommitNs += now - doCommitStart
        doCommitStart = 0
      }
      postCommitStart = now
    }

    override def transactionCommitted(): Unit = {
      val now = GlutenDeltaDeleteTiming.now()
      if (postCommitStart != 0) {
        postCommitNs += now - postCommitStart
        postCommitStart = 0
      }
      GlutenDeltaDeleteTiming.logIfEnabled(
        enabled = true,
        transactionTimingMessage(now, "committed"))
    }

    override def transactionAborted(): Unit = {
      GlutenDeltaDeleteTiming.logIfEnabled(
        enabled = true,
        transactionTimingMessage(GlutenDeltaDeleteTiming.now(), "aborted"))
    }

    override def createChild(): TransactionExecutionObserver = {
      new TimingTransactionExecutionObserver(s"$commandName.child")
    }

    private def transactionTimingMessage(now: Long, status: String): String = {
      s"transaction status=$status command=$commandName " +
        s"totalMs=${GlutenDeltaDeleteTiming.nanosToMs(now - transactionStart)} " +
        s"startingTransactionMs=${GlutenDeltaDeleteTiming.nanosToMs(startingTransactionNs)} " +
        s"prepareCommitMs=${GlutenDeltaDeleteTiming.nanosToMs(prepareCommitNs)} " +
        s"doCommitMs=${GlutenDeltaDeleteTiming.nanosToMs(doCommitNs)} " +
        s"backfillMs=${GlutenDeltaDeleteTiming.nanosToMs(backfillNs)} " +
        s"postCommitMs=${GlutenDeltaDeleteTiming.nanosToMs(postCommitNs)}"
    }
  }
}
