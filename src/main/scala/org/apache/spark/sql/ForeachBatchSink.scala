package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.Sink


class ForeachBatchSink[T](batchWriter: (Dataset[T], Long) => Unit, encoder: ExpressionEncoder[T])
  extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    println("+++++Custom ForeachBatchSink+++++")
    val resolvedEncoder = encoder.resolveAndBind(
      data.logicalPlan.output,
      data.sparkSession.sessionState.analyzer)
    val rdd = data.queryExecution.toRdd.map[T](resolvedEncoder.fromRow)(encoder.clsTag)
    val ds = data.sparkSession.createDataset(rdd)(encoder)
    batchWriter(ds, batchId)
  }

  override def toString(): String = "ForeachBatchSink"
}

