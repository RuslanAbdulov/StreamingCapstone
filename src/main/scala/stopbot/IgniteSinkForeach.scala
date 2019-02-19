package stopbot

import java.util.Collections

import org.apache.ignite.spark.IgniteContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

class IgniteSinkForeach(val igniteContext: IgniteContext) extends ForeachWriter[AggregateEvent]{

//  private val ignite = IgniteSparkSession.builder()
//    .appName("Spark Ignite catalog example")
//    .master("local")
//    .config("spark.executor.instances", "1")
    //Only additional option to refer to Ignite cluster.
//    .igniteConfig("ignite-client-config.xml")
//    .getOrCreate()

  override def open(partitionId: Long, version: Long): Boolean = {
    println(s"Open connection")
    true
  }

  override def process(value: AggregateEvent): Unit = {
    igniteContext.ignite().getOrCreateCache("bots").putIfAbsent(value.ipAddress, value.window)
  }

  override def close(errorOrNull: Throwable): Unit = {
    println(s"Close connection")
  }
}
