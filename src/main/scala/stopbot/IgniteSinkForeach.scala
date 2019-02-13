package stopbot



import java.util.Collections

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.apache.spark.sql.{ForeachWriter, Row}

class IgniteSinkForeach() extends ForeachWriter[Row]{

  private val ignite = IgniteSparkSession.builder()
    .appName("Spark Ignite catalog example")
    .master("local")
//    .config("spark.executor.instances", "1")
    //Only additional option to refer to Ignite cluster.
    .igniteConfig("ignite-client-config.xml")
    .getOrCreate()

  override def open(partitionId: Long, version: Long): Boolean = {
    println(s"Open connection")
    true
  }

  override def process(value: Row): Unit = {
    ignite.createDataFrame(Collections.singletonList(value), value.schema)
  }

  override def close(errorOrNull: Throwable): Unit = {
    println(s"Close connection")
  }
}
