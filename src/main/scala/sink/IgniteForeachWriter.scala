package sink

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.{Ignite, IgniteDataStreamer, Ignition}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ForeachWriter, Row}

class IgniteForeachWriter(sparkContext: SparkContext) extends ForeachWriter[Row] with Serializable {

//  private val streamer: IgniteDataStreamer[String, String] =
//    Ignition.start("ignite-client-config.xml").dataStreamer("bots")
//  streamer.autoFlushFrequency(100)

  val igniteContext: IgniteContext = new IgniteContext(sparkContext, "ignite-client-config.xml")

  override def open(partitionId: Long, version: Long): Boolean = {
    // open connection
//    streamer = Ignition.start("config/example-ignite.xml").dataStreamer("bots")
    true
  }

  override def process(record: Row): Unit = {
    var ignite: Ignite = igniteContext.ignite()
    var streamer: IgniteDataStreamer[String, String] = ignite.dataStreamer("bots")
    val botIp = record.getAs[String]("ipAddress")
    streamer.addData(botIp, botIp)
    streamer.flush()
  }

  override def close(errorOrNull: Throwable): Unit = {
    // close the connection
//    streamer.flush()
//    streamer.close()
  }

}
