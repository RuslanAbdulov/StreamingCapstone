package writer

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import model.AggregateEvent
import org.apache.ignite.IgniteCache
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.ForeachWriter

class IgniteForeachWriter(sparkContext: SparkContext) extends ForeachWriter[AggregateEvent] with Serializable {

  val igniteContext: IgniteContext = new IgniteContext(sparkContext, "ignite-client-config.xml")

  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(aggregateEvent: AggregateEvent): Unit = {
    val ignite = igniteContext.ignite()
    val cache: IgniteCache[String, LocalDateTime] = ignite.getOrCreateCache("bots")
    //todo use putIfAbsentAsync(it doesn't show data, seems like you need to call
    // get from future that this method return or some kind of flush)
    aggregateEvent.events.foreach(event => {
      cache.putIfAbsentAsync(event.ipAddress,
        LocalDateTime.parse(aggregateEvent.window.toString,
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
    })
  }

  override def close(errorOrNull: Throwable): Unit = Unit

//  batchDf
//    .select("ip_address", "unix_time")
//    .write
//    .format(IgniteDataFrameSettings.FORMAT_IGNITE)
//    .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, IGNITE_CONFIG)
//    .option(IgniteDataFrameSettings.OPTION_TABLE, "bots")
//    .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "ip_address")
//    .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
//    .mode("append")
//    .save()

}
