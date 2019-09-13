package stopbot

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spray.json._

object SpotBotDStream {

  val IGNITE_CONFIG = "ignite-client-config.xml"


  def main(args: Array[String]) {

    case class EventSchema(unix_time: Long, category_id: Int, ip: String, `type`: String)

    object MyJsonProtocol extends DefaultJsonProtocol {
      implicit val colorFormat = jsonFormat4(EventSchema)
    }


    val conf = new SparkConf().setAppName("DStreamSpotBot").setMaster("local[5]")
    val streamingContext = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("ad-events")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    import MyJsonProtocol._
    val regexp = """(\{.*\})""".r

    val events = stream.map(rdd => rdd.value())
      .map(json => json.replaceAll("\\\\", ""))
      .map(json => regexp.findFirstIn(json).orNull)
      .filter(json => json != null)
      .map(json => {
        json.parseJson.convertTo[EventSchema]
      })


    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
