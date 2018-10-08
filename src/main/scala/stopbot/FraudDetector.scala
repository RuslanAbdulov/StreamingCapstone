package stopbot

import java.util.regex.{MatchResult, Pattern}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.functions.reverse


//[{"unix_time": 1538076151, "category_id": 1009, "ip": "172.10.2.42", "type": "view"},
//{"unix_time": 1538076151, "category_id": 1004, "ip": "172.10.1.139", "type": "click"},
//val regex = "^\\[?(\\{.*\\})[\\,\\]]?$".r

abstract class EventAbstract {
  def unixTime: Long
  def categoryId: Int
  def ipAddress: String
  def eventType: String
}

case class Click(unixTime: Long, categoryId: Int, ipAddress: String, eventType: String) extends EventAbstract
case class View(unixTime: Long, categoryId: Int, ipAddress: String, eventType: String) extends EventAbstract

case class Event(unixTime: Long, categoryId: Int, ipAddress: String, eventType: String) {
  def isClick: Boolean = eventType == "click"
  def isView: Boolean = eventType == "view"
}

object FraudDetector {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("Fraud Detector")
      .config("spark.driver.memory", "2g")
      .getOrCreate()


    val df = spark
//      .readStream
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ad-events")
      .option("key.deserializer", classOf[StringDeserializer].toString)
      .option("value.deserializer", classOf[StringDeserializer].toString)
      .load()

    df.printSchema()


    import spark.implicits._


    val eventSchema = new StructType()
      .add("unix_time", LongType, nullable = false)
      .add("category_id", IntegerType, nullable = false)
      .add("ip", StringType, nullable = false)
      .add("type", StringType, nullable = false)


//    var evPattern = """^\[?(\{.*\})[\,\]]?$"""

    val originalEvents = df.limit(10).select($"value".cast("string"))

    val events = df.limit(10)
      .select(regexp_extract($"value".cast("string"), "(\\{.*\\})", 1).alias("value2"))
//      .select(from_json($"value2", eventSchema)).as("struct")
//      .map(r => Event(r.getLong(0), r.getInt(1), r.getString(2), r.getString(3)))


    events.count()

//    val query = events.writeStream
//      .format("console")
//      .trigger(Trigger.ProcessingTime("2 seconds"))
//      .start()
//
//    query.awaitTermination()







    //
//
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "use_a_separate_group_id_for_each_stream",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )

//    val topics = Array("ad-events")
//    val stream = KafkaUtils.createDirectStream[String, String](
//      streamingContext,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams)
//    )


//    stream.map(record => (record.key, record.value))



  }


}
