package tmp

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, regexp_extract, translate}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object FraudDetector2 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("Fraud Detector")
      .config("spark.driver.memory", "2g")
      .getOrCreate()


    val df = spark
      .readStream
//      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ad-events")
      .option("key.deserializer", classOf[StringDeserializer].toString)
      .option("value.deserializer", classOf[StringDeserializer].toString)
      .option("failOnDataLoss", value = false)
      .load()


    import spark.implicits._

    val eventSchema =  new StructType()
      .add("unix_time", LongType, nullable = false)
      .add("category_id", IntegerType, nullable = false)
      .add("ip", StringType, nullable = false)
      .add("type", StringType, nullable = false)

    val groomedJson = df
      .select(translate($"value".cast(StringType), "\\", "").as("value"))
      .select(regexp_extract($"value".cast(StringType), "(\\{.*\\})", 1).as("json"))

    val events = groomedJson
      .select(from_json($"json".cast(StringType), schema = eventSchema).as("struct"))
      .na.drop()
      .select($"struct.*")
      .toDF("unixTime", "categoryId", "ipAddress", "eventType")










//    val query = events.writeStream
//      .format("console")
//      .trigger(Trigger.ProcessingTime("2 seconds"))
//      .start()
//
//    query.awaitTermination()






    //    val testDS = Seq("{\"unix_time\": 1538932022, \"category_id\": 1008, \"ip\": \"172.10.0.65\", \"type\": \"click\"}").toDS
    //    val testParsed = testDS.select(from_json($"value", schema = eventSchema).as("struct"))//.select("struct.*")
    //    testParsed.collect()

    //    val dfExample = spark.sql(select "{\"unix_time\": 1538932022, \"category_id\": 1009, \"ip\": \"172.10.2.2\", \"type\": \"view\"}" as json""")
    //    val dfICanWorkWith = dfExample.select(from_json($"json", eventSchema))
    //    dfICanWorkWith.collect()




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




    //TODO log corrupt records
    //    if(financesDS.hasColumn("_corrupt_record")) {
    //	    financesDS.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
    //	              .write.mode(SaveMode.Overwrite).text("Output/corrupt_finances")
    //    }
  }

//  val filteredEvents = events
//    .filter(column => {
//      val cache: IgniteCache[String, String] = igniteContext.ignite().getOrCreateCache("bots")
//      val result = !cache.containsKey(column.ipAddress)
//      println(result)
//      result
//    })

}
