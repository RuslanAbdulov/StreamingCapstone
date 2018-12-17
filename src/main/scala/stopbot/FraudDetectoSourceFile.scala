package stopbot

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_extract, _}
import org.apache.spark.sql.types._


//[{"unix_time": 1538076151, "category_id": 1009, "ip": "172.10.2.42", "type": "view"},
//{"unix_time": 1538076151, "category_id": 1004, "ip": "172.10.1.139", "type": "click"},
//val regex = "^\\[?(\\{.*\\})[\\,\\]]?$".r

object FraudDetectoSourceFile {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("Fraud Detector")
      .config("spark.driver.memory", "2g")
      .getOrCreate()


    val df = spark
//      .readStream
      .read
      .text("Data/ad-events.json")

    df.printSchema()


    import spark.implicits._

    val eventSchema =  new StructType()
      .add("unix_time", LongType, nullable = false)
      .add("category_id", IntegerType, nullable = false)
      .add("ip", StringType, nullable = false)
      .add("type", StringType, nullable = false)


//    df.take(5).foreach(r => println(r))

    val groomedJson = df
      .select(translate($"value".cast(StringType), "\\", "").as("value"))
      .select(regexp_extract($"value".cast(StringType), "(\\{.*\\})", 1).as("json"))

    groomedJson.foreach(r => println(r))
    groomedJson.printSchema()

    val events = groomedJson
      .select(from_json($"json".cast(StringType), schema = eventSchema).as("struct"))
      .na.drop()
      .select($"struct.*")
      .toDF("unixTime", "categoryId", "ipAddress", "eventType")
//      .as[Event]

//    val groupedByIp = events.groupByKey(ev => ev.ipAddress)

    events.show(5)


    //Enormous event rate, e.g. more than 1000 request in 10 minutes*.
    val enormousRateDF = events.groupBy($"ipAddress")
        .agg(count($"unixTime").as("amount"))
        .filter($"amount" > 10)


    enormousRateDF.show()






//    val query = events.writeStream
//      .format("console")
//      .trigger(Trigger.ProcessingTime("2 seconds"))
//      .start()
//
//    query.awaitTermination()




}
