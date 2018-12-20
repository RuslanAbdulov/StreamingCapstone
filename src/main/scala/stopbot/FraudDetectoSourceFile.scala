package stopbot

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_extract, _}
import org.apache.spark.sql.streaming.Trigger
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
      .readStream
      .text("Data/")


    import spark.implicits._

    val eventSchema = new StructType()
      .add("unix_time", TimestampType, nullable = false)
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
    //      .as[Event]

    val groupedByIp = events
      .withWatermark("unixTime", "1 minutes") //10 minutes
      .groupBy(
        window($"unixTime", "1 minutes", "30 seconds"), //10 minutes, 5 minutes
        $"ipAddress")


    //Enormous event rate, e.g. more than 1000 request in 10 minutes*.
    val enormousAmountDF = groupedByIp
      .agg(count($"unixTime").as("amount"))
      .filter($"amount" > 10)


    //High difference between click and view events, e.g. (clicks/views) more than 3-5. Correctly process cases when there is no views.
    val highDifferenceDF = groupedByIp
      .agg((
        count(when($"eventType" === "click", $"unixTime"))
          / count(when($"eventType" === "view", $"unixTime"))).as("rate"))
      .filter($"rate" > 3)


    //Looking for many categories during the period, e.g. more than 5 categories in 10 minutes.
    val enormousCategoriesDF = groupedByIp
      .agg(countDistinct($"categoryId").as("categories"))
      .filter($"categories" > 5)


//    enormousAmountDF.show()
    val query1 = enormousAmountDF.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

//    highDifferenceDF.show()
    val query2 = highDifferenceDF.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

//    enormousCategoriesDF.show()
    val query3 = enormousCategoriesDF.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()


    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()


  }

}
