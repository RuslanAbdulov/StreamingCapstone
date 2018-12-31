package stopbot

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_extract, _}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.FileInputDStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.parsing.json.JSON


//[{"unix_time": 1538076151, "category_id": 1009, "ip": "172.10.2.42", "type": "view"},
//{"unix_time": 1538076151, "category_id": 1004, "ip": "172.10.1.139", "type": "click"},
//val regex = "^\\[?(\\{.*\\})[\\,\\]]?$".r

object FraudDetectorSourceFileDStream {

  def main(args: Array[String]) {

//    val spark = SparkSession.builder
//      .master("local[5]")
//      .appName("Fraud Detector")
//      .config("spark.driver.memory", "2g")
//      .getOrCreate()

    val conf = new SparkConf()
      .setAppName("Fraud Detector DStream")
      .setMaster("local[5]")

    val ssc = new StreamingContext(conf, Seconds(5))


    val dStream = ssc.textFileStream("Data/")
//    val dStream = ssc.fileStream[LongWritable, Text, TextInputFormat]("Data/",
//      (path: Path) => !path.getName.startsWith("."), newFilesOnly = false)



    println("=====Start======")
    dStream.print()
//    dStream.foreachRDD(rdd => rdd.take(10).foreach(entry => println(entry._2.toString)))

//    val windowed = dStream.window(Minutes(10), Minutes(5))
    val windowed = dStream.window(Minutes(10), Minutes(5))


    val events = windowed
      .map(str => str.replaceAll("[{", "{"))
      .map(str => str.replaceAll("},", "}"))
      .map(str => str.replaceAll("}]", "}"))
//      .map(entry => JSON.parseRaw(entry))



      events.print(10)
    events.count().print()
//      events.take(10).foreach(println())

    ssc.start()

    ssc.awaitTermination()




//    import spark.implicits._
//
//    val eventSchema = new StructType()
//      .add("unix_time", TimestampType, nullable = false)
//      .add("category_id", IntegerType, nullable = false)
//      .add("ip", StringType, nullable = false)
//      .add("type", StringType, nullable = false)
//
//
//    val groomedJson = df
//      .select(translate($"value".cast(StringType), "\\", "").as("value"))
//      .select(regexp_extract($"value".cast(StringType), "(\\{.*\\})", 1).as("json"))
//
//    val events = groomedJson
//      .select(from_json($"json".cast(StringType), schema = eventSchema).as("struct"))
//      .na.drop()
//      .select($"struct.*")
//      .toDF("unixTime", "categoryId", "ipAddress", "eventType")
//    //      .as[Event]
//
//    val groupedByIp = events
//      .withWatermark("unixTime", "10 minutes") //10 minutes
//      .groupBy(
//        window($"unixTime", "10 minutes", "5 minutes"), //10 minutes, 5 minutes
//        $"ipAddress")
//
//
//    //Enormous event rate, e.g. more than 1000 request in 10 minutes*.
//    val enormousAmountDF = groupedByIp
//      .agg(count($"unixTime").as("amount"))
//      .filter($"amount" > 10)
//
//
//    //High difference between click and view events, e.g. (clicks/views) more than 3-5. Correctly process cases when there is no views.
//    val highDifferenceDF = groupedByIp
//      .agg((
//        count(when($"eventType" === "click", $"unixTime"))
//          / count(when($"eventType" === "view", $"unixTime"))).as("rate"))
//      .filter($"rate" > 3)
//
//
//    //Looking for many categories during the period, e.g. more than 5 categories in 10 minutes.
//    val enormousCategoriesDF = groupedByIp
//      .agg(size(collect_set($"categoryId")).as("categories"))
//      .filter($"categories" > 5)
//
//
//    val query1 = enormousAmountDF.writeStream
//      .outputMode("update")//append
//      .format("console")
//      .trigger(Trigger.ProcessingTime("20 seconds"))
//      .start()
//
//    val query2 = highDifferenceDF.writeStream
//      .outputMode("update")//append
//      .format("console")
//      .trigger(Trigger.ProcessingTime("20 seconds"))
//      .start()
//
//    val query3 = enormousCategoriesDF.writeStream
//      .outputMode("update")
//      .format("console")
//      .trigger(Trigger.ProcessingTime("20 seconds"))
//      .start()
//
//
//    query1.awaitTermination()
//    query2.awaitTermination()
//    query3.awaitTermination()


  }

}
