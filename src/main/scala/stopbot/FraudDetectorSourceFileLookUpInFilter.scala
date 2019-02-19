package stopbot

import org.apache.ignite.IgniteCache
import org.apache.ignite.spark.{IgniteContext, IgniteDataFrameSettings}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}

object FraudDetectorSourceFileLookUpInFilter {


  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local[5]")
      .appName("Fraud Detector")
      .config("spark.driver.memory", "2g")
      .getOrCreate()


    val igniteContext = new IgniteContext(spark.sparkContext, "ignite-client-config.xml")
    //    val cache: IgniteCache[String, String] = igniteContext.ignite().getOrCreateCache("bots")
    //    val cache = igniteContext.fromCache("bots")

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
      .as[Event]
      .filter(column => {
        val cache: IgniteCache[String, String] = igniteContext.ignite().getOrCreateCache("bots")
        !cache.containsKey(column.ipAddress)})

    val groupedByIp = events
      .withWatermark("unixTime", "1 minute") //10 minutes
      .groupBy(
      window($"unixTime", "10 minutes", "5 minutes"),
      $"ipAddress")


    //Enormous event rate, e.g. more than 1000 request in 10 minutes*.
    val enormousAmountDF = groupedByIp
      .agg(
        count($"unixTime").as("amount"),
        (count(when($"eventType" === "click", $"unixTime"))
          / count(when($"eventType" === "view", $"unixTime"))).as("rate"),
        size(collect_set($"categoryId")).as("categories")
      )
      .filter($"amount" > 10 || $"rate" > 3 || $"categories" > 5) //TODO set $"amount" > 1000
    //        .as[AggregateEvent]

    enormousAmountDF
      .writeStream
      .outputMode("update") //TODO append?
      //      .foreach(new IgniteSinkForeach(igniteContext))
      .foreachBatch((batchDF: Dataset[Row], batchId: Long) =>
    {

      //        batchDF
      //          .foreach(row => {
      //            val cache: IgniteCache[String, String] = igniteContext.ignite().getOrCreateCache("bots")
      //            cache.putIfAbsent(row.ipAddress, "window.end")})
      batchDF
        .select("ipAddress", "window.end")
        .write
        .format(IgniteDataFrameSettings.FORMAT_IGNITE)
        .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, "ignite-client-config.xml")
        .option(IgniteDataFrameSettings.OPTION_TABLE, "bots")
        .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "ipAddress")
        .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
        .mode("append")
        .save()
    })
      .start()
      .awaitTermination()

  }
}
