package tmp

import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object FraudDetectorIgniteTableLookUp {

  val IGNITE_CONFIG = "ignite-client-config.xml"

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local[5]")
      .appName("Fraud Detector")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val df = spark
      .readStream
      .text("Data/")


    //Define Ignite table
    val botsIgniteDF = spark.read
      .format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_TABLE, "bots")
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, IGNITE_CONFIG)
      .load()
    botsIgniteDF.createOrReplaceTempView("bots")


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


    //Filter using Ignite table
    events.createGlobalTempView("events")

    val events2 = events.join(
      botsIgniteDF.select($"ipAddress".as("cachedIp")), $"ipAddress" === $"cachedIp", "left_anti")


    val groupedByIp = events2
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

    enormousAmountDF
      .writeStream
      .outputMode("update") //TODO append?
      .foreachBatch((batchDF: Dataset[Row], batchId: Long) =>
      batchDF
        .select("ipAddress", "window.end")
          .write
          .format(IgniteDataFrameSettings.FORMAT_IGNITE)
          .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, IGNITE_CONFIG)
          .option(IgniteDataFrameSettings.OPTION_TABLE, "bots")
          .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "ipAddress")
          .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
          .mode("append")
          .save())
      .start()
      .awaitTermination()

  }

}
