package BotChecker

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object AppSStream {

  val CHECKPOINT_DIRECTORY = "/Users/avaniukov/Documents/Training/Streaming/BotGen/checkpoints"

  val IP_FIELD = "ip"
  val CATEGORY_FIELD = "category_id"
  val TYPE_FIELD = "type"
  val UNIX_TIME_FIELD = "unix_time"

  val VIEW_TYPE = "view"
  val CLICK_TYPE = "click"

  val LATENCY_DUR = 10
  val SLIDE_DUR = 30
  val WINDOW_DUR = 60
  val TERMINATION_WAIT_TIME = 5

  val PRINT_LIMIT = 1000

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SSBotChecker")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test_stream4")
      .option("startingOffsets", "earliest")
      .load()

    //val eventScheme = ScalaReflection.schemaFor[EventScheme].dataType.asInstanceOf[StructType]

    val eventScheme = StructType(
      List(
        StructField("unix_time", StringType, false),
        StructField("category_id", StringType, false),
        StructField("ip", StringType, false),
        StructField("type", StringType, false))
    )
    val records = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp as TIMESTAMP)")
      .select($"timestamp", from_json($"value", eventScheme) as "event")
      .select("timestamp", "event.*")

    // #################
    //NActs by ip
    val resNActs = records
      .groupBy($"ip", window($"timestamp", "10 minutes"))
      .count()

    // #################
    // Count ip <-> type
    val countClickView = records
      .groupBy($"ip", $"type", window($"timestamp", "10 minutes"))
      .count()

    val resClicks = countClickView
      .where(countClickView("type") === CLICK_TYPE)
      .select($"ip", $"count", $"window")
      .withColumnRenamed("count", "clicks")
      .withWatermark("window", "30 seconds")

    val resViews = countClickView
      .where(countClickView("type") === VIEW_TYPE)
      .select($"ip", $"count", $"window")
      .withColumnRenamed("count", "views")
      .withWatermark("window", "30 seconds")

    val resOverClickView = resClicks.join(resViews)

    // #################
    // Count by category
    val resCat = records
      .select($"ip", $"category_id", window($"timestamp", "10 minutes"))
      .distinct()
      .groupBy($"ip")
      .count()

    val query = countClickView.writeStream
      .outputMode("complete")
      .format("console")
      .option("numRows", PRINT_LIMIT)
      .start()

    query.awaitTermination()
  }

  def filterByNActs(pair: (String, Int)): Boolean = {
    val limitActs = 1000
    pair._2 > limitActs
  }

  def filterByViewAct(pair: ((String, String), Int)): Boolean = {
    pair._1._2 == VIEW_TYPE
  }

  def filterByClickAct(pair: ((String, String), Int)): Boolean = {
    pair._1._2 == CLICK_TYPE
  }
}

