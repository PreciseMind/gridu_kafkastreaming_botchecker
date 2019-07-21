package BotChecker.sstream.apps

import BotChecker.sstream.apps.NActsBotDetector.{CassandraKeySpace, CassandraTtl}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions.{from_json, from_unixtime, window}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object OverCategoryBotDetector {


  val IpField = "ip"
  val CategoryField = "category_id"
  val TypeField = "type"
  val UnixTimeField = "unix_time"

  val ViewType = "view"
  val ClickType = "click"

  val WindowDur = "10 minutes"

  val ClicksLimit = 1000
  val CategoryLimit = 5
  val OverClicksAndViewsLimit = 5

  val CassandraKeySpace = "keySpace0"
  val CassandraTable = "blockedips"
  val CassandraTtl = 60

  val PrintLimit = 1000

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SSBotChecker")
      .set("spark.cassandra.connection.keep_alive_ms", "630000")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val connector = CassandraConnector.apply(sparkSession.sparkContext)

    import sparkSession.implicits._

    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test_stream4")
      .option("startingOffsets", "earliest")
      .load()

    val eventScheme = StructType(
      List(
        StructField(UnixTimeField, StringType, false),
        StructField(CategoryField, StringType, false),
        StructField(IpField, StringType, false),
        StructField(TypeField, StringType, false))
    )
    val records = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(from_json($"value", eventScheme) as "event")
      .select("event.*")
      .select($"ip", $"category_id", $"type", from_unixtime($"unix_time") as "timestamp")

    // #################
    // Count by category
    val resCat = detectBotsByCategory(records, sparkSession)

    val query = resCat.writeStream
      .outputMode("complete")
      .foreach(new ForeachWriter[Row] {
        override def open(partitionId: Long, epochId: Long): Boolean = true

        override def process(value: Row): Unit = {
          connector.withSessionDo(session => {
            val query = s"INSERT INTO $CassandraKeySpace.$CassandraTable (ip) VALUES ('${value.getAs[String]("ip")}') USING TTL $CassandraTtl"
            session.execute(query)
          })
        }

        override def close(errorOrNull: Throwable): Unit = {
        }
      })
      .start()

    query.awaitTermination()
  }

  def detectBotsByCategory(events: DataFrame, sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._

    events
      .select($"ip", $"category_id", window($"timestamp", WindowDur))
      .distinct()
      .groupBy($"ip")
      .count()
      .filter($"count">CategoryLimit)
      .select($"ip")
  }
}
