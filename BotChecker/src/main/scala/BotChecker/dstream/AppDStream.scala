package BotChecker.dstream

import BotChecker.data.Event
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AppDStream {

  val CheckpointDirectory = "/Users/avaniukov/Documents/Training/Streaming/BotGen/checkpoints"

  val IpField = "ip"
  val CategoryField = "category_id"
  val TypeField = "type"
  val UnixTimeField = "unix_time"

  val ViewType = "view"
  val ClickType = "click"

  val LatencyDur = 10
  val SlideDur = 30
  val WindowDur = 60
  val TerminationWaitTime = 5

  val ClicksLimit = 1000
  val CategoryLimit = 5
  val OverClicksAndViewsLimit = 5

  val CassandraKeySpace = "keyspace0"
  val CassandraTable = "blockedips"
  val CassandraTtl = 60

  val PrintLimit = 1000

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("DSBotChecker")
      .set("spark.cassandra.connection.keep_alive_ms", "630000")

    val ssc = new StreamingContext(conf, Seconds(LatencyDur))
    ssc.checkpoint(CheckpointDirectory)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "bot_checker",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test_stream4")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val parser: ObjectMapper = new ObjectMapper()

    val convertStream = stream.map(record => {
      val r: JsonNode = parser.readTree(record.value())
      (r.get(IpField).asText(), createEvent(r))
    })

    // #################
    //NActs by ip

    val resNActs = detectBotsByNActs(convertStream)

    // #################
    // Count ip <-> type
    val resOverClickView = detectBotsByClickAndView(convertStream)

    // #################
    // Count by category
    val resCat = detectBotsByCategory(convertStream)

    resNActs
      .union(resOverClickView)
      .union(resCat)
      .countByValueAndWindow(Seconds(WindowDur), Seconds(WindowDur))
      .map(k => (k._1, 0))
      .foreachRDD(
        rdd => {
          rdd.saveToCassandra(CassandraKeySpace, CassandraTable, SomeColumns("ip"), writeConf = WriteConf(ttl = TTLOption.constant(CassandraTtl)))
        }
      )

    println("Start streaming")
    ssc.start()
    val stopped = ssc.awaitTerminationOrTimeout(Seconds(WindowDur + TerminationWaitTime).milliseconds)
    println(s"Finished streaming: ${stopped}")
  }

  def createEvent(record: JsonNode): Event = {
    new Event(record.get(TypeField).asText(), record.get(IpField).asText(), record.get(CategoryField).asText(), record.get(UnixTimeField).asText())
  }

  def filterByNActs(pair: (String, Int)): Boolean = {
    pair._2 > ClicksLimit
  }

  def filterByViewAct(pair: ((String, String), Int)): Boolean = {
    pair._1._2 == ViewType
  }

  def filterByClickAct(pair: ((String, String), Int)): Boolean = {
    pair._1._2 == ClickType
  }

  def detectBotsByNActs(events: DStream[(String, Event)]): DStream[String] = {

    val nActsStream = events
      .map(r => (r._1, 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => {
        a + b
      }, Seconds(WindowDur), Seconds(SlideDur))

    nActsStream
      .filter(filterByNActs)
      .map(r => r._1)
  }

  def detectBotsByClickAndView(events: DStream[(String, Event)]): DStream[String] = {

    val ipTypeStream = events
      .map(r => ((r._1, r._2.kind), 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => {
        a + b
      }, Seconds(WindowDur), Seconds(SlideDur))

    val clickActsStream = ipTypeStream
      .filter(filterByClickAct).map(r => (r._1._1, (r._1._2, r._2)))

    val viewActsStream = ipTypeStream
      .filter(filterByViewAct).map(r => (r._1._1, (r._1._2, r._2)))

    val clickAndViewStream = clickActsStream
      .join(viewActsStream)
      .map(r => (r._1, r._2._1._2 / r._2._2._2))

    //clickAndViewStream.print(PrintLimit)

    clickAndViewStream
      .filter(r => r._2 > OverClicksAndViewsLimit)
      .map(r => r._1)
  }

  def detectBotsByCategory(events: DStream[(String, Event)]): DStream[String] = {

    val categories = events
      .map(r => ((r._1, r._2.category), 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => {
        a + b
      }, Seconds(WindowDur), Seconds(SlideDur))
      .map(r => r._1._1)
      .countByValue()

    categories.print(PrintLimit)

    categories
      .filter(r => r._2 > CategoryLimit)
      .map(r => r._1)
  }
}
