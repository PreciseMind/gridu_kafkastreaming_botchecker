package BotChecker

import BotChecker.data.Event
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AppDStream {

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

    val conf = new SparkConf().setMaster("local[2]").setAppName("DSBotChecker")
    val ssc = new StreamingContext(conf, Seconds(LATENCY_DUR))
    ssc.checkpoint(CHECKPOINT_DIRECTORY)

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
      (r.get(IP_FIELD).asText(), createEvent(r))
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
      .countByValueAndWindow(Seconds(WINDOW_DUR), Seconds(WINDOW_DUR))
      .map(k => k._1)
      .foreachRDD(
        rdd => {
          rdd.foreachPartition(
            partitionRDD => {
              //TODO upload into DB: Check existing IP into DB, insert IP if not exist
              partitionRDD.foreach(println)
            }
          )
        }
      )

    println("Start streaming")
    ssc.start()
    val stopped = ssc.awaitTerminationOrTimeout((WINDOW_DUR+TERMINATION_WAIT_TIME) * 1000)
    println(s"Finished streaming: ${stopped}")
  }

  def createEvent(record: JsonNode): Event = {
    new Event(record.get(TYPE_FIELD).asText(), record.get(IP_FIELD).asText(), record.get(CATEGORY_FIELD).asText(), record.get(UNIX_TIME_FIELD).asText())
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

  def detectBotsByNActs(events: DStream[(String, Event)]): DStream[String] = {

    val nActsStream = events
      .map(r => (r._1, 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => {
        a + b
      }, Seconds(WINDOW_DUR), Seconds(SLIDE_DUR))

    nActsStream.print(PRINT_LIMIT)

    nActsStream
      .filter(filterByNActs)
      .map(r => r._1)
  }

  def detectBotsByClickAndView(events: DStream[(String, Event)]): DStream[String] = {

    val ipTypeStream = events
      .map(r => ((r._1, r._2.kind), 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => {
        a + b
      }, Seconds(WINDOW_DUR), Seconds(SLIDE_DUR))

    val clickActsStream = ipTypeStream
      .filter(filterByClickAct).map(r => (r._1._1, (r._1._2, r._2)))

    val viewActsStream = ipTypeStream
      .filter(filterByViewAct).map(r => (r._1._1, (r._1._2, r._2)))

    val clickAndViewStream = clickActsStream
      .join(viewActsStream)
      .map(r => (r._1, r._2._1._2 / r._2._2._2))

    clickAndViewStream.print(PRINT_LIMIT)

    clickAndViewStream
      .filter(r => r._2 > 5)
      .map(r => r._1)
  }

  def detectBotsByCategory(events: DStream[(String, Event)]): DStream[String] = {

    val categories = events
      .map(r => ((r._1, r._2.category, r._2.kind), 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => {
        a + b
      }, Seconds(WINDOW_DUR), Seconds(SLIDE_DUR))
      .filter(r => r._1._3 == VIEW_TYPE)
      .map(r => r._1._1)
      .countByValue()

    categories.print(PRINT_LIMIT)

    categories
      .filter(r => r._2 > 5)
      .map(r => r._1)
  }
}
