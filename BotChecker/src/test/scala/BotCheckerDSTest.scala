import BotChecker.data.Event
import BotChecker.dstream.AppDStream
import BotChecker.dstream.AppDStream.createEvent
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec}

import scala.collection.mutable

class BotCheckerDSTest extends FunSpec with
  BeforeAndAfterEach with BeforeAndAfterAll {

  val EventScheme = StructType(
    List(
      StructField(UnixTimeField, StringType, false),
      StructField(CategoryField, StringType, false),
      StructField(IpField, StringType, false),
      StructField(TypeField, StringType, false)))

  val AppName = "BotCheckerTestApp"
  val Master = "local[2]"
  val IpField = "ip"
  val CategoryField = "category_id"
  val TypeField = "type"
  val UnixTimeField = "unix_time"

  @transient var sc: SparkContext = null
  @transient var ss: SparkSession = null
  @transient var ssc: StreamingContext = null

  override def beforeAll(): Unit = {

    val sparkConfig = new SparkConf()
      .setAppName(AppName)
      .setMaster(Master)

    sc = new SparkContext(sparkConfig)

    ss = SparkSession.builder()
      .config(sparkConfig)
      .enableHiveSupport()
      .getOrCreate()
    ssc = new StreamingContext(sc, Milliseconds(1000))
  }

  describe("DStream testing") {

    it("[DStream] NActsBotDetector") {

      val input = extractTestDS()

      AppDStream.detectBotsByNActs(input)
        .count().foreachRDD(rdd => assertResult(1L)(rdd.count()))
    }
  }

  it("[DStream] OverCategoryBotDetector") {

    val input = extractTestDS()

    AppDStream.detectBotsByCategory(input)
      .count().foreachRDD(rdd => assertResult(1L)(rdd.count()))
  }

  it("[DStream] OverClicksAndViewBotDetector") {

    val input = extractTestDS()
    AppDStream.detectBotsByClickAndView(input)
      .count().foreachRDD(rdd => assertResult(1L)(rdd.count()))
  }

  def generateTestSeq(): Seq[String] = {

    val clicks = for (i <- 1 to 1000) yield additionalClick
    data.split(System.lineSeparator()).toSeq.++(clicks)
  }

  def extractTestDS(): DStream[(String, Event)] = {

    val testRDD = sc.makeRDD(generateTestSeq())
    val testQueue = mutable.Queue(testRDD)
    val stream = ssc.queueStream(testQueue)

    val ipField = IpField

    val parser: ObjectMapper = new ObjectMapper()
    val parsed = stream.map(record => {
      val r: JsonNode = parser.readTree(record)
      (r.get(ipField).asText(), createEvent(r))
    })
    parsed
  }

  val data =
    """{"unix_time": 1540559093, "category_id": 1000, "ip": "172.10.0.51", "type": "click"}
              {"unix_time": 1540559093, "category_id": 1000, "ip": "172.10.0.51", "type": "view"}
              {"unix_time": 1540559095, "category_id": 1001, "ip": "172.20.0.51", "type": "click"}
              {"unix_time": 1540559095, "category_id": 1002, "ip": "172.20.0.51", "type": "view"}
              {"unix_time": 1540559095, "category_id": 1003, "ip": "172.20.0.51", "type": "click"}
              {"unix_time": 1540559095, "category_id": 1004, "ip": "172.20.0.51", "type": "view"}
              {"unix_time": 1540559095, "category_id": 1005, "ip": "172.20.0.51", "type": "click"}
              {"unix_time": 1540559095, "category_id": 1006, "ip": "172.20.0.51", "type": "view"}
              {"unix_time": 1540559095, "category_id": 1007, "ip": "172.20.0.51", "type": "click"}
              {"unix_time": 1540559095, "category_id": 1008, "ip": "172.20.0.51", "type": "view"}"""

  val additionalClick = """{"unix_time": 1540559095, "category_id": 1007, "ip": "172.20.0.51", "type": "click"}"""
}
