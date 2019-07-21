import BotChecker.sstream.apps.{NActsBotDetector, OverCategoryBotDetector, OverClicksAndViewBotDetector}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec}

class BotCheckerSSTest extends FunSpec with
  BeforeAndAfterEach with BeforeAndAfterAll {

  val AppName = "BotCheckerTestApp"
  val Master = "local[2]"
  val IpField = "ip"
  val CategoryField = "category_id"
  val TypeField = "type"
  val UnixTimeField = "unix_time"

  val EventScheme = StructType(
    List(
      StructField(UnixTimeField, StringType, false),
      StructField(CategoryField, StringType, false),
      StructField(IpField, StringType, false),
      StructField(TypeField, StringType, false)))

  @transient var ss: SparkSession = null

  override def beforeAll(): Unit = {

    val sparkConfig = new SparkConf()
      .setAppName(AppName)
      .setMaster(Master)

    ss = SparkSession.builder()
      .config(sparkConfig)
      .enableHiveSupport()
      .getOrCreate()
  }

  describe("SS testing") {

    it("[SS] NActsBotDetector") {

      val sparkSession = ss

      val input = extractTestDataFrame()

      val expectedValue = 1L

      val result = NActsBotDetector.detectBotsByNActs(input, sparkSession)

      assertResult(expectedValue)(result.count())
    }

    it("[SS] OverCategoryBotDetector") {

      val sparkSession = ss

      val input = extractTestDataFrame()

      val expectedValue = 1L

      val result = OverCategoryBotDetector.detectBotsByCategory(input, sparkSession)

      assertResult(expectedValue)(result.count())
    }

    it("[SS] OverClicksAndViewBotDetector") {

      val sparkSession = ss

      val input = extractTestDataFrame()

      val expectedValue = 1L

      val result = OverClicksAndViewBotDetector.detectBotsByClickAndView(input, sparkSession)

      assertResult(expectedValue)(result.count())
    }
  }

  def generateTestSeq(): Seq[String] = {

    val clicks = for (i <- 1 to 1000) yield additionalClick
    data.split(System.lineSeparator()).toSeq.++(clicks)
  }

  def generateTestDataFrame(): DataFrame = {

    val sparkSession = ss
    import sparkSession.implicits._

    generateTestSeq().toDF()
  }

  def extractTestDataFrame(): DataFrame = {

    val sparkSession = ss
    import sparkSession.implicits._

    generateTestDataFrame().select(from_json($"value", EventScheme) as "event")
      .select("event.*")
      .select($"ip", $"category_id", $"type", from_unixtime($"unix_time") as "timestamp")
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
