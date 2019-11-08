package spark.streaming

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ClockWrapper, Seconds, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}

abstract class AbstractIntegrationTest extends FunSpec with Matchers with BeforeAndAfterEach with Eventually with LazyLogging {

  private val master = "local[*]"
  private val appName = "spark-streaming-test"

  protected var ssc: StreamingContext = _

  protected val batchDuration = Seconds(1)

  var clock: ClockWrapper = _

  override def beforeEach(): Unit = {
    val conf = new SparkConf()
      .setMaster(master).setAppName(appName)
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
      .set("spark.driver.host", "localhost")

    ssc = new StreamingContext(conf, batchDuration)
    clock = new ClockWrapper(ssc)
  }

  override def afterEach(): Unit = {
    if (ssc != null) {
      ssc.stop()
    }
  }
}
