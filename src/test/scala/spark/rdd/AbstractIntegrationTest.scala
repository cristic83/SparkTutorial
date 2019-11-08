package spark.rdd

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}

abstract class AbstractIntegrationTest extends FunSpec with Matchers with BeforeAndAfterEach with LazyLogging {
  val sc = new SparkContext("local", "BasicAvg", System.getenv("SPARK_HOME"))
}