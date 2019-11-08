package spark.rdd.basic

import org.apache.spark.rdd.RDD
import spark.rdd.AbstractUnitTest

class RddBasicTransformationsTest extends AbstractUnitTest {
  val rddBasicTransformations = RddBasicTransformations()
  describe("RDD Operations Tests") {

    it("toFahrenheit should correctly convert Celsius to Fahrenheit") {
      val temperatures: RDD[Double] = sc.parallelize(List(18.5, 20 ,31.5, 24))
      val fahrenheitTemperatures = rddBasicTransformations.toFahrenheit(temperatures).collect()
      fahrenheitTemperatures should be (Array(65.3, 68.0, 88.7, 75.2))
    }

    it("findApiEvents should return all lines of texts containing [ApiEvent]") {
      val logLines: RDD[String] = sc.textFile("src/test/resources/log.txt")
      val apiEvents = rddBasicTransformations.findApiEvents(logLines).collect()
      apiEvents should have length 13
      apiEvents.foreach(event =>  event.contains("[ApiEvent]"))
    }

    it("toWords should correctly split the lines of texts to words") {
      val logLines: RDD[String] = sc.parallelize(List("The first sentence", "The second sentence"))
      val words = rddBasicTransformations.toWords(logLines).collect()
      words should be (Array("The", "first", "sentence", "The", "second", "sentence"))
    }

    it("toWordsDuplicatesRemoved should correctly split the lines of texts to words, removing duplicates") {
      val logLines: RDD[String] = sc.parallelize(List("The first sentence", "The second sentence"))
      val words = rddBasicTransformations.toWordsDuplicatesRemoved(logLines).collect()
      words should contain only ("The", "first", "sentence", "second")
    }

    it("buildShirtCollection should correctly generate color/size pairs") {
      val sizes: RDD[String] = sc.parallelize(List("S", "M", "L"))
      val colors: RDD[String] = sc.parallelize(List("RED", "ORANGE", "YELLOW"))
      val words = rddBasicTransformations.buildShirtCollection(sizes, colors).collect()
      words should contain only (
        ("S", "RED"), ("S", "ORANGE"), ("S", "YELLOW"),
        ("M", "RED"), ("M", "ORANGE"), ("M", "YELLOW"),
        ("L", "RED"), ("L", "ORANGE"), ("L", "YELLOW")
      )
    }
  }
}
