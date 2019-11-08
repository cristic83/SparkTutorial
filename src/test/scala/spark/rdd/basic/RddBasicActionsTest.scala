package spark.rdd.basic

import org.apache.spark.rdd.RDD
import spark.rdd.AbstractIntegrationTest

class RddBasicActionsTest extends AbstractIntegrationTest {
  val rddBasicActions = RddBasicActions()

  describe("RDD Operations Tests") {

    it("average should correctly compute the average of temperatures") {
      val temperatures: RDD[Double] = sc.parallelize(List(18.5, 20 ,31.5, 24))
      val avg = rddBasicActions.average(temperatures)
      avg should be (23.5)
    }

    it("howManyLines should return the number of lines of texts in the file") {
      val logLines: RDD[String] = sc.textFile("src/test/resources/log.txt")
      val count = rddBasicActions.howManyLines(logLines)
      count should === ( 38)
    }

    it("getFirstLines should return the first lines of text from the input") {
      val logLines: RDD[String] = sc.parallelize(List("first", "second", "third"))
      val lines = rddBasicActions.getFirstLines(logLines)
      lines should contain only ( "first", "second")
    }

    it("getFirstLinesComparedByLength should return the first lines of text from the input") {
      val logLines: RDD[String] = sc.parallelize(List("1", "10", "100"))
      val lines = rddBasicActions.getFirstLinesComparedByLength(logLines)
      lines should contain only ( "100", "10")
    }

    it("joinWordsToSentence should return the sentence from the input") {
      val words: RDD[String] = sc.parallelize(List("The", "first", "Spark", "job"))
      val sentence = rddBasicActions.joinWordsToSentence(words)
      sentence should ===  ("The first Spark job")
    }
  }
}
