package spark.rdd.basic

import org.apache.spark.rdd.RDD
import spark.rdd.basic.solution.RddBasicActionsSolution

trait RddBasicActions {

  def joinWordsToSentence(words: RDD[String]): String

  def getFirstLinesComparedByLength(logLines: RDD[String]): Array[String]

  def getFirstLines(logLines: RDD[String]): Array[String]

  def howManyLines(logLines: RDD[String]): Long

  def average(temperatures: RDD[Double]): Double
}

object RddBasicActions {
  def apply(): RddBasicActions = new RddBasicActionsSolution()
}
