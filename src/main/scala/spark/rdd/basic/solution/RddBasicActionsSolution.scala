package spark.rdd.basic.solution

import org.apache.spark.rdd.RDD
import spark.rdd.basic.RddBasicActions

class RddBasicActionsSolution extends RddBasicActions {

  override def joinWordsToSentence(words: RDD[String]): String = words.reduce((first, second) => first + " " + second)

  class StringSizeOrdering extends Ordering[String] {
    override def compare(x: String, y: String): Int = x.size compareTo  y.size
  }

  override def getFirstLinesComparedByLength(logLines: RDD[String]): Array[String] = logLines.top(2)(new StringSizeOrdering)

  override def getFirstLines(logLines: RDD[String]): Array[String] = logLines.take(2)

  override def howManyLines(logLines: RDD[String]): Long = logLines.count()

  override def average(temperatures: RDD[Double]) : Double = {
    val aggregate = temperatures.aggregate(0d, 0)((agg, currentElem) => (agg._1 + currentElem, agg._2 + 1), (agg1, agg2) => (agg1._1 + agg2._1, agg1._2 + agg2._2))
    aggregate._1/aggregate._2
  }

}

object RddBasicActionsSolution {
  def apply(): RddBasicActionsSolution = new RddBasicActionsSolution()
}

