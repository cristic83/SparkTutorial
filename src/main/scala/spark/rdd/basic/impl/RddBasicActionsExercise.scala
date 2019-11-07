package spark.rdd.basic.impl

import org.apache.spark.rdd.RDD
import spark.rdd.basic.RddBasicActions

class RddBasicActionsExercise extends RddBasicActions {

  override def joinWordsToSentence(words: RDD[String]): String = ???

  override def getFirstLinesComparedByLength(logLines: RDD[String]): Array[String] = ???

  override def getFirstLines(logLines: RDD[String]): Array[String] = ???

  override def howManyLines(logLines: RDD[String]): Long = ???

  override def average(temperatures: RDD[Double]): Double = ???
}
