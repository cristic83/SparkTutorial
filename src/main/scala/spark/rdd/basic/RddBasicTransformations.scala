package spark.rdd.basic

import org.apache.spark.rdd.RDD
import spark.rdd.basic.impl.RddBasicTransformationsExercise
import spark.rdd.basic.solution.RddBasicTransformationsSolution

trait RddBasicTransformations {

  def buildShirtCollection(sizes: RDD[String], colors: RDD[String]): RDD[(String, String)]

  def toWordsDuplicatesRemoved(logLines: RDD[String]): RDD[String]

  def toWords(logLines: RDD[String]): RDD[String]

  def toFahrenheit(temperatures: RDD[Double]): RDD[Double]

  def findApiEvents(logLines: RDD[String]): RDD[String]
}

object RddBasicTransformations {
  def apply(): RddBasicTransformations = new RddBasicTransformationsExercise()
}