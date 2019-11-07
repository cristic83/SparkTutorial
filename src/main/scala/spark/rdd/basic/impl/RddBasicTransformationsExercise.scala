package spark.rdd.basic.impl

import org.apache.spark.rdd.RDD
import spark.rdd.basic.RddBasicTransformations

class RddBasicTransformationsExercise extends RddBasicTransformations {
  override def buildShirtCollection(sizes: RDD[String], colors: RDD[String]): RDD[(String, String)] = ???

  override def toWordsDuplicatesRemoved(logLines: RDD[String]): RDD[String] = ???

  override def toWords(logLines: RDD[String]): RDD[String] = ???

  override def toFahrenheit(temperatures: RDD[Double]): RDD[Double] = ???

  override def findApiEvents(logLines: RDD[String]): RDD[String] = ???
}
