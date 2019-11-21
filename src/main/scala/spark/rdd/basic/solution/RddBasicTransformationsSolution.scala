package spark.rdd.basic.solution

import org.apache.spark.rdd.RDD
import spark.rdd.basic.RddBasicTransformations

class RddBasicTransformationsSolution extends RddBasicTransformations {
  override def buildShirtCollection(sizes: RDD[String], colors: RDD[String]): RDD[(String, String)] = sizes.cartesian(colors)

  /**
   * https://rxmarbles.com/#distinct
   * @param logLines
   * @return
   */
  override def toWordsDuplicatesRemoved(logLines: RDD[String]): RDD[String] = toWords(logLines).distinct()

  /**
   * http://reactivex.io/documentation/operators/flatmap.html
   * @param logLines
   * @return
   */
  override def toWords(logLines: RDD[String]): RDD[String] = logLines.flatMap(line => line.split(" "))

  /**
   * https://rxmarbles.com/#map
   * @param temperatures
   * @return
   */
  override def toFahrenheit(temperatures: RDD[Double]): RDD[Double] = temperatures.map(t => t * 9/5 + 32)

  /**
   * https://rxmarbles.com/#filter
   * @param logLines
   * @return
   */
  override def findApiEvents(logLines: RDD[String]): RDD[String] = logLines.filter(line => line.contains("[ApiEvent]"))
}

object RddBasicTransformationsSolution {
  def apply(): RddBasicTransformationsSolution = new RddBasicTransformationsSolution()
}