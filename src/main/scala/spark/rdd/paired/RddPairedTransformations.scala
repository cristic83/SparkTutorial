package spark.rdd.paired

import org.apache.spark.rdd.RDD
import spark.rdd.paired.solution.RddPairedTransformationsSolution

trait RddPairedTransformations {
  def reserveSeats(wordOccurrences: RDD[(String, (Int, Int))]): RDD[(String, String)]

  def computeAveragePerKey(wordOccurrences: RDD[(String, Int)]): RDD[(String, Double)]

  def computeTotalValueAndAppearanceCountPerKey(wordOccurrences: RDD[(String, Int)]): RDD[(String, (Int, Int))]

  def getArrayOfValuesPerKey(wordOccurrences: RDD[(String, Int)]): RDD[(String, Iterable[Int])]

  def countHowManyTimesStringsOccurInText(wordOccurrences: RDD[(String, Int)]): RDD[(String, Int)]
}

object RddPairedTransformations {
  def apply(): RddPairedTransformations = new RddPairedTransformationsSolution()
}