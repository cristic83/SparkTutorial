package spark.rdd.paired.impl

import org.apache.spark.rdd.RDD
import spark.rdd.paired.RddPairedTransformations

class RddPairedTransformationsExercise extends RddPairedTransformations {
  override def countHowManyTimesStringsOccurInText(wordOccurrences: RDD[(String, Int)]): RDD[(String, Int)] = ???

  override def getArrayOfValuesPerKey(wordOccurrences: RDD[(String, Int)]): RDD[(String, Iterable[Int])] = ???

  override def computeTotalValueAndAppearanceCountPerKey(wordOccurrences: RDD[(String, Int)]): RDD[(String, (Int, Int))] = ???

  override def computeAveragePerKey(wordOccurrences: RDD[(String, Int)]): RDD[(String, Double)] = ???

  override def reserveSeats(wordOccurrences: RDD[(String, (Int, Int))]): RDD[(String, String)] = ???
}
