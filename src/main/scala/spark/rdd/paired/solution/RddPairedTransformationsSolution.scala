package spark.rdd.paired.solution

import org.apache.spark.rdd.RDD
import spark.rdd.paired.RddPairedTransformations

class RddPairedTransformationsSolution extends RddPairedTransformations {
  override def countHowManyTimesStringsOccurInText(wordOccurrences: RDD[(String, Int)]): RDD[(String, Int)] =
    wordOccurrences.reduceByKey((count1, count2) => count1 + count2)

  override def getArrayOfValuesPerKey(wordOccurrences: RDD[(String, Int)]): RDD[(String, Iterable[Int])] = wordOccurrences.groupByKey()

  override def computeTotalValueAndAppearanceCountPerKey(wordOccurrences: RDD[(String, Int)]): RDD[(String, (Int, Int))] = {
    wordOccurrences.combineByKey(value => (value, 1),
      (acc: (Int, Int), value) => (acc._1 + value, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
  }

  override def computeAveragePerKey(wordOccurrences: RDD[(String, Int)]): RDD[(String, Double)] =
    computeTotalValueAndAppearanceCountPerKey(wordOccurrences).mapValues(acc => acc._1 / acc._2.toDouble)

  override def reserveSeats(wordOccurrences: RDD[(String, (Int, Int))]): RDD[(String, String)] = wordOccurrences
    .flatMapValues(value => value._1 * 10 + 1 to  value._1 * 10 + value._2)
    .groupByKey().mapValues(iterable => iterable.mkString(" "))

  override def reserveSeatsInOrder(seatsByTeam: RDD[(String, (Int, Int))]): RDD[(String, String)] =
    reserveSeats(seatsByTeam).sortByKey()
}
