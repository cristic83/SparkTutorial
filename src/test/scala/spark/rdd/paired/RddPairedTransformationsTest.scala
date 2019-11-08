package spark.rdd.paired

import spark.AbstractUnitTest

class RddPairedTransformationsTest extends AbstractUnitTest {
  val rddBasicTransformations = RddPairedTransformations()
  describe("RDD Paired Operations Tests") {

    it("countHowManyTimesStringsOccurInText should return how many times each word appeared in text") {
      val wordOccurrences = sc.parallelize(List(
        ("even", 1), ("new", 1), ("want", 1), ("because", 1),
        ("new", 1), ("want", 1), ("because", 1),
        ("want", 1), ("because", 1),
        ("because", 1)))
      val counterMap = rddBasicTransformations.countHowManyTimesStringsOccurInText(wordOccurrences).collectAsMap()
      counterMap should have size 4
      counterMap("even") should be (1)
      counterMap("new") should be (2)
      counterMap("want") should be (3)
      counterMap("because") should be (4)
    }

    it("getArrayOfValuesPerKey should return all lines where word is present") {
      val wordOccurrences = sc.parallelize(List(
        ("even", 1), ("new", 1), ("want", 1), ("because", 1),
        ("new", 2), ("want", 2), ("because", 2),
        ("want", 3), ("because", 3),
        ("because", 4)))
      val arraysByKey = rddBasicTransformations.getArrayOfValuesPerKey(wordOccurrences).collectAsMap()
      arraysByKey should have size 4
      arraysByKey("even") should contain only (1)
      arraysByKey("new") should contain only (1, 2)
      arraysByKey("want") should contain only  (1, 2, 3)
      arraysByKey("because") should contain only  (1, 2, 3, 4)
    }

    it("computeTotalValueAndAppearanceCountPerKey should return average number of appearances per key") {
      val wordOccurrences = sc.parallelize(List(
        ("even", 1), ("new", 1), ("want", 1), ("because", 1),
        ("new", 2), ("want", 2), ("because", 2),
        ("want", 3), ("because", 3),
        ("because", 4)))
      val averageByKey = rddBasicTransformations.computeTotalValueAndAppearanceCountPerKey(wordOccurrences).collectAsMap()
      averageByKey should have size 4
      averageByKey("even") should === (1, 1)
      averageByKey("new") should === (3, 2)
      averageByKey("want") should ===  (6, 3)
      averageByKey("because") should ===  (10, 4)
    }

    it("computeAveragePerKey should return average number of appearances per key") {
      val wordOccurrences = sc.parallelize(List(
        ("even", 1), ("new", 1), ("want", 1), ("because", 1),
        ("new", 2), ("want", 2), ("because", 2),
        ("want", 3), ("because", 3),
        ("because", 4)))
      val averageByKey = rddBasicTransformations.computeAveragePerKey(wordOccurrences).collectAsMap()
      averageByKey should have size 4
      averageByKey("even") should === (1)
      averageByKey("new") should === (1.5)
      averageByKey("want") should ===  (2)
      averageByKey("because") should ===  (2.5)
    }

    it("reserveSeats should return reserved seats per team") {
      val seatsByTeam = sc.parallelize(List(
        ("steaua", (1,3)), ("craiova", (2, 2)), ("dinamo", (3,4))))
      val result = rddBasicTransformations.reserveSeats(seatsByTeam)
        .collectAsMap()
      result should have size (3)
      result("steaua") should === ("11 12 13")
      result("craiova") should === ("21 22")
      result("dinamo") should === ("31 32 33 34")
    }

    it("reserveSeatsInOrder should return reserved seats per team") {
      val seatsByTeam = sc.parallelize(List(
        ("steaua", (1,3)), ("craiova", (2, 2)), ("dinamo", (3,4))))
      val result = rddBasicTransformations.reserveSeatsInOrder(seatsByTeam)
        .keys.collect()

      result should contain inOrderOnly("craiova", "dinamo", "steaua")
    }

  }
}
