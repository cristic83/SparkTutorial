package spark.streaming.stateful

import java.util.function.Consumer

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.time.{Seconds, Span}
import spark.streaming.AbstractIntegrationTest

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.reflect.io.Path
import scala.util.Try

class DStreamStateTransformationsTests extends AbstractIntegrationTest {

  private val dstreamTransformations = DStreamStatefulTransformations()

  private val resultsDir = "target/results"
  private val checkpointDir = "target/checkpoint"

  override def beforeEach(): Unit = {
    super.beforeEach()
    ssc.checkpoint(checkpointDir)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    Try(Path(checkpointDir).deleteRecursively)
    Try(Path(resultsDir).deleteRecursively)
  }
  describe("RDD Operations Tests") {

    it("getLastVisitedPagesByUserSorted should list the last 3 pages visited by a user, sorted alphabetically") {
      //given
      val batches: mutable.Queue[RDD[(String, String)]] = mutable.Queue[RDD[(String, String)]]()
      val dstream: InputDStream[(String, String)] = ssc.queueStream(batches)

      val outputDir = resultsDir + "/batch"


      //operation under test
      val visitedPagesByUser = dstreamTransformations.getLastVisitedPagesByUser(dstream, 3)

      visitedPagesByUser.mapValues(pages => pages.sorted).saveAsTextFiles(outputDir)

      ssc.start()

      //when

      batches += ssc.sparkContext.makeRDD(Seq(
        ("user1", "Home"),
        ("user1", "Contact"),
        ("user2", "About"),
        ("user2", "Donate"),
        ("user3", "FAQ"),
        ("user4", "Terms and conditions")))

      clock.advance(batchDuration.milliseconds)

      batches += ssc.sparkContext.makeRDD(Seq(
        ("user1", "About"),
        ("user1", "Donate"),
        ("user2", "Home"),
        ("user5", "FAQ")))

      clock.advance(batchDuration.milliseconds)

      //then
      eventually(timeout(Span(2, Seconds))){

        checkResults(outputDir, "-1000", resultsRDD => {
          resultsRDD.count() should be(4)
          resultsRDD.collect() should contain allOf(
            "(user1,Vector(Contact, Home))",
            "(user2,Vector(About, Donate))",
            "(user3,Vector(FAQ))",
            "(user4,Vector(Terms and conditions))")})

        checkResults(outputDir, "-2000", resultsRDD => {
          resultsRDD.count() should be(5)
          resultsRDD.collect() should contain allOf(
            "(user1,Vector(About, Donate, Home))",
            "(user2,Vector(About, Donate, Home))",
            "(user3,Vector(FAQ))",
            "(user4,Vector(Terms and conditions))",
            "(user5,Vector(FAQ))")})
      }
    }

    it("countVotesCastEvery3Hours should return how many votes were cast during a 3 hours time frame") {
      //given
      val batches: mutable.Queue[RDD[Long]] = mutable.Queue[RDD[Long]]()
      val dstream: InputDStream[Long] = ssc.queueStream(batches)

      val outputDir = resultsDir + "/batch"
      clock.setTime(7000); //voting section opens at 7:00, which in our test in at timestamp 7000.

      //operation under test
      val votesPeTimeFrame = dstreamTransformations.countVotesCastEvery3Hours(dstream)

      votesPeTimeFrame.saveAsTextFiles(outputDir)

      ssc.start()

      //Votes cast at times shown below...
      /*
      Vote: List(700, 701)
      Vote: List(800, 801, 802)
      Vote: List(900, 901)
      Vote: List(1000, 1001, 1002, 1003)
      Vote: List(1100, 1101, 1102, 1103, 1104)
      Vote: List(1200, 1201, 1202, 1203, 1204, 1205)
      Vote: List(1300, 1301, 1302, 1303, 1304, 1305, 1306, 1307, 1308, 1309)
      Vote: List(1400, 1401, 1402, 1403, 1404, 1405, 1406, 1407, 1408, 1409, 1410, 1411, 1412, 1413)
      Vote: List(1500, 1501, 1502, 1503, 1504, 1505, 1506, 1507, 1508, 1509, 1510, 1511, 1512, 1513, 1514, 1515, 1516, 1517, 1518, 1519)
      Vote: List(1600, 1601, 1602, 1603, 1604, 1605, 1606, 1607, 1608, 1609, 1610, 1611, 1612, 1613, 1614, 1615, 1616)
      Vote: List(1700, 1701, 1702, 1703, 1704, 1705, 1706, 1707, 1708, 1709, 1710, 1711, 1712, 1713, 1714, 1715, 1716, 1717)
      Vote: List(1800, 1801, 1802, 1803, 1804, 1805, 1806, 1807, 1808, 1809, 1810, 1811, 1812, 1813, 1814, 1815)
      Vote: List(1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909)
      Vote: List(2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007)
      Vote: List(2100, 2101, 2102, 2103, 2104, 2105)
         */
      val expectedVotesPerHour = TreeMap[Int, Int](
        700 -> 2,
        800 -> 3,
        900 -> 2,
        1000 -> 4,
        1100 -> 5,
        1200 -> 6,
        1300 -> 10,
        1400 -> 14,
        1500 -> 20,
        1600 -> 17,
        1700 -> 18,
        1800 -> 16,
        1900 -> 10,
        2000 -> 8,
        2100 -> 6
      )
      expectedVotesPerHour.foreach(hourAndVotes => {
        val votesToCast = hourAndVotes._2
        var votes = Seq[Long]()
        Range(0, votesToCast).foreach(vCount => {
          val vote: Long = hourAndVotes._1 + vCount
          votes = votes :+ vote
        })
        batches += ssc.sparkContext.makeRDD(votes)
        /* Another hour has past, we should process the votes.
        *  One second in our mighty test is one hour in real life. so we process the votes cast in one hour in 1 second.
        * */
        clock.advance(batchDuration.milliseconds)
      })

      //then
      eventually(timeout(Span(2, Seconds))){
        //at 10:00 we check the results for the interval [07:00 - 10:00)
        checkResults(outputDir, "-10000", resultsRDD => {
          resultsRDD.count() should be(1)
          resultsRDD.collect() should contain ("(700,7)")
        })

        checkResults(outputDir, "-13000", resultsRDD => {
          resultsRDD.count() should be(1)
          resultsRDD.collect() should contain ("(1000,15)")
        })

        checkResults(outputDir, "-16000", resultsRDD => {
          resultsRDD.count() should be(1)
          resultsRDD.collect() should contain ("(1300,44)")
        })

        checkResults(outputDir, "-19000", resultsRDD => {
          resultsRDD.count() should be(1)
          resultsRDD.collect() should contain ("(1600,51)")
        })

        checkResults(outputDir, "-22000", resultsRDD => {
          resultsRDD.count() should be(1)
          resultsRDD.collect() should contain ("(1900,24)")
        })
      }
    }
  }


  private def checkResults(outputDir: String, batchTime: String, check: Consumer[RDD[String]]) = {
    val fileName = outputDir + batchTime
    val resultsRDD: RDD[String] = ssc.sparkContext.textFile(fileName)
    check.accept(resultsRDD)
  }
}
