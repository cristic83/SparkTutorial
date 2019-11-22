package spark.streaming.stateless

import java.util.function.Consumer

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.time.{Seconds, Span}
import spark.streaming.AbstractIntegrationTest

import scala.collection.mutable
import scala.reflect.io.Path
import scala.util.Try

class DStreamStatelessTransformationsTests extends AbstractIntegrationTest {

  private val dstreamTransformations = DStreamStatelessTransformations()

  describe("RDD Operations Tests") {

    it("toUppercase should uppercase all its input") {
      //given
      val batches: mutable.Queue[RDD[String]] = mutable.Queue[RDD[String]]()
      val dstream: InputDStream[String] = ssc.queueStream(batches)

      val outputDir = "target/testfiles"

      //operation under test
      val toUpperDStream = dstreamTransformations.toUppercase(dstream)

      toUpperDStream.saveAsTextFiles(outputDir, "txt")

      ssc.start()

      //when

      batches += ssc.sparkContext.makeRDD(Seq("b", "c"))

      clock.advance(batchDuration.milliseconds)

      batches += ssc.sparkContext.makeRDD(Seq("d", "e"))

      clock.advance(batchDuration.milliseconds)
      //then
      eventually(timeout(Span(2, Seconds))){
        checkResults(outputDir, "-1000", resultsRDD => {
          resultsRDD.count() should be(2)
          resultsRDD.collect() should contain allOf("B", "C")})
        checkResults(outputDir, "-2000", resultsRDD => {
          resultsRDD.count() should be(2)
          resultsRDD.collect() should contain allOf("D", "E")})
      }
    }
  }

  private def checkResults(outputDir: String, batchTime: String, check: Consumer[RDD[String]]) = {
    val fileName = outputDir + batchTime + ".txt"
    val resultsRDD: RDD[String] = ssc.sparkContext.textFile(fileName)
    check.accept(resultsRDD)
    Try(Path(fileName).deleteRecursively)
  }
}
