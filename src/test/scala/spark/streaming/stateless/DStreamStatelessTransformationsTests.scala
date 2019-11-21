package spark.streaming.stateless

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

      //then
      eventually(timeout(Span(2, Seconds))){
        val fileName = outputDir + "-" + batchDuration.milliseconds + ".txt"
        val wFile: RDD[String] = ssc.sparkContext.textFile(fileName)
        wFile.count() should be (2)
        wFile.collect().foreach(println)
        Try(Path(fileName).deleteRecursively)
      }
    }
  }
}
