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

    it("average should correctly compute the average of temperatures") {
      //given
      val lines: mutable.Queue[RDD[String]] = mutable.Queue[RDD[String]]()
      val dstream: InputDStream[String] = ssc.queueStream(lines)

      val outputDir = "target/testfiles"

      val toUpperDStream = dstreamTransformations.toUppercase(dstream)

      toUpperDStream.saveAsTextFiles(outputDir, "txt")

      ssc.start()

      //when

      lines += ssc.sparkContext.makeRDD(Seq("b", "c"))

      clock.advance(batchDuration.milliseconds)

      //then
      eventually(timeout(Span(2, Seconds))){
        val fileName = outputDir + "-" + batchDuration.milliseconds + ".txt"
        val wFile: RDD[String] = ssc.sparkContext.textFile(fileName)
        wFile.count() should be (2)
        wFile.collect().foreach(println)
        Try(Path(fileName + "-1000").deleteRecursively)
      }
    }
  }
}
