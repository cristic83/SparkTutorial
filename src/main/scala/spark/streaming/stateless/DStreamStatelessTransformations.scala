package spark.streaming.stateless

import org.apache.spark.streaming.dstream.DStream
import spark.streaming.stateless.solution.DStreamStatelessTransformationsSolution

trait DStreamStatelessTransformations {
  def toUppercase(strings: DStream[String]): DStream[String]
}

object DStreamStatelessTransformations {
  def apply(): DStreamStatelessTransformations = new DStreamStatelessTransformationsSolution()
}
