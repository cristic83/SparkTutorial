package spark.streaming.stateless.impl

import org.apache.spark.streaming.dstream.DStream
import spark.streaming.stateless.DStreamStatelessTransformations

class DStreamStatelessTransformationsExercise extends DStreamStatelessTransformations {
  override def toUppercase(strings: DStream[String]): DStream[String] = ???
}
