package spark.streaming.stateless.solution

import org.apache.spark.streaming.dstream.DStream
import spark.streaming.stateless.DStreamStatelessTransformations

class DStreamStatelessTransformationsSolution extends DStreamStatelessTransformations {
  override def toUppercase(strings: DStream[String]): DStream[String] = strings.map( s=> s.toUpperCase)
}
