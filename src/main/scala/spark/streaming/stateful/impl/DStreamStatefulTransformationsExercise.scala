package spark.streaming.stateful.impl

import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import spark.streaming.stateful

class DStreamStatefulTransformationsExercise extends stateful.DStreamStatefulTransformations {
  override def getLastVisitedPagesByUser(strings: DStream[(String, String)], pagesTokKeep: Int): DStream[(String, Vector[String])] = ???

  override def countVotesCastEvery3Hours(votes: DStream[Long]): DStream[(String, Int)] = ???

  override def computeSumOfKConsecutiveElements(dstream: InputDStream[Long], length: Int): DStream[Long] = ???
}
