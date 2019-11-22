package spark.streaming.stateful

import org.apache.spark.streaming.dstream.DStream
import spark.streaming.stateful.impl.DStreamStatefulTransformationsExercise
import spark.streaming.stateful.solution.DStreamStatefulTransformationsSolution

trait DStreamStatefulTransformations {
  def getLastVisitedPagesByUser(logs: DStream[(String, String)], pagesTokKeep: Int): DStream[(String, Vector[String])]
  def countVotesCastEvery3Hours(votes: DStream[Long]): DStream[(String, Int)]
}


object DStreamStatefulTransformations {
  def apply(): DStreamStatefulTransformations = new DStreamStatefulTransformationsExercise()
}


