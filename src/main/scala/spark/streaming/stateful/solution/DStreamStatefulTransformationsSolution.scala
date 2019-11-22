package spark.streaming.stateful.solution

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import spark.streaming.stateful.DStreamStatefulTransformations

class DStreamStatefulTransformationsSolution extends DStreamStatefulTransformations {
  override def getLastVisitedPagesByUser(logs: DStream[(String, String)], pagesTokKeep: Int): DStream[(String, Vector[String])] = {
    logs.updateStateByKey[Vector[String]]((logEvents: Seq[String], visits: Option[Vector[String]])  => {
      if (visits.isEmpty) Option(logEvents.take(pagesTokKeep).toVector)
      else Option((logEvents.toVector ++ visits.get).take(pagesTokKeep))
    })
  }

  override def countVotesCastEvery3Hours(votes: DStream[Long]): DStream[(String, Int)] = {
    votes.map(vote => (vote - vote % 100, 1 ))
      .reduceByWindow((x, y) => (if (x._1 < y._1) x._1 else y._1, x._2 + y._2), Seconds(3), Seconds(3))
      .map(x => (x._1.toString, x._2))
  }
}
