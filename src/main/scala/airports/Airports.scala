package airports

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

object Airports {
  import FlightGraph._

  trait NearestAirports {
    def fromAirport(airport: String, limit: Int = 5): List[String]
  }

  class NearestAirportsByDistance(graph: Graph[String, FlightAttr]) extends NearestAirports {
    def fromAirport(airport: String, limit: Int = 5):  List[String] =
      flightsFrom(graph, airport).triplets
        .sortBy(_.attr.distance)
        .take(limit)
        .map(triplet => s"${triplet.dstAttr}: ${triplet.attr.distance} miles")
        .toList
  }

  class NearestAirportsByAvgFlightTime(graph: Graph[String, FlightAttr]) extends NearestAirports {
    def fromAirport(airport: String, limit: Int = 5): List[String] =
      flightsFrom(graph, airport).triplets
        .sortBy(_.attr.time)
        .take(limit)
        .map(triplet => s"${triplet.dstAttr}: ${triplet.attr.time} minutes in average")
        .toList
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Airports")
      .setMaster("local[*]")

    implicit val sc = new SparkContext(conf)

    try {
      val graph = buildFrom("2008.csv")

      val calcByDistance = new NearestAirportsByDistance(graph)
      val calcByAvgFlightTime = new NearestAirportsByAvgFlightTime(graph)

      println("ATL TOP5 nearest airports")
      println(calcByDistance.fromAirport("ATL"))
      println(calcByAvgFlightTime.fromAirport("ATL"))

      println("EWR TOP10 nearest airports")
      println(calcByDistance.fromAirport("EWR", 10))
      println(calcByAvgFlightTime.fromAirport("EWR", 10))
    } finally {
      sc.stop()
    }
  }
}
