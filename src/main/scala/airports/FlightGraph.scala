package airports

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.rdd.RDD

case class FlightAttr(distance: Int, time: Double, count: Int = 1) {
  def ++(other: FlightAttr) = copy(time = time + other.time, count = count + other.count)
  def withAverageTime = copy(time = time / count, count = 1)
}

object FlightGraph {
  val ORIGIN = 16
  val DESTINATION = 17
  val DISTANCE = 18
  val ACTUAL_ELAPSED_TIME = 11
  val SCHEDULED_ELAPSED_TIME = 12

  def buildFrom(csvFile: String)(implicit sc: SparkContext): Graph[String, FlightAttr] = {
    val data = dataOf(csvFile)
    Graph(airports(data), flights(data).filter(_.attr.time > 0))
      .partitionBy(EdgePartition2D)
      .groupEdges(_ ++ _)
      .mapEdges(_.attr.withAverageTime)
      .cache()
  }

  def dataOf(csvFile: String)(implicit sc: SparkContext): RDD[Array[String]] = {
    val file = sc.textFile(csvFile)
    val header = file.first()
    file.filter(_ != header).map(_.split(","))
  }

  def airports(data: RDD[Array[String]]): VertexRDD[String] =
    VertexRDD(data flatMap { line =>
      val orig = line(ORIGIN).toString
      val dest = line(DESTINATION).toString

      List(
        (orig.hashCode, orig),
        (dest.hashCode, dest)
      )
    })

  def flights(data: RDD[Array[String]]): EdgeRDD[FlightAttr] =
    EdgeRDD fromEdges {
      data map { line =>
        val distance = line(DISTANCE).toInt
        val timeCandidates = List(
          line(ACTUAL_ELAPSED_TIME).toString,
          line(SCHEDULED_ELAPSED_TIME).toString
        )
        val elapsedTime = timeCandidates
          .find(_ forall Character.isDigit)
          .map(_.toDouble).getOrElse(-1.0)

        Edge(
          line(ORIGIN).toString.hashCode,
          line(DESTINATION).toString.hashCode,
          FlightAttr(distance, elapsedTime)
        )
      }
    }

  def flightsFrom(graph: Graph[String, FlightAttr], airport: String): Graph[String, FlightAttr] =
    graph.subgraph(epred = (_.srcAttr == airport))
}
