package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

/** Computes an approximation to pi */
object GraphX {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[4]").setAppName("GraphX")
    val sc = new SparkContext(conf)
    
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String, Int))] =
      sc.parallelize(Array((3L, ("rxin", "student", 3)), (7L, ("jgonzal", "postdoc", 7)),
                           (5L, ("franklin", "prof", 5)), (2L, ("istoica", "prof", 2))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                           Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing", 0)
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    
    // Count all users which are postdocs
    graph.vertices.filter { case (id, (name, pos, count)) => pos == "postdoc" }.count
    // Count all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId).count
    
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
    /**
     * rxin is the collab of jgonzal
     * franklin is the advisor of rxin
     * istoica is the colleague of franklin
     * franklin is the pi of jgonzal
     */
    
    // aggregateMessage test
    val aggregatedGraph: VertexRDD[Int] = graph.mapReduceTriplets[Int](
      triplet => { Iterator((triplet.dstId, triplet.srcAttr._3)) },
      (a, b) => (a + b)
    )
    aggregatedGraph.collect.foreach(println(_))
    /**
     * (5,2)
     * (3,5)
     * (7,8)
     */
    
    sc.stop()
  }
}
