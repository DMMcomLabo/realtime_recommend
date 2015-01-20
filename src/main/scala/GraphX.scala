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
    
    val nodes: RDD[(VertexId, (String, String, Double))] =
      sc.parallelize(Array(
          (10001L, ("word", "艦隊これくしょん", 0.8)),
          (10002L, ("word", "魔法少女まどか☆マギカ", 0.8)),
          (20001L, ("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）", 1.0)),
          (20002L, ("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱", 1.0)),
          (20003L, ("product", "第1話 艦隊これくしょん-艦これ-", 1.0)),
          (20004L, ("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）", 1.0)),
          (20005L, ("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語", 1.0)),
          (20006L, ("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語", 1.0)),
          (30001L, ("genre", "青年コミック", 1.0)),
          (30002L, ("genre", "2015年冬アニメ", 1.0)),
          (30003L, ("genre", "アクション", 1.0)),
          (30004L, ("genre", "2010年代", 1.0)),
          (30005L, ("genre", "テレビ", 1.0)),
          (30006L, ("genre", "魔法少女", 1.0)),
          (30007L, ("genre", "劇場版（アニメ映画）", 1.0)),
          (30008L, ("genre", "2010年代", 1.0)),
          (30009L, ("genre", "SF/ファンタジー", 1.0))
        ))
    val edges: RDD[Edge[(String, Double)]] =
      sc.parallelize(Array(
          (Edge(10001L, 20001L, ("search", 0.8))),
          (Edge(10001L, 20002L, ("search", 0.7))),
          (Edge(10001L, 20003L, ("search", 0.6))),
          (Edge(10001L, 20004L, ("search", 0.5))),
          (Edge(10002L, 20005L, ("search", 0.8))),
          (Edge(10002L, 20006L, ("search", 0.7))),
          (Edge(20001L, 30001L, ("attr", 0.7))),
          (Edge(20002L, 30001L, ("attr", 0.7))),
          (Edge(20003L, 30002L, ("attr", 0.7))),
          (Edge(20004L, 30002L, ("attr", 0.7))),
          (Edge(20003L, 30003L, ("attr", 0.7))),
          (Edge(20004L, 30003L, ("attr", 0.7))),
          (Edge(20003L, 30004L, ("attr", 0.7))),
          (Edge(20004L, 30004L, ("attr", 0.7))),
          (Edge(20005L, 30004L, ("attr", 0.7))),
          (Edge(20006L, 30004L, ("attr", 0.7))),
          (Edge(20003L, 30005L, ("attr", 0.7))),
          (Edge(20004L, 30005L, ("attr", 0.7))),
          (Edge(20005L, 30006L, ("attr", 0.7))),
          (Edge(20006L, 30006L, ("attr", 0.7))),
          (Edge(20005L, 30007L, ("attr", 0.7))),
          (Edge(20006L, 30007L, ("attr", 0.7))),
          (Edge(20003L, 30008L, ("attr", 0.7))),
          (Edge(20004L, 30008L, ("attr", 0.7))),
          (Edge(20005L, 30008L, ("attr", 0.7))),
          (Edge(20006L, 30008L, ("attr", 0.7))),
          (Edge(20003L, 30009L, ("attr", 0.7))),
          (Edge(20004L, 30009L, ("attr", 0.7))),
          (Edge(20005L, 30009L, ("attr", 0.7))),
          (Edge(20006L, 30009L, ("attr", 0.7)))
        ))
    // Build the initial Graph
    val graph = Graph(nodes, edges)
    
    // Count all users which are postdocs
    graph.vertices.filter { case (id, (name, pos, count)) => pos == "postdoc" }.count
    // Count all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId).count
    
    // Show triplets
    graph.triplets.map(triplet =>
        triplet.srcAttr._2 + " :- " + triplet.attr._2 + " -> " + triplet.dstAttr._2)
        .collect.foreach(println(_))
    
    // AggregateMessage test
    val aggregatedGraph: VertexRDD[Double] = graph.mapReduceTriplets[Double](
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
