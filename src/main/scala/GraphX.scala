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
    
    val initialMap:Map[Long, Double] = Map.empty
    val nodes: RDD[(VertexId, (String, String, Double, Map[Long, Double]))] =
      sc.parallelize(Array(
          (10001L, ("word", "艦隊これくしょん", 0.8, initialMap)),
          (10002L, ("word", "魔法少女まどか☆マギカ", 0.8, initialMap)),
          (20001L, ("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）", 1.0, initialMap)),
          (20002L, ("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱", 1.0, initialMap)),
          (20003L, ("product", "第1話 艦隊これくしょん-艦これ-", 1.0, initialMap)),
          (20004L, ("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）", 1.0, initialMap)),
          (20005L, ("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語", 1.0, initialMap)),
          (20006L, ("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語", 1.0, initialMap)),
          (30001L, ("genre", "青年コミック", 1.0, initialMap)),
          (30002L, ("genre", "2015年冬アニメ", 1.0, initialMap)),
          (30003L, ("genre", "アクション", 1.0, initialMap)),
          (30004L, ("genre", "2010年代", 1.0, initialMap)),
          (30005L, ("genre", "テレビ", 1.0, initialMap)),
          (30006L, ("genre", "魔法少女", 1.0, initialMap)),
          (30007L, ("genre", "劇場版（アニメ映画）", 1.0, initialMap)),
          (30008L, ("genre", "2010年代", 1.0, initialMap)),
          (30009L, ("genre", "SF/ファンタジー", 1.0, initialMap))
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
          (Edge(20006L, 30009L, ("attr", 0.7))),
          (Edge(30001L, 10001L, ("cluster", 0.0))),
          (Edge(30002L, 10001L, ("cluster", 0.0))),
          (Edge(30003L, 10001L, ("cluster", 0.0))),
          (Edge(30004L, 10001L, ("cluster", 0.0))),
          (Edge(30005L, 10001L, ("cluster", 0.0))),
          (Edge(30006L, 10001L, ("cluster", 0.0))),
          (Edge(30007L, 10001L, ("cluster", 0.0))),
          (Edge(30008L, 10001L, ("cluster", 0.0))),
          (Edge(30009L, 10001L, ("cluster", 0.0))),
          (Edge(30001L, 10002L, ("cluster", 0.0))),
          (Edge(30002L, 10002L, ("cluster", 0.0))),
          (Edge(30003L, 10002L, ("cluster", 0.0))),
          (Edge(30004L, 10002L, ("cluster", 0.0))),
          (Edge(30005L, 10002L, ("cluster", 0.0))),
          (Edge(30006L, 10002L, ("cluster", 0.0))),
          (Edge(30007L, 10002L, ("cluster", 0.0))),
          (Edge(30008L, 10002L, ("cluster", 0.0))),
          (Edge(30009L, 10002L, ("cluster", 0.0)))
        ))
    // Build the initial Graph
    val graph = Graph(nodes, edges)
    
    // Show triplets
    graph.triplets.map(triplet =>
        triplet.srcAttr._2 + " :- " + triplet.attr._2 + " -> " + triplet.dstAttr._2)
        .collect.foreach(println(_))
    
    def mergeMap(map1: Map[Long, Double], map2: Map[Long, Double]):Map[Long, Double] =  {
      (map1.keySet ++ map2.keySet).map( i => (i, map1.getOrElse(i, 0.0) + map2.getOrElse(i, 0.0))).toMap
    }
    
    def correlation(map1: Map[Long, Double], map2: Map[Long, Double]):Double =  {
      val total = (map1.keySet ++ map2.keySet).map( i => (map1.getOrElse(i, 0.0) * map2.getOrElse(i, 0.0))).sum
      val base = map2.values.sum
      if (base > 0) {
        total / base
      } else {
        0.0
      }
    }
    
    // Calculate correlation between word and genre
    val newGraph = graph.pregel(initialMap, 1)(
      (id, VD, message) => VD.copy(_4 = message),
      triplet => { 
        if (triplet.attr._1 == "search") {
          Iterator((triplet.srcId, Map(triplet.dstId -> triplet.attr._2)))
        } else if (triplet.attr._1 == "attr") {
          Iterator((triplet.dstId, Map(triplet.srcId -> triplet.attr._2)))
        } else {
          Iterator.empty
        }
      },
      (map1, map2) => mergeMap(map1, map2)
    )
    // Show correlation
    newGraph.triplets.filter(triplet => triplet.attr._1 == "cluster")
    .map(triplet => {
        triplet.srcAttr._2 + " :- " + correlation(triplet.srcAttr._4, triplet.dstAttr._4) + " -> " + triplet.dstAttr._2
      }
    ).collect.foreach(println(_))
    
    sc.stop()
  }
}
