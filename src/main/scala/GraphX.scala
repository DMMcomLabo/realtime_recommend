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
    val initialMessage: (Map[Long, Double], Double) = (initialMap, 0.0)
    val nodes: RDD[(VertexId, (String, String, Double, (Map[Long, Double], Double)))] =
      sc.parallelize(Array(
          (10001L, ("word", "艦隊これくしょん", 0.8, initialMessage)),
          (10002L, ("word", "魔法少女まどか☆マギカ", 0.8, initialMessage)),
          (20001L, ("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）", 1.0, initialMessage)),
          (20002L, ("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱", 1.0, initialMessage)),
          (20003L, ("product", "第1話 艦隊これくしょん-艦これ-", 1.0, initialMessage)),
          (20004L, ("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）", 1.0, initialMessage)),
          (20005L, ("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語", 1.0, initialMessage)),
          (20006L, ("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語", 1.0, initialMessage)),
          (30001L, ("genre", "青年コミック", 1.0, initialMessage)),
          (30002L, ("genre", "2015年冬アニメ", 1.0, initialMessage)),
          (30003L, ("genre", "アクション", 1.0, initialMessage)),
          (30004L, ("genre", "2010年代", 1.0, initialMessage)),
          (30005L, ("genre", "テレビ", 1.0, initialMessage)),
          (30006L, ("genre", "魔法少女", 1.0, initialMessage)),
          (30007L, ("genre", "劇場版（アニメ映画）", 1.0, initialMessage)),
          (30008L, ("genre", "SF/ファンタジー", 1.0, initialMessage))
        ))
    val edges: RDD[Edge[Double]] =
      sc.parallelize(Array(
          (Edge(10001L, 20001L, 0.8)),
          (Edge(10001L, 20002L, 0.7)),
          (Edge(10001L, 20003L, 0.6)),
          (Edge(10001L, 20004L, 0.5)),
          (Edge(10002L, 20005L, 0.8)),
          (Edge(10002L, 20006L, 0.7)),
          (Edge(20001L, 30001L, 0.7)),
          (Edge(20002L, 30001L, 0.7)),
          (Edge(20003L, 30002L, 0.7)),
          (Edge(20004L, 30002L, 0.7)),
          (Edge(20003L, 30003L, 0.7)),
          (Edge(20004L, 30003L, 0.7)),
          (Edge(20005L, 30003L, 0.7)),
          (Edge(20006L, 30003L, 0.7)),
          (Edge(20003L, 30004L, 0.7)),
          (Edge(20004L, 30004L, 0.7)),
          (Edge(20005L, 30004L, 0.7)),
          (Edge(20006L, 30004L, 0.7)),
          (Edge(20003L, 30005L, 0.7)),
          (Edge(20004L, 30005L, 0.7)),
          (Edge(20005L, 30006L, 0.7)),
          (Edge(20006L, 30006L, 0.7)),
          (Edge(20005L, 30007L, 0.7)),
          (Edge(20006L, 30007L, 0.7)),
          (Edge(20004L, 30008L, 0.7)),
          (Edge(20005L, 30008L, 0.7)),
          (Edge(20006L, 30008L, 0.7))
        ))
    // Build the initial Graph
    val graph = Graph(nodes, edges)
    
    // Show triplets
//    graph.triplets.map(triplet =>
//        triplet.srcAttr._2 + " :- " + triplet.attr + " -> " + triplet.dstAttr._2)
//        .collect.foreach(println(_))
    
    def mergeMap(map1: Map[Long, Double], map2: Map[Long, Double]):Map[Long, Double] =  {
      (map1.keySet ++ map2.keySet).map( i => (i, map1.getOrElse(i, 0.0) + map2.getOrElse(i, 0.0))).toMap
    }
    
    val newGraph = graph.pregel(initialMessage, 2)(
      (id, VD, message) => VD.copy(_4 = message),
      triplet => { 
        if (triplet.srcAttr._1 == "word" && triplet.dstAttr._1 == "product") {
          Iterator((triplet.dstId, (Map(triplet.srcId -> triplet.attr), 0.0)))
        } else if (triplet.srcAttr._1 == "product" && triplet.dstAttr._1 == "genre") {
          Iterator({
              val oldMap = triplet.srcAttr._4._1
              val attr = triplet.attr
              val newMap:Map[Long, Double] = oldMap.keySet.map( i => (i, oldMap.getOrElse(i, 0.0) * attr)).toMap
            (triplet.dstId, (newMap, triplet.attr))
          })
        } else {
          Iterator.empty
        }
      },
      (msg1, msg2) => (mergeMap(msg1._1, msg2._1), msg1._2 + msg2._2)
    )
    // Show correlation
    newGraph.vertices.filter(v => v._2._1 == "genre")
    .map( v => {
        val id = v._1
        val name = v._2._2
        val wordmap = v._2._4._1
        val weight = v._2._4._2
        val modwordmap = wordmap.keySet.map( i => (i, wordmap.getOrElse(i, 0.0) / weight))
        (id, name, modwordmap)
      })
    .collect.foreach(println(_))
    
    sc.stop()
  }
}
