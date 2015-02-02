package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object GraphX {
  val initialMap:Map[Long, (String, Double)] = Map.empty
  val initialMessage: (Map[Long, (String, Double)], Double) = (initialMap, 0.0)
  
  def mergeMap(map1: Map[Long, (String, Double)], map2: Map[Long, (String, Double)]):Map[Long, (String, Double)] =  {
    (map1.keySet ++ map2.keySet).map( i => {
        val map1value = map1.getOrElse(i, ("", 0.0))
        val map2value = map2.getOrElse(i, ("", 0.0))
        val name = if (map1value._1 != "") map1value._1 else map2value._1
        val score = map1value._2 + map2value._2
      (i, (name, score))
    }).toMap
  }
  
  def normalizeRelations(rawRelations: Map[Long, (String, Double)], normalizeBase: Double) = {
    if (normalizeBase == 0.0) {
      rawRelations
    } else {
      rawRelations.keySet.map( i => {
              val rawValue = rawRelations.getOrElse(i, ("", 0.0))
              val rawName = rawValue._1
              val normalizedValue = rawValue._2 / normalizeBase
              (i, (rawName, normalizedValue))
            }).toMap
    }
  }
  
  def calcGenreWordRelation(graph: Graph[(String, String, Double, (Map[Long, (String, Double)], Double)), Double]) = {
    graph.pregel(initialMessage, 2)(
      (id, VD, message) => {
        val normalizedRelations = normalizeRelations(message._1, message._2)
        VD.copy(_4 = (normalizedRelations, 0.0))
      },
      triplet => { 
        if (triplet.srcAttr._1 == "word" && triplet.dstAttr._1 == "product") {
          Iterator({
              val wordId = triplet.srcId
              val wordName = triplet.srcAttr._2
              val wordProductRelation = triplet.attr
              (triplet.dstId, (Map(wordId -> (wordName, wordProductRelation)), 0.0))
            })
        } else if (triplet.srcAttr._1 == "product" && triplet.dstAttr._1 == "genre") {
          Iterator({
              val receiveMsg = triplet.srcAttr._4._1
              val productGenreRelation = triplet.attr
              // Map.mapValuesは使えない
              val sendMsg:Map[Long, (String, Double)] = receiveMsg.keySet.map( i => {
                  val receiveValue = receiveMsg.getOrElse(i, ("", 0.0))
                  val receiveScore = receiveValue._2
                  val sendScore = receiveScore * productGenreRelation
                  val sendValue = receiveValue.copy(_2 = sendScore)
                  (i, sendValue)
                }).toMap
            (triplet.dstId, (sendMsg, productGenreRelation))
          })
        } else {
          Iterator.empty
        }
      },
      (msg1, msg2) => (mergeMap(msg1._1, msg2._1), msg1._2 + msg2._2)
    )
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[4]").setAppName("GraphX")
    val sc = new SparkContext(conf)

    val nodes: RDD[(VertexId, (String, String, Double, (Map[Long, (String, Double)], Double)))] =
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
    val newGraph = calcGenreWordRelation(graph);
    newGraph.vertices.filter(v => v._2._1 == "genre")
    .map( v => {
        val genreId = v._1
        val genreName = v._2._2
        val wordRelations = v._2._4._1
        (genreId, genreName, wordRelations.filter(p => p._2._2 > 0.3))
      })
    .collect.foreach(println(_))
    
    // Show triplets
//    graph.triplets.map(triplet =>
//        triplet.srcAttr._2 + " :- " + triplet.attr + " -> " + triplet.dstAttr._2)
//        .collect.foreach(println(_))
    
    sc.stop()
  }
}
