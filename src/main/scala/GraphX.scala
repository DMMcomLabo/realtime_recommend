package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.security.MessageDigest
import scala.collection.mutable.HashMap

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

    var m_table = HashMap.empty[Long, (String, String)]
    var edge = List.empty[(Long ,Long, Double)]
    def generateHash(nodeType: String, name: String): Long = {
      MessageDigest.getInstance("MD5")
      .digest((nodeType + name).getBytes)
      .slice(0, 8)
      .zipWithIndex
      .map({case (byte, index) => (byte & 0xffL) << (8 * index)})
      .sum
    }
    def generateNode(nodeType: String, name: String) = {
       generateHash(nodeType, name) -> (nodeType, name)
    }
    m_table += generateNode("word", "艦隊これくしょん")
    m_table += generateNode("word", "魔法少女まどか☆マギカ")
    m_table += generateNode("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）")
    m_table += generateNode("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱")
    m_table += generateNode("product", "第1話 艦隊これくしょん-艦これ-")
    m_table += generateNode("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）")
    m_table += generateNode("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語")
    m_table += generateNode("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語")
    m_table += generateNode("genre", "青年コミック")
    m_table += generateNode("genre", "2015年冬アニメ")
    m_table += generateNode("genre", "アクション")
    m_table += generateNode("genre", "2010年代")
    m_table += generateNode("genre", "テレビ")
    m_table += generateNode("genre", "魔法少女")
    m_table += generateNode("genre", "劇場版（アニメ映画）")
    m_table += generateNode("genre", "SF/ファンタジー")
    edge = (generateHash("word", "艦隊これくしょん"), generateHash("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）"), 0.8) :: edge
    edge = (generateHash("word", "艦隊これくしょん"), generateHash("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱"), 0.7) :: edge
    edge = (generateHash("word", "艦隊これくしょん"), generateHash("product", "第1話 艦隊これくしょん-艦これ-"), 0.6) :: edge
    edge = (generateHash("word", "艦隊これくしょん"), generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), 0.5) :: edge
    edge = (generateHash("word", "魔法少女まどか☆マギカ"), generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), 0.8) :: edge
    edge = (generateHash("word", "魔法少女まどか☆マギカ"), generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), 0.7) :: edge
    edge = (generateHash("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）"), generateHash("genre", "青年コミック"), 0.7) :: edge
    edge = (generateHash("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱"), generateHash("genre", "青年コミック"), 0.7) :: edge
    edge = (generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "2015年冬アニメ"), 0.7) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "2015年冬アニメ"), 0.7) :: edge
    edge = (generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "アクション"), 0.7) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "アクション"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "アクション"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "アクション"), 0.7) :: edge
    edge = (generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "2010年代"), 0.7) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "2010年代"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "2010年代"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "2010年代"), 0.7) :: edge
    edge = (generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "テレビ"), 0.7) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "テレビ"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "魔法少女"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "魔法少女"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "劇場版（アニメ映画）"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "劇場版（アニメ映画）"), 0.7) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "SF/ファンタジー"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "SF/ファンタジー"), 0.7) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "SF/ファンタジー"), 0.7) :: edge
    
    val nodes: RDD[(VertexId, (String, String, Double, (Map[Long, (String, Double)], Double)))] =
      sc.parallelize(m_table.toArray.map({ case (id, (nodeType, name)) => (id, (nodeType, name, 1.0, initialMessage)) }))
    val edges: RDD[Edge[Double]] =
      sc.parallelize(edge.toArray.map({ case(fromId, toId, value) => Edge(fromId, toId, value)}))
    
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
