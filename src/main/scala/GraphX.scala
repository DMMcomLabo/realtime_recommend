package org.apache.spark.streaming.dmtc

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.security.MessageDigest
import scala.collection.mutable.HashMap

object GraphX {
  val initialMap:Map[Long, (String, String, Double)] = Map.empty
  val initialMessage: (Map[Long, (String, String, Double)], Double) = (initialMap, 0.0)
  
  def mergeMap(map1: Map[Long, (String, String, Double)], map2: Map[Long, (String, String, Double)]):Map[Long, (String, String, Double)] =  {
    (map1.keySet ++ map2.keySet).map( i => {
        val map1value = map1.getOrElse(i, ("", "", 0.0))
        val map2value = map2.getOrElse(i, ("", "", 0.0))
        val wordName = if (map1value._1 != "") map1value._1 else map2value._1
        val genreName = if (map1value._2 != "") map1value._2 else map2value._2
        val score = map1value._3 + map2value._3
      (i, (wordName, genreName, score))
    }).toMap
  }
  
  def normalizeRelations(rawRelations: Map[Long, (String, String, Double)], normalizeBase: Double) = {
    if (normalizeBase == 0.0) {
      rawRelations
    } else {
      rawRelations.keySet.map( i => {
              val (wordName, genreName, rawValue) = rawRelations.getOrElse(i, ("", "", 0.0))
              val normalizedValue = rawValue / normalizeBase
              (i, (wordName, genreName, normalizedValue))
            }).toMap
    }
  }
  
  def calcGenreWordRelation(graph: Graph[(Map[Long, (String, String, Double)], Double), (String, String, Double)]) = {
    graph.pregel(initialMessage, 2)(
      (id, VD, message) => {
        val normalizedRelations = normalizeRelations(message._1, message._2)
        (normalizedRelations, 0.0)
      },
      triplet => { 
        val (edgeType, edgeLabel, edgeWeight) = triplet.attr
        if (edgeType == "search") {
          Iterator({
              val wordId = triplet.srcId
              val wordName = edgeLabel
              val wordProductRelation = edgeWeight
              (triplet.dstId, (Map(wordId -> (wordName, "", wordProductRelation)), 0.0))
            })
        } else if (edgeType == "attr") {
          Iterator({
              val receiveMsg = triplet.srcAttr._1
              val productGenreRelation = edgeWeight
              // Map.mapValuesは使えない
              val sendMsg:Map[Long, (String, String, Double)] = receiveMsg.keySet.map( i => {
                  val (wordName, _, receiveScore) = receiveMsg.getOrElse(i, ("", "", 0.0))
                  val genreName = edgeLabel
                  val sendScore = receiveScore * productGenreRelation
                  (i, (wordName, genreName, sendScore))
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
  def createGraph(sc: SparkContext, edge: List[(Long ,Long, (String, String, Double))]) = {
    val edges: RDD[Edge[(String, String, Double)]] =
      sc.parallelize(edge.toArray.map({ case(fromId, toId, (edgeType, label, value)) => Edge(fromId, toId, (edgeType, label, value))}))
    Graph.fromEdges(edges, initialMessage)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[4]").setAppName("GraphX")
    val sc = new SparkContext(conf)

    var m_table = HashMap.empty[Long, (String, String)]
    var edge = List.empty[(Long ,Long, (String, String, Double))]
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
    edge = (generateHash("word", "艦隊これくしょん"), generateHash("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）"), ("search", "艦隊これくしょん", 0.8)) :: edge
    edge = (generateHash("word", "艦隊これくしょん"), generateHash("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱"), ("search", "艦隊これくしょん", 0.7)) :: edge
    edge = (generateHash("word", "艦隊これくしょん"), generateHash("product", "第1話 艦隊これくしょん-艦これ-"), ("search", "艦隊これくしょん", 0.6)) :: edge
    edge = (generateHash("word", "艦隊これくしょん"), generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), ("search", "艦隊これくしょん", 0.5)) :: edge
    edge = (generateHash("word", "魔法少女まどか☆マギカ"), generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), ("search", "魔法少女まどか☆マギカ", 0.8)) :: edge
    edge = (generateHash("word", "魔法少女まどか☆マギカ"), generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), ("search", "魔法少女まどか☆マギカ", 0.7)) :: edge
    edge = (generateHash("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）"), generateHash("genre", "青年コミック"), ("attr", "青年コミック", 0.7)) :: edge
    edge = (generateHash("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱"), generateHash("genre", "青年コミック"), ("attr", "青年コミック", 0.7)) :: edge
    edge = (generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "2015年冬アニメ"), ("attr", "2015年冬アニメ", 0.7)) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "2015年冬アニメ"), ("attr", "2015年冬アニメ", 0.7)) :: edge
    edge = (generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "アクション"), ("attr", "アクション", 0.7)) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "アクション"), ("attr", "アクション", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "アクション"), ("attr", "アクション", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "アクション"), ("attr", "アクション", 0.7)) :: edge
    edge = (generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "2010年代"), ("attr", "2010年代", 0.7)) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "2010年代"), ("attr", "2010年代", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "2010年代"), ("attr", "2010年代", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "2010年代"), ("attr", "2010年代", 0.7)) :: edge
    edge = (generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "テレビ"), ("attr", "テレビ", 0.7)) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "テレビ"), ("attr", "テレビ", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "魔法少女"), ("attr", "魔法少女", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "魔法少女"), ("attr", "魔法少女", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "劇場版（アニメ映画）"), ("attr", "劇場版（アニメ映画）", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "劇場版（アニメ映画）"), ("attr", "劇場版（アニメ映画）", 0.7)) :: edge
    edge = (generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "SF/ファンタジー"), ("attr", "SF/ファンタジー", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "SF/ファンタジー"), ("attr", "SF/ファンタジー", 0.7)) :: edge
    edge = (generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "SF/ファンタジー"), ("attr", "SF/ファンタジー", 0.7)) :: edge
    
    // Build the initial Graph
    val graph = createGraph(sc, edge)
    val newGraph = calcGenreWordRelation(graph)
    newGraph.triplets.filter(t => t.attr._1 == "attr")
    .map( t => {
        val genreId = t.dstId
        val wordRelations = t.dstAttr._1
        (genreId, wordRelations.filter(p => p._2._3 > 0.3))
      })
    .collect.foreach(println(_))
    
    // Show triplets
//    graph.triplets.map(triplet =>
//        triplet.srcAttr._2 + " :- " + triplet.attr + " -> " + triplet.dstAttr._2)
//        .collect.foreach(println(_))
    
    sc.stop()
  }
}
