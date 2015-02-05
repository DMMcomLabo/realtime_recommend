package org.apache.spark.streaming.dmtc

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.security.MessageDigest

object GraphX {
  type EdgeProperties = (String, String, Double)
  type Relations = Map[Long, (String, String, Double)]
  type Message = (Relations, String, Double)
  
  val initialMap:Relations = Map.empty
  val initialMessage:Message = (initialMap, "", 0.0)
  
  def mergeMap(map1: Relations, map2: Relations):Relations =  {
    (map1.keySet ++ map2.keySet).map( wordId => {
        val map1value = map1.getOrElse(wordId, ("", "", 0.0))
        val map2value = map2.getOrElse(wordId, ("", "", 0.0))
        val wordName = if (map1value._1 != "") map1value._1 else map2value._1
        val genreName = if (map1value._2 != "") map1value._2 else map2value._2
        val score = map1value._3 + map2value._3
      (wordId, (wordName, genreName, score))
    }).toMap
  }
  
  def normalizeRelations(rawRelations: Relations, normalizeBase: Double) = {
    if (normalizeBase == 0.0) {
      rawRelations
    } else {
      rawRelations.map { case (wordId, (wordName, genreName, rawValue)) =>
         val normalizedValue = rawValue / normalizeBase
         wordId -> (wordName, genreName, normalizedValue)
      }
    }
  }
  
  def calcGenreWordRelation(graph: Graph[Message, EdgeProperties]) = {
    graph.pregel(GraphX.initialMessage, 2)(
      (id, VD, message) => {
        val (rawRelations, nodeType, normalizeBase) = message
        val normalizedRelations = normalizeRelations(rawRelations, normalizeBase)
        (normalizedRelations, nodeType, 0.0)
      },
      triplet => { 
        val (edgeType, edgeLabel, edgeWeight) = triplet.attr
        val sendMsgOpt = edgeType match {
          case "search" =>
              val wordId = triplet.srcId
              val wordName = edgeLabel
              val wordProductRelation = edgeWeight
              Some(triplet.dstId, (Map(wordId -> (wordName, "", wordProductRelation)), "product", 0.0))
          case "attr" =>
              val receiveMsg = triplet.srcAttr._1
              val productGenreRelation = edgeWeight
              val sendMsg = receiveMsg.map { case (wordId, (wordName, _, receiveScore)) =>
                  val genreName = edgeLabel
                  val sendScore = receiveScore * productGenreRelation
                  wordId -> (wordName, genreName, sendScore)
              }
              Some(triplet.dstId, (sendMsg, "genre", productGenreRelation))
          case _ => None
        }
        sendMsgOpt.iterator
      },
      (msg1, msg2) => (mergeMap(msg1._1, msg2._1), msg1._2, msg1._3 + msg2._3)
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
  def createGraph(sc: SparkContext, edge: List[(Long ,Long, EdgeProperties)]) = {
    val edges: RDD[Edge[EdgeProperties]] =
      sc.parallelize(edge.toArray.map({ case(fromId, toId, (edgeType, label, value)) => Edge(fromId, toId, (edgeType, label, value))}))
    Graph.fromEdges(edges, initialMessage)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[4]").setAppName("GraphX")
    val sc = new SparkContext(conf)

    val edgeList = scala.collection.mutable.ArrayBuffer.empty[Edge[GraphX.EdgeProperties]]
    edgeList += Edge(generateHash("word", "艦隊これくしょん"), generateHash("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）"), ("search", "艦隊これくしょん", 0.8))
    edgeList += Edge(generateHash("word", "艦隊これくしょん"), generateHash("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱"), ("search", "艦隊これくしょん", 0.7))
    edgeList += Edge(generateHash("word", "艦隊これくしょん"), generateHash("product", "第1話 艦隊これくしょん-艦これ-"), ("search", "艦隊これくしょん", 0.6))
    edgeList += Edge(generateHash("word", "艦隊これくしょん"), generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), ("search", "艦隊これくしょん", 0.5))
    edgeList += Edge(generateHash("word", "魔法少女まどか☆マギカ"), generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), ("search", "魔法少女まどか☆マギカ", 0.8))
    edgeList += Edge(generateHash("word", "魔法少女まどか☆マギカ"), generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), ("search", "魔法少女まどか☆マギカ", 0.7))
    edgeList += Edge(generateHash("product", "艦隊これくしょん -艦これ- アンソロジーコミック 横須賀鎮守府編 （1）"), generateHash("genre", "青年コミック"), ("attr", "青年コミック", 0.7))
    edgeList += Edge(generateHash("product", "艦隊これくしょん‐艦これ‐コミックアラカルト 舞鶴鎮守府編 壱"), generateHash("genre", "青年コミック"), ("attr", "青年コミック", 0.7))
    edgeList += Edge(generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "2015年冬アニメ"), ("attr", "2015年冬アニメ", 0.7))
    edgeList += Edge(generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "2015年冬アニメ"), ("attr", "2015年冬アニメ", 0.7))
    edgeList += Edge(generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "アクション"), ("attr", "アクション", 0.7))
    edgeList += Edge(generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "アクション"), ("attr", "アクション", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "アクション"), ("attr", "アクション", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "アクション"), ("attr", "アクション", 0.7))
    edgeList += Edge(generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "2010年代"), ("attr", "2010年代", 0.7))
    edgeList += Edge(generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "2010年代"), ("attr", "2010年代", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "2010年代"), ("attr", "2010年代", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "2010年代"), ("attr", "2010年代", 0.7))
    edgeList += Edge(generateHash("product", "第1話 艦隊これくしょん-艦これ-"), generateHash("genre", "テレビ"), ("attr", "テレビ", 0.7))
    edgeList += Edge(generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "テレビ"), ("attr", "テレビ", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "魔法少女"), ("attr", "魔法少女", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "魔法少女"), ("attr", "魔法少女", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "劇場版（アニメ映画）"), ("attr", "劇場版（アニメ映画）", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "劇場版（アニメ映画）"), ("attr", "劇場版（アニメ映画）", 0.7))
    edgeList += Edge(generateHash("product", "艦隊これくしょん-艦これ- 第1巻 限定版 【DMMオリジナル特典付き】（ブルーレイディスク）"), generateHash("genre", "SF/ファンタジー"), ("attr", "SF/ファンタジー", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [前編] 始まりの物語"), generateHash("genre", "SF/ファンタジー"), ("attr", "SF/ファンタジー", 0.7))
    edgeList += Edge(generateHash("product", "劇場版 魔法少女まどか☆マギカ [後編] 永遠の物語"), generateHash("genre", "SF/ファンタジー"), ("attr", "SF/ファンタジー", 0.7))
    
    // Build the initial Graph
    val edgeRDD = sc.parallelize(edgeList)
    val graph = Graph.fromEdges(edgeRDD, GraphX.initialMessage)
    val clustedGraph = GraphX.calcGenreWordRelation(graph)
    clustedGraph.vertices.filter(v => v._2._2 == "genre")
    .map( v => {
        val genreId = v._1
        val wordRelations = v._2._1
        wordRelations.filter(p => p._2._3 > 0.3)
      })
    .collect.foreach(println(_))
    
    // Show triplets
//    graph.triplets.map(triplet =>
//        triplet.srcAttr._2 + " :- " + triplet.attr + " -> " + triplet.dstAttr._2)
//        .collect.foreach(println(_))
    
    sc.stop()
  }
}
