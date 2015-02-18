package org.apache.spark.streaming.dmtc

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.security.MessageDigest

object GraphX {
  // edgeType, edgeLabel, count, score, productList
  type EdgeProperties = (String, String, Int, Double, String)
  // wordId, (wordName, genreName, wordCount, score, productList)
  type Relations = Map[Long, (String, String, Int, Double, String)]
  // relations, nodeType, weight
  type Message = (Relations, String, Double)
  
  val initialMap:Relations = Map.empty
  val initialMessage:Message = (initialMap, "", 0.0)
  
  def mergeMap(map1: Relations, map2: Relations):Relations =  {
    (map1.keySet ++ map2.keySet).map( wordId => {
        val map1value = map1.getOrElse(wordId, ("", "", 0, 0.0, ""))
        val map2value = map2.getOrElse(wordId, ("", "", 0, 0.0, ""))
        val wordName = if (map1value._1 != "") map1value._1 else map2value._1
        val genreName = if (map1value._2 != "") map1value._2 else map2value._2
        val count = if (map1value._3 != 0) map1value._3 else map2value._3
        val score = map1value._4 + map2value._4
        val productList = if (map1value._5 != "") map1value._5 else map2value._5
      (wordId, (wordName, genreName, count, score, productList))
    }).toMap
  }
  
  def normalizeRelations(rawRelations: Relations, normalizeBase: Double) = {
    if (normalizeBase == 0.0) {
      rawRelations
    } else {
      rawRelations.map { case (wordId, (wordName, genreName, count, rawValue, productList)) =>
         val normalizedValue = rawValue / normalizeBase
         wordId -> (wordName, genreName, count, normalizedValue, productList)
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
        val (edgeType, edgeLabel, count, edgeWeight, productList) = triplet.attr
        val sendMsgOpt = edgeType match {
          case "search" =>
              val wordId = triplet.srcId
              val wordName = edgeLabel
              val wordProductRelation = edgeWeight
              Some(triplet.dstId, (Map(wordId -> (wordName, "", count, wordProductRelation, productList)), "product", 0.0))
          case "attr" =>
              val receiveMsg = triplet.srcAttr._1
              val productGenreRelation = edgeWeight
              val sendMsg = receiveMsg.map { case (wordId, (wordName, _, wordCount, receiveScore, productList)) =>
                  val genreName = edgeLabel
                  val sendScore = receiveScore * productGenreRelation
                  wordId -> (wordName, genreName, wordCount, sendScore, productList)
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
}
