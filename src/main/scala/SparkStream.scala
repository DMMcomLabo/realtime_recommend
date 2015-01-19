
package org.apache.spark.streaming.dmtc

import org.apache.spark.streaming.dmtc._
import twitter4j._
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.auth.Authorization
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver
import StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.JavaConverters._
import org.apache.spark._
//import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel

import com.rabbitmq.client.ConnectionFactory

import dispatch._
import dispatch.Defaults._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{ Try, Success, Failure }

import org.atilika.kuromoji.Tokenizer
import org.atilika.kuromoji.Token

import java.util.regex._

import com.typesafe.config.ConfigFactory

object SparkStream {

val http = new Http()

  def main(args: Array[String]){
    val config = ConfigFactory.load()
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("twitter4j.oauth.consumerKey", config.getString("twitter.consumerKey"))
    System.setProperty("twitter4j.oauth.consumerSecret", config.getString("twitter.consumerSecret"))
    System.setProperty("twitter4j.oauth.accessToken", config.getString("twitter.accessToken"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.getString("twitter.accessTokenSecret"))
    val conf = new SparkConf().setAppName("SparkStream")
    val ssc = new StreamingContext(conf, Seconds(5))
    val filter = new FilterQuery
    val locations = Array(Array( 122.87d,24.84d ),Array(153.01d,46.80d))
    filter.locations(locations)
    
    val tweets = TwitterDmmUtils.createStream(ssc, None,filter)
    /**
      * 5秒分のtweet
      * <word>,(<userId>,1)
      */
    val statuses = tweets.flatMap(status =>{
	    val kuromojiList = kuromojiParser(status.getText,status.getId)
	    //@todo search 
	 (kuromojiList)
	}).map{row =>{
		(row.split(",")(1),(row.split(",")(0),1))
	}}
//    statuses.print()
    /**
     * 1時間分のtweetの集計countの降順
     * <1時間分のcount> ,(<userID[:UserID]> ,<ワード>)
     */
    val perOneHours = statuses.reduceByKeyAndWindow((a:(String,Int),b:(String,Int)) =>{(a._1+":"+b._1,a._2+b._2)},Minutes(60)).map{
		 case(a,b) => (b._2,(b._1,a))
	}.transform(_.sortByKey(false))
    perOneHours.print()
    
//    val graph = statuses.map(fields => (findShipName(account_map_list, fields._1, fields._2), textConverter(keywords, shipNames, fields._2)))
//            .flatMap(fields => fields._2.map(fields._1 + "dmtc_separator" + _))
//    graph.foreachRDD(rdd => rdd.foreach(t => publishMQ(t)))
//    val tokenizer = Tokenizer.builder.mode(Tokenizer.Mode.NORMAL).build
//ハッシュタグで抜き出し
//    val words = statuses.flatMap{status => tokenizer.tokenize(status).toArray}
//    words.foreach { t =>
// 	val token = t.asInstanceOf[Token]
//	println(s"$token.getSurfaceFrom} - $token.getAllFeatures}")
//    }
//    val hashtags = words.filter(word => word.startsWith("#"))
//    val counts = hashtags.countByValueAndWindow(Seconds(60 * 5), Seconds(1))
//                         .map { case(tag, count) => (count, tag) }
//    counts.foreach(rdd => println(rdd.top(10).mkString("\n")))

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
  
//  def findShipName(data: List[(Long, String)], key: Long, text: String): String = {
//    val result = data.filter(_._1 == key)
//    result.length match {
//      case 0 => getKanmusuName(text)
//      case _ => result(0)._2
//    }
//  }
//  def combiner(a: List[String], b: List[String]): List[String] = {
//    a.length match {
//      case 0 => combiner(List(""), b)
//      case _ => b.length match {
//          case 0 => combiner(a, List(""))
//          case _ => a.map( x => b.map( y => x + "dmtc_separator" + y)).flatten
//      }
//    }
//  }
//  def textParser(dic: List[String], text: String): List[String] = {
//    dic.filter(text.indexOf(_) > -1)
//  }
//  def textConverter(dic1: List[String], dic2: List[String], text: String): List[String] = {
//    val simpleKeywordList = textParser(dic1, text)
//    val simpleKanmusuList = textParser(dic2, text)
//    val solrKanmusuName = getKanmusuName(text)
//    println(solrKanmusuName)
//    val kanmusuList = solrKanmusuName.length match {
//      case 0 => simpleKanmusuList
//      case _ => solrKanmusuName :: simpleKanmusuList
//    }
//    println(simpleKanmusuList)
//    println(kanmusuList)
//    val kuromojiList = kuromojiParser(text)
//    val keywordList = kuromojiList ::: simpleKeywordList
//    val graph = combiner(keywordList.distinct, kanmusuList.distinct)
//    val result = graph.map(_ + "dmtc_separator" + text.replace("\"", "\\\"").replace("\r", "").replace("\n", ""))
////    println(result)
//    result
//  }
//  def publishMQ(message: String) {
//    val fields = message.split("dmtc_separator", 4)
//    val factory = new ConnectionFactory()
//    factory.setUsername("guest")
//    factory.setPassword("guest")
//    factory.setVirtualHost("/")
//    factory.setHost("vmsvr003")
//    factory.setPort(5672)
//    val conn = factory.newConnection()
//    val channel = conn.createChannel()
//    val json = "{\"source\": \"" + fields(0) + "\", \"target\": \"" + fields(2) + "\", \"word\": \"" + fields(1) + "\", \"message\": \"" + fields(3) + "\", \"weight\": 1}"
//    println(json)
//    fields(0).length + fields(2).length match {
//      case 0 => println("skip")
//      case _ => channel.basicPublish("", "kankore", null, json.getBytes())
//    }
//    channel.close()
//    conn.close()
//  }
//  def searchKanmusuName(text: String): String = {
//    val encodeText = URLEncoder.encode(text, "UTF-8")
//    // Solrに渡す
//    val request = url("http://vmsvr004:8888/solr/select?defType=edismax&qf=name_ja&q=" + encodeText + "&wt=csv")
//    val response = http(request OK as.String)
//    response.onComplete {
//      case Success(msg) => msg
//      case Failure(t)   => ""
//    }
//    Await.result(response, 1.seconds)
//  }
//  def getKanmusuName(text: String):String = {
//    val solrKanmusuResult = searchKanmusuName(text)
//    val solrResultList = solrKanmusuResult.replace("\r", "").replace("\n", "").split(",")
//    val solrKanmusuName = solrResultList.length match {
//      case 3 => solrResultList(2)
//      case _ => ""
//    }
//    solrKanmusuName
//  }
  def kuromojiParser(text: String, id:Long): List[String] = {
    //@todo modified tokenize
    val tokenizer = Tokenizer.builder.mode(Tokenizer.Mode.NORMAL).build
    //val tokenizer = Tokenizer.builder.userDictionary("/tmp/dmm_userdict.txt").build
    val tokens = tokenizer.tokenize(text).toArray
    val result = tokens
      .filter { t =>
        val token = t.asInstanceOf[Token]

        token.getPartOfSpeech.indexOf("名詞") > -1 && token.getPartOfSpeech.indexOf("一般") > -1 
//        token.getPartOfSpeech.indexOf("名詞") > -1 
      }
      .map(t => t.asInstanceOf[Token].getSurfaceForm)
      .filter{ v =>
	v.length > 2 && !(v matches "^[a-zA-Z]+$|^[0-9]+$")
      }
      .toList
    result.length match {
      case 0 => List.empty[String]
      case _ => List(id + "," + result.last)
    }
  }
}
