
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
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
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
import scala.util.control.Exception._
import scala.util.{ Try, Success, Failure }

import org.atilika.kuromoji.Tokenizer
import org.atilika.kuromoji.Token

import java.util.regex._
import java.net.{URI,URLDecoder,URLEncoder}
import java.security.MessageDigest

import com.typesafe.config.ConfigFactory

object SparkStream {
  // (title, genre, score)
  type Product = (String, String, Double)
  
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
    val sc = ssc.sparkContext
    val filter = new FilterQuery
//    val locations = Array(Array( 122.87d,24.84d ),Array(153.01d,46.80d))
//    filter.locations(locations)
    val track = Array("#kurobas","#dp_anime","#暗殺教室","#jojo_anime","#konodan","#drrr_anime","#夜ヤッター","#falgaku","#みりたり","#rollinggirls","#milkyholmes","#aldnoahzero","#shohari","#fafner","#mikagesha","#ISUCA","#fafnir_a","#koufukug","#tkg_anime","#艦これ","#yamato2199","#ぱんきす","#boueibu","#shinmaimaou","#maria_anime","#ワルブレ_A","#yurikuma","#dogdays","#saekano","#garupan","#abso_duo","#anisama","#imas_cg","#1kari","#monogatari","#cfvanguard","#実在性ミリオンアーサー","#teamdayan","#anime_dayan", "#dayan","#nekonodayan","#morikawa3","#donten","#kiseiju_anime","#loghorizon","#pp_anime")
    filter.track(track)
    
    val tweets = TwitterDmmUtils.createStream(ssc, None, filter)
    /**
      * 5秒分のtweet
      * <word>, (Array[Product], count = 1)
      */
    val statuses = tweets.flatMap { status =>
	    kuromojiParser(status.getText, status.getId)
	} .map { word =>
            val searchResultCSV = wordSearch(word).split("\n")
            val products:Array[Product] =
              searchResultCSV.drop(1).map { row =>
                  val cols = row.split(",")
                  val (title, genre) = (cols(0), cols(1))
                  val score = allCatch opt cols(2).toDouble getOrElse(0.0)
                  (title, genre, score)
              } .filter { case (title, genre, score) => score > 0.0 }
            (word, (products, 1))
	}
//    statuses.print()
    /**
     * 1時間分のtweetの集計countの降順
     * <1時間分のcount>, (Array[Product], <ワード>)
     */
    val perOneHours = statuses.reduceByKeyAndWindow( 
      { case ((products1:Array[Product], count1:Int),
              (products2:Array[Product], count2:Int)) =>
          (products1, count1 + count2)
      }, Minutes(60)).map {
          case(word, (products, count)) => (count, (products, word))
      }.transform(_.sortByKey(false))
//    perOneHours.print()
     /**
      * 1時間分のGraphを生成する
      */
    val graphBaseData = perOneHours.flatMap { case (count, (products, word)) =>
        val word_digest = GraphX.generateHash("word", word)
        products.toList.flatMap { case (title, genre, score) =>
            val product_digest = GraphX.generateHash("product", title)
            val genre_digest = GraphX.generateHash("genre", genre)
            List(
                Edge(word_digest, product_digest, ("search", word, score)),
                Edge(product_digest, genre_digest, ("attr", genre, score))
            )
        }
    } foreachRDD { edgeRDD =>
      val graph = Graph.fromEdges(edgeRDD, GraphX.initialMessage)
      val clustedGraph = GraphX.calcGenreWordRelation(graph)
      clustedGraph.vertices.filter(v => v._2._2 == "genre")
      .map { v => 
          val genreId = v._1
          val wordRelations = v._2._1
          wordRelations.filter({ case (id, (word, genre, score)) => genre != "" && score > 0.5}).values
        }
      .collect { case t if t.nonEmpty =>
          val genre = t.head._2
          val words = t.map({ case (word, genre, score) => (word, score) })
          (genre, words)
        }
      .collect.foreach(println(_))
      println("----------------------------")
    }

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * search from solr
   */
  def wordSearch(text:String): String = {
    val encodeText = URLEncoder.encode(text,"UTF-8")
    val request = url("http://vmsvr004:8888/solr/dmm/select?start=0&rows=30&defType=edismax&fl=title,genre,score&fq=-service:mono&qf=title_ja%5E30.0%20title_cjk%5E12.0%20subtitle_ja%5E20.0%20subtitle_cjk%5E8.0%20comment_ja%5E0.1%20comment_cjk%5E0.1&q=" + encodeText + "&wt=csv")
    val response = http(request OK as.String)
    response.onComplete {
      case Success(msg) => msg
      case Failure(t)   => ""
    }
    Await.result(response, 1.seconds)
  }
  /**
   * tweet to word list
   */
  def kuromojiParser(text: String, id:Long): List[String] = {
    //@todo modified tokenize
    val tokenizer = UserDic.getInstance()
    //val tokenizer = Tokenizer.builder.userDictionary("/tmp/dmm_userdict.txt").build
    val tokens = tokenizer.tokenize(text).toArray
    val result = tokens
      .map { token => token.asInstanceOf[Token] }
      .collect { case token if {
            val partOfSpeech = token.getPartOfSpeech
            val normalNoun = (partOfSpeech.indexOf("名詞") > -1 && partOfSpeech.indexOf("一般") > -1)
            val customNoun = partOfSpeech.indexOf("カスタム名詞") > -1
            normalNoun || customNoun } =>
         token.asInstanceOf[Token].getSurfaceForm
      }
      .filter{ v => v.length >= 4 && !(v matches "^[a-zA-Z]+$|^[0-9]+$") }
      .toList
    result.length match {
      case 0 => List.empty[String]
      case _ => result
    }
  }
}
