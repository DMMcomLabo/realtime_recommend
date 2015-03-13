
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
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import math._

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

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import org.atilika.kuromoji.Tokenizer
import org.atilika.kuromoji.Token

import java.util.regex._
import java.net.{ URI, URLDecoder, URLEncoder }
import java.security.MessageDigest

import com.typesafe.config.{ Config, ConfigFactory }

object SparkStream {
  // (title, genre, score, imageUrl)
  type Product = (String, String, Double, String)
  
  val http = new Http()
  
  def main(args: Array[String]) {
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
    val track = Array(
      "#kurobas", "#dp_anime", "#暗殺教室", "#jojo_anime", "#konodan",
      "#drrr_anime", "#夜ヤッター", "#falgaku", "#みりたり", "#rollinggirls",
      "#milkyholmes", "#aldnoahzero", "#shohari", "#fafner", "#mikagesha",
      "#ISUCA", "#fafnir_a", "#koufukug", "#tkg_anime", "#艦これ",
      "#yamato2199", "#ぱんきす", "#boueibu", "#shinmaimaou", "#maria_anime",
      "#ワルブレ_A", "#yurikuma", "#dogdays", "#saekano", "#garupan",
      "#abso_duo", "#anisama", "#imas_cg", "#1kari", "#monogatari",
      "#cfvanguard", "#実在性ミリオンアーサー", "#teamdayan", "#anime_dayan", "#dayan",
      "#nekonodayan", "#morikawa3", "#donten", "#kiseiju_anime", "#loghorizon",
      "#pp_anime", "#gレコ", "#なりヒロwww", "#君嘘", "#yuyuyu"
    )
    filter.track(track)

    val learning_data = MLUtils.loadLibSVMFile(sc, "/tmp/sample.data.svm.txt").cache()
    val model = SVMWithSGD.train(learning_data, 10)
    model.clearThreshold()

    val tweets = TwitterDmmUtils.createStream(ssc, None, filter)
    /**
     * 5秒分のtweet
     * <word>, (Array[Product], count)
     */
    val statuses = tweets.map { status =>
      kuromojiParser(status.getText, status.getId)
    }.map {
      case (tweet, words) =>
        /**
         * 各単語ごとに商品を検索して、スコアの中央値を計算
         */
        val findImage = """(\/[\/\w]+\.(jpg|png))""".r
        val wordsWithProducts = words.map { word =>
          val searchResultCSV = wordSearch(word).split("\n")
          val products: Array[Product] =
            searchResultCSV.drop(1).map { row =>
              val cols = row.split(",")
              val (title, genre) = (cols(0), cols(1))
              val score = allCatch opt cols(2).toDouble getOrElse (0.0)
              val image = allCatch opt findImage.findFirstIn(row).head.toString getOrElse("")
              (title, genre, score, image)
            } filter { case (title, genre, score, image) => score > 0.0 && image != "" }
          val median = products.length match {
            case 0 => 0.0
            case n => products(ceil(n / 2).toInt)._3
          }
          (word, products, median)
        }
        /**
         * 各ツイートごとに、単語を次元とした中央値のベクトルを生成
         */
        val svmVector = wordsWithProducts.map {
          case (word, products, median) => median
        }
        val svmPredict = svmVector.length match {
          case 0 => 0.0
          case n => model.predict(Vectors.dense(svmVector.sum / svmVector.length))
        }
        (tweet, wordsWithProducts, svmPredict)
    }.filter {
      case (tweet, wordsWithProducts, svmPredict) => svmPredict > 1.0
    }.flatMap {
      case (tweet, wordsWithProducts, svmPredict) =>
        wordsWithProducts.map {
          case (word, products, median) =>
            (word, (1, products))
        }
    }
    //    statuses.print()
    /**
     * 1時間分のwordの集計
     * <ワード>, (<1時間分のcount>, Array[Product])
     */
    val perOneHours = statuses.reduceByKeyAndWindow(
      {
        case ((count1: Int, products1: Array[Product]),
              (count2: Int, products2: Array[Product])) =>
          (count1 + count2, products1)
      }, Minutes(60)
    )
    //    perOneHours.print()
    /**
     * 1時間分のGraphを生成する
     */
    val graphBaseData = perOneHours.flatMap {
      case (word, (count, products)) =>
        val word_digest = GraphX.generateHash("word", word)
        val productList = products.toList.map {
          case (title, genre, score, image) =>
            val product_digest = GraphX.generateHash("product", title)
            List(
              product_digest,
              title,
              score,
              image
            ).mkString("::")
        }.take(5).mkString(":-:")
        products.toList.flatMap {
          case (title, genre, score, image) =>
            val product_digest = GraphX.generateHash("product", title)
            val genre_digest = GraphX.generateHash("genre", genre)
            List(
              Edge(word_digest, product_digest, ("search", word, count, score, productList)),
              Edge(product_digest, genre_digest, ("attr", genre, 1, score, ""))
            )
        }
    }.foreachRDD { edgeRDD =>
      val graph = Graph.fromEdges(edgeRDD, GraphX.initialMessage)
      val clustedGraph = GraphX.calcGenreWordRelation(graph)
      val words = clustedGraph.vertices.filter { vertex =>
        vertex._2._2 == "genre"
      }.map { vertex =>
        val genreId = vertex._1
        val wordRelations = vertex._2._1
        val wordScores = wordRelations.filter {
          case (id, (word, genre, count, score, productList)) =>
            genre != ""
        }.map { wordRelation =>
          val wordId = wordRelation._1
          val word = wordRelation._2._1
          val score = wordRelation._2._4
          (wordId, (word, score))
        }
        val genre = wordRelations.values.head._2
        (genreId, genre, wordScores)
      }.flatMap {
        case (genreId, genre, wordScores) =>
          wordScores.map {
            case (wordId, (word, score)) =>
              val intGenreId = genreId.toInt.abs % 100000 // intGenreIdからgenreIdへの写像が欲しい
              (wordId, (word, genre, intGenreId, Seq((intGenreId, score))))
          }
      }
      val genreIdMap = words.map {
        case(wordId, (word, genre, intGenreId, genreSeq)) =>
          (intGenreId, genre)
      }.collect.toMap

      val wordVectors = words.reduceByKey {
        case ((word1, genre1, intGenreId1, seq1), (word2, genre2, intGenreId2, seq2)) =>
          (word1, genre1, intGenreId1, seq1++seq2)
      }.map {
        case (wordId, (word, genre, intGenreId, genreSeq)) =>
          (Vectors.sparse(100000, genreSeq), wordId, word)
      }.cache()
      
      //wordVectors.collect.foreach(println(_))
      if (wordVectors.count > 0){
        val numClusters  = 20
        val numIterations = 20
        val trainVectors = wordVectors.map(_._1)
        trainVectors.cache()
        val clusters = KMeans.train(trainVectors, numClusters, numIterations)
        //val WSSSE = clusters.computeCost(wordVectors)
        //println("Within Set Sum of Squared Errors = " + WSSSE)


        // clusterCentersはArray
        // centerVectorはmllib.linalg.Vector
        val genreMap = clusters.clusterCenters.zipWithIndex.map { case(centerVector, clusterId) =>
          var maxIndex: Int = 0
          var maxValue: Double = 0
          for (i <- 0 to centerVector.size-1 ) {
            val element = centerVector.apply(i)
            if (element > maxValue) {
              maxIndex = i
              maxValue = element
            }
          }
          (clusterId, genreIdMap.getOrElse(maxIndex,""))
          //centerVector.apply(0)
        }.toMap

        wordVectors.map {
          case(wordVector, wordId, word) =>
            //(clusters.predict(wordVector), (wordId, word))
            (genreMap.getOrElse(clusters.predict(wordVector), ""), word)
        }.groupByKey().collect.foreach(println(_))

        //clusters.clusterCenters.foreach {
        //  center => println(f"${center.toArray.mkString("[", ", ", "]")}%s")
        //}
      }

/*
      .collect {
        case genreRow if genreRow._2.size >= 3 =>
          val (genreId, wordRelations) = genreRow
          val genre = wordRelations.values.head._2
          val words = wordRelations.map {
            case (wordId, (word, genre, count, score, productList)) => 
              List(
                GraphX.generateHash(genre, word),
                word,
                count,
                score,
                productList
              ).mkString(":=:")
          }
          val result = List(
            genreId,
            genre,
            words.mkString("<>")
          ).mkString("\t")
          publishMQ(config, result)
          result.take(100) + "..."
      }
 */
//.collect.foreach(println(_))
      println("----------------------------")
    }


    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * search from solr
   */
  def wordSearch(text: String): String = {
    val encodeText = URLEncoder.encode(text, "UTF-8")
    val request = url("http://10.1.1.175:8983/solr/dmm/select?start=0&rows=30&defType=edismax&fl=title,genre,score,detail&fq=-service:mono&qf=title_ja%5E30.0%20title_cjk%5E12.0%20subtitle_ja%5E20.0%20subtitle_cjk%5E8.0%20comment_ja%5E0.1%20comment_cjk%5E0.1&q=" + encodeText + "&wt=csv")
    val response = http(request OK as.String)
    response.onComplete {
      case Success(msg) => msg
      case Failure(t)   => ""
    }
    Await.result(response, 5.seconds)
  }
  /**
   * tweet to word list
   */
  def kuromojiParser(text: String, id: Long): (String, List[String]) = {
    // @todo modified tokenize
    val tokenizer = UserDic.getInstance()
    val tokens = tokenizer.tokenize(text).toArray
    val wordList = tokens.map { token =>
      token.asInstanceOf[Token]
    }.collect {
      case token if {
        val partOfSpeech = token.getPartOfSpeech
        val normalNoun = (partOfSpeech.indexOf("名詞") > -1 && partOfSpeech.indexOf("一般") > -1)
        val customNoun = partOfSpeech.indexOf("カスタム名詞") > -1
        normalNoun || customNoun
      } =>
        token.asInstanceOf[Token].getSurfaceForm
    }.filter { word =>
      (word.length >= 2) && !(word matches "^[a-zA-Z]+$|^[0-9]+$")
    }.toList
    (text, wordList)
  }
  def publishMQ(config: Config, message: String) {
    val factory = new ConnectionFactory()
    factory.setUsername(config.getString("rabbitmq.username"))
    factory.setPassword(config.getString("rabbitmq.password"))
    factory.setVirtualHost(config.getString("rabbitmq.virtualHost"))
    factory.setHost(config.getString("rabbitmq.host"))
    factory.setPort(config.getInt("rabbitmq.port"))
    val conn = factory.newConnection()
    val channel = conn.createChannel()
    channel.basicPublish("", config.getString("rabbitmq.queue"), null, message.getBytes())
    channel.close()
    conn.close()
  }
}
