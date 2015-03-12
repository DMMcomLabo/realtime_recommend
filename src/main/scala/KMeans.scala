package org.apache.spark.examples

import scala.math.random
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark._

/** Computes an approximation to pi */
object KMeansTest {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = new SparkContext("local", "demo")
    //val conf = new SparkConf().setAppName("KMeans Test")
    //val sc = new SparkContext(conf)
    // Load and parse the data
    val data = sc.textFile("src/main/scala/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    clusters.clusterCenters.foreach {
        center => println(f"${center.toArray.mkString("[", ", ", "]")}%s")
    }
    
  }
}
