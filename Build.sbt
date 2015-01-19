import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

name := "realtime_recommned"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Atilika Open Source repository" at "http://www.atilika.org/nexus/content/repositories/atilika"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.1.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.1.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.1.0"

// libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.2"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "com.rabbitmq" % "amqp-client" % "2.7.1"

libraryDependencies += "net.databinder.dispatch" %% "dispatch-core" % "0.11.0"

libraryDependencies += "org.atilika.kuromoji" % "kuromoji" % "0.7.7"

assemblySettings
