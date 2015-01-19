
package org.apache.spark.streaming.dmtc

import twitter4j.Status
import twitter4j.auth.Authorization
import twitter4j.FilterQuery
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}

object TwitterDmmUtils {

  def createStream(
      ssc: StreamingContext,
      twitterAuth: Option[Authorization],
      filters: FilterQuery = null,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[Status] = {
    new TwitterDmmInputDStream(ssc, twitterAuth, filters, storageLevel)
  }
  def createStream(jssc: JavaStreamingContext): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None)
  }
  def createStream(jssc: JavaStreamingContext, filters: FilterQuery
      ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, filters)
  }
  def createStream(
      jssc: JavaStreamingContext,
      filters: FilterQuery,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, filters, storageLevel)
  }
  def createStream(jssc: JavaStreamingContext, twitterAuth: Authorization
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth))
  }
  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      filters: FilterQuery
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), filters)
  }

  def createStream(
      jssc: JavaStreamingContext,
      twitterAuth: Authorization,
      filters: FilterQuery,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), filters, storageLevel)
  }
}
