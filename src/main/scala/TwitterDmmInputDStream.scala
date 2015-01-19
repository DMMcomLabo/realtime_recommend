package org.apache.spark.streaming.dmtc

import twitter4j._
import twitter4j.auth.Authorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.FilterQuery

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver

private [streaming]
class TwitterDmmInputDStream (
    @transient ssc_ : StreamingContext,
    twitterAuth: Option[Authorization],
    filters: FilterQuery,
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[Status](ssc_)  {

  private def createOAuthAuthorization(): Authorization = {
    new OAuthAuthorization(new ConfigurationBuilder().build())
  }

  private val authorization = twitterAuth.getOrElse(createOAuthAuthorization())

  override def getReceiver(): Receiver[Status] = {
    new TwitterReceiver(authorization, filters, storageLevel)
  }
}
private [streaming]
class TwitterReceiver(
    twitterAuth: Authorization,
    filters: FilterQuery,
    storageLevel: StorageLevel
  ) extends Receiver[Status](storageLevel) with Logging {

  @volatile private var twitterStream: TwitterStream = _
  @volatile private var stopped = false

  def onStart() {
    try {
      val newTwitterStream = new TwitterStreamFactory().getInstance(twitterAuth)
      newTwitterStream.addListener(new StatusListener {
        def onStatus(status: Status) = {
          store(status)
        }
        // Unimplemented
        def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
        def onTrackLimitationNotice(i: Int) {}
        def onScrubGeo(l: Long, l1: Long) {}
        def onStallWarning(stallWarning: StallWarning) {}
        def onException(e: Exception) {
          if (!stopped) {
            restart("Error receiving tweets", e)
          }
        }
      })

      newTwitterStream.filter(filters)
//      if (filters.size > 0) {
//        newTwitterStream.filter(query)
//      } else {
//        newTwitterStream.sample()
//      }
      setTwitterStream(newTwitterStream)
      logInfo("Twitter receiver started")
      stopped = false
    } catch {
      case e: Exception => restart("Error starting Twitter stream", e)
    }
  }

  def onStop() {
    stopped = true
    setTwitterStream(null)
    logInfo("Twitter receiver stopped")
  }

  private def setTwitterStream(newTwitterStream: TwitterStream) = synchronized {
    if (twitterStream != null) {
      twitterStream.shutdown()
    }
    twitterStream = newTwitterStream
  }
}
