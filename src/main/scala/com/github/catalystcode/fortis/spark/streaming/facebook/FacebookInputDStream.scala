package com.github.catalystcode.fortis.spark.streaming.facebook

import java.util.Date

import com.github.catalystcode.fortis.spark.streaming.facebook.client.FacebookClient
import com.github.catalystcode.fortis.spark.streaming.{PollingReceiver, PollingSchedule}
import facebook4j.Post
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

private class FacebookReceiver(
  client: FacebookClient,
  pollingSchedule: PollingSchedule,
  storageLevel: StorageLevel,
  pollingWorkers: Int
) extends PollingReceiver[Post](pollingSchedule, pollingWorkers, storageLevel) with Logger {

  @volatile private var lastIngestedDate: Option[Date] = None

  override protected def poll(): Unit = {
    client
      .loadNewFacebooks(lastIngestedDate)
      .filter(x => {
        logDebug(s"Got facebook ${x.getPermalinkUrl} from time ${x.getCreatedTime}")
        isNew(x)
      })
      .foreach(x => {
        logInfo(s"Storing facebook ${x.getPermalinkUrl}")
        store(x)
        markStored(x)
      })
  }

  private def isNew(item: Post) = {
    lastIngestedDate.isEmpty || item.getCreatedTime.after(lastIngestedDate.get)
  }

  private def markStored(item: Post): Unit = {
    if (isNew(item)) {
      lastIngestedDate = Some(item.getCreatedTime)
      logDebug(s"Updating last ingested date to ${item.getCreatedTime}")
    }
  }
}

class FacebookInputDStream(
  ssc: StreamingContext,
  client: FacebookClient,
  pollingSchedule: PollingSchedule,
  pollingWorkers: Int,
  storageLevel: StorageLevel
) extends ReceiverInputDStream[Post](ssc) {

  override def getReceiver(): Receiver[Post] = {
    logDebug("Creating facebook receiver")
    new FacebookReceiver(client, pollingSchedule, storageLevel, pollingWorkers)
  }
}
