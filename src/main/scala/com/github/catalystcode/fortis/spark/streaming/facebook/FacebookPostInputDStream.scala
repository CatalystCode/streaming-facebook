package com.github.catalystcode.fortis.spark.streaming.facebook

import java.util.Date

import com.github.catalystcode.fortis.spark.streaming.facebook.client.FacebookPageClient
import com.github.catalystcode.fortis.spark.streaming.facebook.dto.FacebookPost
import com.github.catalystcode.fortis.spark.streaming.{PollingReceiver, PollingSchedule}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

private class FacebookPostReceiver(
  clients: Set[FacebookPageClient],
  pollingSchedule: PollingSchedule,
  storageLevel: StorageLevel,
  pollingWorkers: Int
) extends PollingReceiver[FacebookPost](pollingSchedule, pollingWorkers, storageLevel) with Logger {

  @volatile private var lastIngestedDate: Option[Date] = None

  override protected def poll(): Unit = {
    clients.par.foreach(_
      .loadNewFacebookPosts(lastIngestedDate)
      .filter(x => {
        logDebug(s"Got facebook ${x.post.getPermalinkUrl} from page ${x.pageId} time ${x.post.getCreatedTime}")
        isNew(x)
      })
      .foreach(x => {
        logInfo(s"Storing facebook ${x.post.getPermalinkUrl}")
        store(x)
        markStored(x)
      })
    )
  }

  private def isNew(item: FacebookPost) = {
    lastIngestedDate.isEmpty || item.post.getCreatedTime.after(lastIngestedDate.get)
  }

  private def markStored(item: FacebookPost): Unit = {
    if (isNew(item)) {
      lastIngestedDate = Some(item.post.getCreatedTime)
      logDebug(s"Updating last ingested date to ${item.post.getCreatedTime}")
    }
  }
}

class FacebookPostInputDStream(
  ssc: StreamingContext,
  clients: Set[FacebookPageClient],
  pollingSchedule: PollingSchedule,
  pollingWorkers: Int,
  storageLevel: StorageLevel
) extends ReceiverInputDStream[FacebookPost](ssc) {

  override def getReceiver(): Receiver[FacebookPost] = {
    logDebug("Creating facebook receiver")
    new FacebookPostReceiver(clients, pollingSchedule, storageLevel, pollingWorkers)
  }
}
