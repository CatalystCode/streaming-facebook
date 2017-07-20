package com.github.catalystcode.fortis.spark.streaming.facebook

import java.util.Date

import com.github.catalystcode.fortis.spark.streaming.facebook.client.FacebookPageClient
import com.github.catalystcode.fortis.spark.streaming.facebook.dto.FacebookComment
import com.github.catalystcode.fortis.spark.streaming.{PollingReceiver, PollingSchedule}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

private class FacebookCommentsReceiver(
  clients: Set[FacebookPageClient],
  pollingSchedule: PollingSchedule,
  storageLevel: StorageLevel,
  pollingWorkers: Int
) extends PollingReceiver[FacebookComment](pollingSchedule, pollingWorkers, storageLevel) with Logger {

  @volatile private var lastIngestedDate: Option[Date] = None

  override protected def poll(): Unit = {
    clients.par.foreach(_
      .loadNewFacebookComments(lastIngestedDate)
      .filter(x => {
        logDebug(s"Got comment with id ${x.comment.getId} from page ${x.pageId}")
        isNew(x)
      })
      .foreach(x => {
        logInfo(s"Storing comment ${x.comment.getId} from page ${x.pageId}")
        store(x)
        markStored(x)
      })
    )
  }

  private def isNew(item: FacebookComment) = {
    lastIngestedDate.isEmpty || item.comment.getCreatedTime.after(lastIngestedDate.get)
  }

  private def markStored(item: FacebookComment): Unit = {
    if (isNew(item)) {
      lastIngestedDate = Some(item.comment.getCreatedTime)
      logDebug(s"Updating last ingested date to ${lastIngestedDate.get}")
    }
  }
}

class FacebookCommentsInputDStream(
  ssc: StreamingContext,
  clients: Set[FacebookPageClient],
  pollingSchedule: PollingSchedule,
  pollingWorkers: Int,
  storageLevel: StorageLevel
) extends ReceiverInputDStream[FacebookComment](ssc) {

  override def getReceiver(): Receiver[FacebookComment] = {
    logDebug("Creating facebook receiver")
    new FacebookCommentsReceiver(clients, pollingSchedule, storageLevel, pollingWorkers)
  }
}
