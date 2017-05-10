package com.github.catalystcode.fortis.spark.streaming.facebook

import java.util.concurrent.TimeUnit

import com.github.catalystcode.fortis.spark.streaming.PollingSchedule
import com.github.catalystcode.fortis.spark.streaming.facebook.client.FacebookPageClient
import com.github.catalystcode.fortis.spark.streaming.facebook.dto.FacebookPost
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object FacebookUtils {
  def createPageStream(
    ssc: StreamingContext,
    auth: FacebookAuth,
    pageId: String,
    fields: Set[String] = Set("message", "place", "caption", "from", "name", "comments"),
    pollingSchedule: PollingSchedule = PollingSchedule(30, TimeUnit.SECONDS),
    pollingWorkers: Int = 1,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): ReceiverInputDStream[FacebookPost] = {
    new FacebookInputDStream(
      ssc = ssc,
      client = new FacebookPageClient(
        pageId = pageId,
        auth = auth,
        fields = fields),
      pollingSchedule = pollingSchedule,
      pollingWorkers = pollingWorkers,
      storageLevel = storageLevel)
  }
}
