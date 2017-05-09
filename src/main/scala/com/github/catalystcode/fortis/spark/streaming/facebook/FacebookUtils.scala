package com.github.catalystcode.fortis.spark.streaming.facebook

import java.util.concurrent.TimeUnit

import com.github.catalystcode.fortis.spark.streaming.PollingSchedule
import com.github.catalystcode.fortis.spark.streaming.facebook.client.FacebookPageClient
import facebook4j.Post
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object FacebookUtils {
  def createPageStream(
    ssc: StreamingContext,
    auth: FacebookAuth,
    pageId: String,
    pollingSchedule: PollingSchedule = PollingSchedule(30, TimeUnit.SECONDS),
    pollingWorkers: Int = 1,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): ReceiverInputDStream[Post] = {
    new FacebookInputDStream(
      ssc = ssc,
      client = new FacebookPageClient(
        pageId = pageId,
        auth = auth),
      pollingSchedule = pollingSchedule,
      pollingWorkers = pollingWorkers,
      storageLevel = storageLevel)
  }
}
