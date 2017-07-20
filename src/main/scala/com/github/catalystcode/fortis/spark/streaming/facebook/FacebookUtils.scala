package com.github.catalystcode.fortis.spark.streaming.facebook

import java.util.concurrent.TimeUnit

import com.github.catalystcode.fortis.spark.streaming.PollingSchedule
import com.github.catalystcode.fortis.spark.streaming.facebook.client.FacebookPageClient
import com.github.catalystcode.fortis.spark.streaming.facebook.dto.{FacebookComment, FacebookPost}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object FacebookUtils {
  private val DefaultPageFields = Set("message", "place", "caption", "from", "name")
  private val DefaultCommentsFields = Set("comments")
  private val DefaultPollingSchedule = PollingSchedule(30, TimeUnit.SECONDS)
  private val DefaultPollingWorkers = 1
  private val DefaultStorageLevel = StorageLevel.MEMORY_ONLY

  def createPageStreams(
    ssc: StreamingContext,
    auth: FacebookAuth,
    pageIds: Set[String],
    fields: Set[String] = DefaultPageFields,
    pollingSchedule: PollingSchedule = DefaultPollingSchedule,
    pollingWorkers: Int = DefaultPollingWorkers,
    storageLevel: StorageLevel = DefaultStorageLevel
  ): ReceiverInputDStream[FacebookPost] = {
    new FacebookPostInputDStream(
      ssc = ssc,
      clients = pageIds.map(pageId => new FacebookPageClient(
        pageId = pageId,
        auth = auth,
        fields = fields)),
      pollingSchedule = pollingSchedule,
      pollingWorkers = pollingWorkers,
      storageLevel = storageLevel)
  }

  def createPageStream(
    ssc: StreamingContext,
    auth: FacebookAuth,
    pageId: String,
    fields: Set[String] = DefaultPageFields,
    pollingSchedule: PollingSchedule = DefaultPollingSchedule,
    pollingWorkers: Int = DefaultPollingWorkers,
    storageLevel: StorageLevel = DefaultStorageLevel
  ): ReceiverInputDStream[FacebookPost] = {
    createPageStreams(ssc, auth, Set(pageId), fields, pollingSchedule, pollingWorkers, storageLevel)
  }

  def createCommentsStreams(
    ssc: StreamingContext,
    auth: FacebookAuth,
    pageIds: Set[String],
    fields: Set[String] = DefaultCommentsFields,
    pollingSchedule: PollingSchedule = DefaultPollingSchedule,
    pollingWorkers: Int = DefaultPollingWorkers,
    storageLevel: StorageLevel = DefaultStorageLevel
  ): ReceiverInputDStream[FacebookComment] = {
    new FacebookCommentsInputDStream(
    ssc = ssc,
    clients = pageIds.map(pageId => new FacebookPageClient(
    pageId = pageId,
    auth = auth,
    fields = fields)),
    pollingSchedule = pollingSchedule,
    pollingWorkers = pollingWorkers,
    storageLevel = storageLevel)
  }
}
