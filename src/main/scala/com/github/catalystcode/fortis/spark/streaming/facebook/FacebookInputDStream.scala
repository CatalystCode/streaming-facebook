package com.github.catalystcode.fortis.spark.streaming.facebook

import com.github.catalystcode.fortis.spark.streaming.{PollingReceiver, PollingSchedule}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

private class FacebookReceiver(
  pollingSchedule: PollingSchedule,
  storageLevel: StorageLevel,
  pollingWorkers: Int
) extends PollingReceiver[_](pollingSchedule, pollingWorkers, storageLevel) with Logger {

  override protected def poll(): Unit = {
    throw new Exception("not implemented")
  }
}

class FacebookInputDStream(
  ssc: StreamingContext,
  pollingSchedule: PollingSchedule,
  pollingWorkers: Int,
  storageLevel: StorageLevel
) extends ReceiverInputDStream[_](ssc) {

  override def getReceiver(): Receiver[_] = {
    logDebug("Creating facebook receiver")
    new FacebookReceiver(pollingSchedule, storageLevel, pollingWorkers)
  }
}
