package com.github.catalystcode.fortis.spark.streaming

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

case class PollingSchedule(interval: Long, unit: TimeUnit, initialDelay: Long = 1)

// Taken from https://github.com/CatalystCode/streaming-instagram/blob/3873a197212ba5929dd54ec4949f3d1ac10ffc1f/src/main/scala/com/github/catalystcode/fortis/spark/streaming/PollingReceiver.scala
// Put this into a shared library at some point
abstract class PollingReceiver[T](
 pollingSchedule: PollingSchedule,
 pollingWorkers: Int,
 storageLevel: StorageLevel
) extends Receiver[T](storageLevel) {

  private var threadPool: ScheduledThreadPoolExecutor = _

  def onStart(): Unit = {
    threadPool = new ScheduledThreadPoolExecutor(pollingWorkers)

    val pollingThread = new Thread("Polling thread") {
      override def run(): Unit = {
        poll()
      }
    }

    threadPool.scheduleAtFixedRate(
      pollingThread, pollingSchedule.initialDelay,
      pollingSchedule.interval, pollingSchedule.unit)
  }

  def onStop(): Unit = {
    if (threadPool != null) {
      threadPool.shutdown()
    }
  }

  protected def poll(): Unit
}
