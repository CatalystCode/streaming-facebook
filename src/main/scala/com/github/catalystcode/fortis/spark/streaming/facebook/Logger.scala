package com.github.catalystcode.fortis.spark.streaming.facebook

import org.apache.log4j.LogManager

trait Logger {
  @transient private lazy val log = LogManager.getLogger("libfacebook")

  def logDebug(message: String): Unit = log.debug(message)
  def logInfo(message: String): Unit = log.info(message)
  def logError(message: String, throwable: Throwable): Unit = log.error(message, throwable)
}
