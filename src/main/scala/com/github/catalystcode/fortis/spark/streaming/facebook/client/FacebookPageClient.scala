package com.github.catalystcode.fortis.spark.streaming.facebook.client
import java.util.Date

import com.github.catalystcode.fortis.spark.streaming.facebook.FacebookAuth
import facebook4j.{Post, ResponseList}

class FacebookPageClient(
  pageId: String,
  auth: FacebookAuth,
  fields: Set[String] = Set("permalink_url", "message", "created_time"))
extends FacebookClient(auth, fields) {

  override def fetchFacebookResponse(after: Date): ResponseList[Post] = {
    logDebug(s"Fetching posts for $pageId since $after")
    facebook.getPosts(pageId, createReading(after))
  }
}
