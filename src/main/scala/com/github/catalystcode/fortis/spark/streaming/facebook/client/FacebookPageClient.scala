package com.github.catalystcode.fortis.spark.streaming.facebook.client
import java.util.Date

import com.github.catalystcode.fortis.spark.streaming.facebook.FacebookAuth
import facebook4j.{Post, Reading, ResponseList}

class FacebookPageClient(pageId: String, auth: FacebookAuth) extends FacebookClient(auth) {
  override def fetchFacebookResponse(after: Date): ResponseList[Post] = {
    logDebug(s"Fetching posts for $pageId since $after")
    facebook.getPosts(pageId, new Reading().since(after))
  }
}
