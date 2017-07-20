package com.github.catalystcode.fortis.spark.streaming.facebook.dto

import facebook4j.Post

case class FacebookPost(
  pageId: String,
  post: Post
)
