package com.github.catalystcode.fortis.spark.streaming.facebook.dto

import facebook4j.{Comment, Post}

case class FacebookPost(
  pageId: String,
  post: Post,
  comments: Seq[Comment]
)
