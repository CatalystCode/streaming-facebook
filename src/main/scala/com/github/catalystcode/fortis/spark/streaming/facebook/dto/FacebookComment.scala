package com.github.catalystcode.fortis.spark.streaming.facebook.dto

import facebook4j.Comment

case class FacebookComment(
  pageId: String,
  postId: String,
  comment: Comment
)
