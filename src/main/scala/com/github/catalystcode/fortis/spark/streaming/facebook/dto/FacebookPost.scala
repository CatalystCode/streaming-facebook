package com.github.catalystcode.fortis.spark.streaming.facebook.dto

import facebook4j.{Comment, Post}

case class FacebookPost(
  post: Post,
  comments: Seq[Comment]
)
