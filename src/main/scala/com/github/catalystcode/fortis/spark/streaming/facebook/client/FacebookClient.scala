package com.github.catalystcode.fortis.spark.streaming.facebook.client

import java.util
import java.util.Date

import com.github.catalystcode.fortis.spark.streaming.facebook.dto.FacebookPost
import com.github.catalystcode.fortis.spark.streaming.facebook.{FacebookAuth, Logger}
import facebook4j._
import facebook4j.auth.AccessToken

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

@SerialVersionUID(100L)
abstract class FacebookClient(
  auth: FacebookAuth,
  fields: Set[String])
extends Serializable with Logger {

  @transient protected lazy val facebook: Facebook = createFacebook()
  @transient private lazy val defaultLookback = new Date(new Date().getTime - 1 * 604800000 /* one week ago */)

  def loadNewFacebooks(after: Option[Date] = None): Iterable[FacebookPost] = {
    val allPosts = ListBuffer[FacebookPost]()

    try {
      var posts = fetchFacebookResponse(after.getOrElse(defaultLookback))
      while (posts != null && posts.getPaging != null) {
        posts.asScala.foreach(post => {
          allPosts += FacebookPost(post, fetchComments(post))
        })
        logDebug(s"Got another page: ${Option(posts.getPaging.getNext).getOrElse("no")}")
        posts = facebook.fetchNext(posts.getPaging)
      }
    } catch {
      case fbex: FacebookException =>
        logError("Problem fetching response from Facebook", fbex)
    }

    allPosts
  }

  private def fetchComments(post: Post): Seq[Comment] = {
    val allComments = new util.ArrayList[Comment]()

    try {
      var comments = post.getComments
      while (comments != null && comments.getPaging != null) {
        allComments.addAll(comments)
        logDebug(s"Got another comments page: ${Option(comments.getPaging.getNext).getOrElse("no")}")
        comments = facebook.fetchNext(comments.getPaging)
      }
    } catch {
      case fbex: FacebookException =>
        logError(s"Problem fetching comments from Facebook for post ${post.getPermalinkUrl}", fbex)
    }

    allComments.asScala
  }

  private def createFacebook(): Facebook = {
    val facebook = new FacebookFactory().getInstance()
    facebook.setOAuthAppId(auth.appId, auth.appSecret)
    facebook.setOAuthAccessToken(new AccessToken(auth.accessToken, null))
    facebook
  }

  protected def createReading(after: Date): Reading = {
    new Reading().since(after).fields(fields.toArray : _*)
  }

  protected def fetchFacebookResponse(after: Date): ResponseList[Post]
}
