package com.github.catalystcode.fortis.spark.streaming.facebook.client

import java.util
import java.util.Date

import com.github.catalystcode.fortis.spark.streaming.facebook.dto.{FacebookComment, FacebookPost}
import com.github.catalystcode.fortis.spark.streaming.facebook.{FacebookAuth, Logger}
import facebook4j._
import facebook4j.auth.AccessToken

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

@SerialVersionUID(100L)
class FacebookPageClient(
  pageId: String,
  auth: FacebookAuth,
  fields: Set[String])
extends Serializable with Logger {

  @transient protected lazy val facebook: Facebook = createFacebook()
  @transient private lazy val defaultLookback = new Date(new Date().getTime - 2 * 3600000 /* two hours */)
  @transient private lazy val defaultFields = Set("permalink_url", "created_time")

  def loadNewFacebookPosts(after: Option[Date] = None): Iterable[FacebookPost] = {
    val allPosts = ListBuffer[FacebookPost]()

    try {
      var posts = fetchFacebookResponse(after.getOrElse(defaultLookback))
      while (posts != null && posts.getPaging != null) {
        posts.asScala.foreach(post => {
          allPosts += FacebookPost(pageId, post)
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

  def loadNewFacebookComments(after: Option[Date] = None): Iterable[FacebookComment] = {
    val allComments = ListBuffer[FacebookComment]()

    try {
      var posts = fetchFacebookResponse(after.getOrElse(defaultLookback))
      while (posts != null && posts.getPaging != null) {
        posts.asScala.foreach(post => {
          fetchComments(post).foreach(comment => {
            allComments += FacebookComment(pageId, post.getId, comment)
          })
        })
        logDebug(s"Got another page: ${Option(posts.getPaging.getNext).getOrElse("no")}")
        posts = facebook.fetchNext(posts.getPaging)
      }
    } catch {
      case fbex: FacebookException =>
        logError("Problem fetching response from Facebook", fbex)
    }

    allComments
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
    // todo: reduce limit if we got a too-large-request error
    new Reading().since(after).fields((defaultFields ++ fields).toArray : _*).limit(100)
  }

  protected def fetchFacebookResponse(after: Date): ResponseList[Post] = {
    logDebug(s"Fetching posts for $pageId since $after")
    facebook.getPosts(pageId, createReading(after))
  }
}
