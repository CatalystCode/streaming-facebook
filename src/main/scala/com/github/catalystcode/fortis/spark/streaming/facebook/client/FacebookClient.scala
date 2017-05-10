package com.github.catalystcode.fortis.spark.streaming.facebook.client

import java.util
import java.util.Date

import com.github.catalystcode.fortis.spark.streaming.facebook.{FacebookAuth, Logger}
import facebook4j.{Facebook, FacebookException, FacebookFactory, Post, Reading, ResponseList}
import facebook4j.auth.AccessToken

import collection.JavaConverters._

@SerialVersionUID(100L)
abstract class FacebookClient(
  auth: FacebookAuth,
  fields: Set[String])
extends Serializable with Logger {

  @transient protected lazy val facebook: Facebook = createFacebook()
  @transient private lazy val defaultLookback = new Date(new Date().getTime - 1 * 604800000 /* one week ago */)

  def loadNewFacebooks(after: Option[Date] = None): Iterable[Post] = {
    val allPosts = new util.ArrayList[Post]()

    try {
      var posts = fetchFacebookResponse(after.getOrElse(defaultLookback))
      while (posts != null && posts.getPaging != null) {
        allPosts.addAll(posts)
        logDebug(s"Got another page: ${Option(posts.getPaging.getNext).getOrElse("no")}")
        posts = facebook.fetchNext(posts.getPaging)
      }
    } catch {
      case fbex: FacebookException =>
        logError("Problem fetching response from Facebook", fbex)
    }

    allPosts.asScala
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
