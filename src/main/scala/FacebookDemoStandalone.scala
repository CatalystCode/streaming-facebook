import java.util.Date

import com.github.catalystcode.fortis.spark.streaming.facebook.FacebookAuth
import com.github.catalystcode.fortis.spark.streaming.facebook.client.FacebookPageClient

class FacebookDemoStandalone(pageId: String, auth: FacebookAuth) {
  def run(): Unit = {
    val date = Some(new Date(new Date().getTime - 3600000 /* 1 hour */))
    println(new FacebookPageClient(pageId, auth, Set("place", "comments", "message")).loadNewFacebooks(date).toList)
  }
}
