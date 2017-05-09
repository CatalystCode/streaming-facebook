import com.github.catalystcode.fortis.spark.streaming.facebook.FacebookAuth
import com.github.catalystcode.fortis.spark.streaming.facebook.client.FacebookPageClient

class FacebookDemoStandalone(pageId: String, auth: FacebookAuth) {
  def run(): Unit = {
    println(new FacebookPageClient(pageId, auth).loadNewFacebooks().toList)
  }
}
