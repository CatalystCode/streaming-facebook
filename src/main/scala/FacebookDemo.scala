import com.github.catalystcode.fortis.spark.streaming.facebook.FacebookAuth
import org.apache.log4j.{BasicConfigurator, Level, Logger}

object FacebookDemo {
  def main(args: Array[String]) {
    val mode = args.headOption.getOrElse("")

    // configure page for which to ingest posts
    val pageId = "aljazeera"

    // configure interaction with facebook api
    val auth = FacebookAuth(accessToken = System.getenv("FACEBOOK_AUTH_TOKEN"), appId = System.getenv("FACEBOOK_APP_ID"), appSecret = System.getenv("FACEBOOK_APP_SECRET"))

    // configure logging
    BasicConfigurator.configure()
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("libfacebook").setLevel(Level.DEBUG)

    if (mode.contains("standalone")) new FacebookDemoStandalone(pageId, auth).run()
    if (mode.contains("spark")) new FacebookDemoSpark(pageId, auth).run()
  }
}
