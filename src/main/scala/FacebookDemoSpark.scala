import com.github.catalystcode.fortis.spark.streaming.facebook.{FacebookAuth, FacebookUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

class FacebookDemoSpark(pageIds: Set[String], auth: FacebookAuth) {
  def run(): Unit = {
    // set up the spark context and streams
    val conf = new SparkConf().setAppName("Facebook Spark Streaming Demo Application")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    FacebookUtils.createPageStreams(ssc, auth, pageIds).map(x => s"Post: ${x.post.getPermalinkUrl}").print()

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }

}
