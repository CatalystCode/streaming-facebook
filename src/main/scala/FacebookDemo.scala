import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FacebookDemo {
  def main(args: Array[String]) {
    val mode = args.headOption.getOrElse("")

    // configure logging
    BasicConfigurator.configure()
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("libfacebook").setLevel(Level.DEBUG)

    if (mode.contains("standalone")) {
    }

    if (mode.contains("spark")) {
      // set up the spark context and streams
      val conf = new SparkConf().setAppName("Facebook Spark Streaming Demo Application")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(1))

      // run forever
      ssc.start()
      ssc.awaitTermination()
    }
  }
}
