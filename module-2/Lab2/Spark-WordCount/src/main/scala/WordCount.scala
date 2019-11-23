import twitter4j.Status
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

object WordCount {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

//    System.setProperty("hadoop.home.dir","C:\\winutils" )
    // Create the context with a 1 second batch size


    val tweets: DStream[Status] = TwitterUtils.createStream(ssc, None)

    val wc = tweets.map( _.getText()).flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    wc.print()
    ssc.start()
    ssc.awaitTermination()

    //counts.saveAsTextFile("output")
  }
}