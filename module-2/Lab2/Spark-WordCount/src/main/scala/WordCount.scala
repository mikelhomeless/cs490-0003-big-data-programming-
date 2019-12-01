import twitter4j.Status
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

object WordCount {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[2]").setAppName("Hadoop Sqoop Poop")
    val ssc = new StreamingContext(conf, Seconds(5))

//    System.setProperty("hadoop.home.dir","C:\\winutils" )

    val tweetStream: DStream[Status] = TwitterUtils.createStream(ssc, None)

    val tweets = tweetStream.map(_.getText)
    val counts = tweets.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey({case (x, y) => x + y})

    counts.print()
    counts.saveAsTextFiles("Outputs/run")
    ssc.start()
    ssc.awaitTermination()
  }
}