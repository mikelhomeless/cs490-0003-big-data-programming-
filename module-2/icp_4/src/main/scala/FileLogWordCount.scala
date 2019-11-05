import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileLogWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("FileLogWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))   // Streaming will execute in each 3 seconds
    val lines = ssc.textFileStream("log/")  //'log/ mean directory name
    val wc = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    wc.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
