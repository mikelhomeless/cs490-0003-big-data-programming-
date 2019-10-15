import org.apache.spark._

object AddedFilter {

  def main(args: Array[String]) {
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("input.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count

    // apply a filter that limits the word count to words greater than 1 character
    val counts = words.map(word => (word, 1)).filter(pair => pair._1.length() > 1).reduceByKey({case (x, y) => x + y})
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("output")
  }
}