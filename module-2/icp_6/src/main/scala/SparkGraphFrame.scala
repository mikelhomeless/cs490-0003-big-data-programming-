import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object SparkGraphFrame {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    // Question 1
    val stationData = spark.sqlContext.read.option("header", "true").csv("station_data.csv")
      .withColumnRenamed("station_id", "id")
      .dropDuplicates()

    val tripData = spark.sqlContext.read.option("header", "true").csv("trip_data.csv")
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Terminal", "dst")
      .drop("Trip ID", "Start Date","End Station", "End Date","Bike #", "Subscriber Type", "Zip Code", "Start Station")
      .dropDuplicates()

    val g = GraphFrame(stationData, tripData)
    val triangleResults = g.triangleCount.run()

    // Question 2
    triangleResults.select("id", "count").show()

    val gx = g.toGraphX

    // Question 3
    val pathResults = ShortestPaths.run(gx, gx.vertices.map(v => v._1).collect())
    println(pathResults.vertices.collect.mkString("\n"))

    // Question 4
    val rankResults = g.pageRank.resetProbability(0.15).maxIter(2).run()
    rankResults.vertices.select("id", "pagerank").show()
    rankResults.edges.select("src", "dst", "weight").show()

    g.vertices.write.parquet("graphVerticies")
    g.edges.write.parquet("graphedges")
  }
}
