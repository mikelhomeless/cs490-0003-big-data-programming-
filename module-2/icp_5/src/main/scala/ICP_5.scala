import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit}
import org.graphframes.GraphFrame
import org.apache.log4j.{Logger, Level}

object ICP_5 {
  def main(args: Array[String])  {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")




    // Question 1 thru 4
    val input = spark.sqlContext.read.option("header", "true").csv("station_data.csv")
      .withColumnRenamed("station_id", "id")
      .withColumn("lat-long", concat(col("lat"), lit(", "), col("long")))
      .drop("lat", "long")
      .dropDuplicates()

    val output = spark.sqlContext.read.option("header", "true").csv("trip_data.csv")
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Terminal", "dst")
      .dropDuplicates()

    input.show()
    output.show()

    input.coalesce(1).write.csv("modded_input_data.csv")
    output.coalesce(1).write.csv("modded_output_data.csv")

//  Question 6 thru 8
    val g = GraphFrame(input, output)
    g.vertices.show()
    g.edges.show()

    // quetions 9 and 10
    g.inDegrees.show()
    g.outDegrees.show()

    // Question 11
    val motif: DataFrame = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motif.dropDuplicates().show()
  }
}
