import org.apache.spark.sql.SparkSession

object icp_3 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    // Question 1.1
    val df = spark.sqlContext.read.option("header", "true").csv("survey.csv")
    df.show()

    // Question 1.2
    df.write.csv("replacement.csv")

    // Question 1.3
    val df_duplicates = df.dropDuplicates(df.columns)
    if(df.count() > df_duplicates.count()){
      print("There is duplicate data")
    } else {
      print("There is not duplicate data")
    }

    // Question 1.4
    val split = df.randomSplit(Array(1,1))
    val (new_df1, new_df2) = (split(0), split(1))
    val union_df = new_df1.union(new_df2)
    union_df.createOrReplaceTempView("data")
    union_df.sqlContext.sql("SELECT * FROM data ORDER BY Country ASC").show()

    // Question 1.5
    df.groupBy("treatment").count().show()

//
//    // Question 2.1
    new_df1.createOrReplaceTempView("dataFrame_1")
    new_df2.createOrReplaceTempView("dataFrame_2")
    new_df1.sqlContext.sql("SELECT A.Country, count(A.Country) from dataFrame_1 as A inner join dataFrame_2 as B on A.Country=B.Country group by A.Country").show()
    new_df1.sqlContext.sql("SELECT A.state, avg(A.Age) from dataFrame_1 as A inner join dataFrame_2 as B on A.Country=B.Country group by A.state").show()
//
//    // Question 2.2
    print(df.collect()(12).toString())
  }
}
