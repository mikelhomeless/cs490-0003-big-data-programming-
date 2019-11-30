import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkDataFrame {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Lab2Queries")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.sqlContext.read.option("header", "true").csv("WorldCupMatches.csv").withColumnRenamed("Home Team Name", "Home_Team")
        .withColumnRenamed("Away Team Name", "Away_Team").withColumnRenamed("Away Team Goals", "Away_Team_Goals")
        .withColumnRenamed("Home Team Goals", "Home_Team_Goals").withColumnRenamed("Win conditions", "Win_conditions")
        .withColumnRenamed("Half-time Home Goals", "Half_time_Home_Goals").withColumnRenamed("Half-time Away Goals", "Half_time_Away_Goals")
        .withColumnRenamed("Assistant 1", "Assistant_1").withColumnRenamed("Assistant 2", "Assistant_2")
        .withColumnRenamed("Home Team Initials", "Home_Team_Initials").withColumnRenamed("Away Team Initials", "Away_Team_Initials")

    df.createOrReplaceTempView("worldcup")


    //Shows games where there was a specific Observation
    df.filter(df.Observation != " ").orderBy("DateTime").show()

    //Shows final matches where the home team wins by only one goal
    df.filter(df.Round == "Final").filter(df.Home_Team_Goals == (df.Away_Team_Goals + 1)).show()

    //Shows high-scoring matches by year
    val high1 = df.filter(df.Home_Team_Goals > 3).orderBy("DateTime")
    val high2 = df.filter(df.Away_Team_Goals > 3).orderBy("DateTime")
    val highscores = high1.union(high2)
    high2.show()

    //Shows Average Home and Away team scores
    spark.sqlContext.sql("SELECT avg(Home_Team_Goals), avg(Away_Team_Goals) FROM worldcup").show()

    //Shows games that went into extra time
    spark.sqlContext.sql("SELECT * FROM worldcup WHERE Observation LIKE '%extra time%'").show()

    df.show()

  }
}
