import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDataFrame {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Lab2Queries")
      .config("spark.master", "local[2]")
      .getOrCreate()

    val matchesDF = spark.sqlContext.read.option("header", "true").csv("WorldCupMatches.csv")
      .withColumnRenamed("Home Team Name", "HomeTeam")
      .withColumnRenamed("Away Team Name", "AwayTeam")
      .withColumnRenamed("Away Team Goals", "AwayTeamGoals")
      .withColumnRenamed("Home Team Goals", "HomeTeamGoals")
      .withColumnRenamed("Win conditions", "WinConditions")
      .withColumnRenamed("Half-time Home Goals", "HalftimeHomeGoals")
      .withColumnRenamed("Half-time Away Goals", "HalftimeAwayGoals")
      .withColumnRenamed("Assistant 1", "Assistant1")
      .withColumnRenamed("Assistant 2", "Assistant2")
      .withColumnRenamed("Home Team Initials", "HomeTeamInitials")
      .withColumnRenamed("Away Team Initials", "AwayTeamInitials")

    val playersDF = spark.sqlContext.read.option("header", "true").csv("WorldCupPlayers.csv")
      .withColumnRenamed("Team Initials", "TeamInitials")
      .withColumnRenamed("Coach Name", "CoachName")
      .withColumnRenamed("Line-up", "LineUp")
      .withColumnRenamed("Shirt Number", "ShirtNumber")
      .withColumnRenamed("Player Name", "PlayerName")

    val winnersDF = spark.sqlContext.read.option("header", "true").csv("WorldCups.csv")
      .withColumnRenamed("Runners-Up", "RunnersUp")
      .withColumnRenamed("Winner", "SeasonWinner")

    matchesDF.createOrReplaceTempView("worldcupmatches")
    playersDF.createOrReplaceTempView("worldcupplayers")
    winnersDF.createOrReplaceTempView("worldcupwinners")


    //Shows games where there was a specific Observation
    matchesDF.filter("WinConditions != ' '").orderBy("DateTime").show()

    //Shows final matches where the home team wins by only one goal
    matchesDF.filter("Stage == 'Final'").filter("HomeTeamGoals == (AwayTeamGoals + 1)").show()

    //Shows high-scoring matches by year
    val high1 = matchesDF.filter("HomeTeamGoals > 3").orderBy("DateTime")
    val high2 = matchesDF.filter("AwayTeamGoals > 3").orderBy("DateTime")
    val highscores = high1.union(high2)
    high2.show()
    //Shows Average Home and Away team scores
    spark.sqlContext.sql("SELECT avg(HomeTeamGoals), avg(AwayTeamGoals) FROM worldcupmatches").show()
    //Shows games that went into extra time
    spark.sqlContext.sql("SELECT * FROM worldcupmatches WHERE WinConditions LIKE '%extra time%'").show()

      val homeUpset = matchesDF
        .filter("HalftimeHomeGoals < HalftimeAwayGoals AND HomeTeamGoals > AwayTeamGoals")
        .withColumnRenamed("HomeTeam", "Winner")
        .withColumnRenamed("AwayTeam", "Loser")

      val awayUpset = matchesDF
        .filter("HalftimeHomeGoals > HalftimeAwayGoals AND HomeTeamGoals < AwayTeamGoals")
        .withColumnRenamed("AwayTeam", "Winner")
        .withColumnRenamed("HomeTeam", "Loser")
//
//      // Print all of the games that were upsets
      val upsetDF = homeUpset.union(awayUpset)
      upsetDF.show()
//
//      // Of all the World cup winners, what games they won were upsets?
      winnersDF
        .join(
          upsetDF,
          upsetDF("Winner") <=> winnersDF("SeasonWinner") && upsetDF("Year") <=> winnersDF("Year"))
        .select("DateTime", "Stadium", "City", "Winner", "Loser", "HomeTeamGoals", "AwayTeamGoals", "HalftimeHomeGoals", "HalftimeAwayGoals")
        .show()

      // Grab the maximum attendance for each year
      matchesDF
        .groupBy("Year")
        .agg(max("Attendance") as "Attendance")
        .select("Year", "Attendance")
        .show()

      // Get counts of the number of games a referee has been a part of
      matchesDF
        .groupBy("Referee")
        .agg(count("Referee") as "Count")
        .orderBy(desc("Count"))
        .show()

      // Get all players that played in a final game
      matchesDF
        .filter("Stage = 'Final'")
        .join(
            playersDF,
            playersDF("MatchID") <=> matchesDF("MatchID")
        )
        .select("PlayerName", "TeamInitials", "Year", "DateTime")
        .orderBy("Year", "TeamInitials", "PlayerName")
        .show()
  }
}
