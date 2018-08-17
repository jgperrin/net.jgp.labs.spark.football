package net.jgp.labs.spark.football.lab200;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author jgp
 */
public class HistoricScoreStatisticsApp {
  private static Logger log = LoggerFactory.getLogger(
      HistoricScoreStatisticsApp.class);

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    HistoricScoreStatisticsApp app = new HistoricScoreStatisticsApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Historic score statistics")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> historicalScoresDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/WorldCupMatches.csv");
    historicalScoresDf = historicalScoresDf
        .orderBy(historicalScoresDf.col("Year").desc())
        .drop("Datetime")
        .drop("Win conditions")
        .drop("Half-time Home Goals")
        .drop("Half-time Away Goals")
        .drop("Referee")
        .drop("Assistant 1")
        .drop("Assistant 2")
        .drop("RoundID")
        .drop("MatchID")
        .drop("Home Team Initials")
        .drop("Away Team Initials")
        .withColumnRenamed("Home Team Name", "country1")
        .withColumnRenamed("Away Team Name", "country2")
        .withColumnRenamed("Home Team Goals", "score1")
        .withColumnRenamed("Away Team Goals", "score2");
    log.debug("There were {} games in the history of the Soccer World Cup ",
        historicalScoresDf.count());
    historicalScoresDf.show(5);

    Dataset<Row> lastWorldCupScoresDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/Cup.Russia.Matches.csv");
    lastWorldCupScoresDf = lastWorldCupScoresDf
        .withColumnRenamed("Home Team", "country1")
        .withColumnRenamed("Away Team", "country2")
        .withColumnRenamed("Home Team Goals", "score1")
        .withColumnRenamed("Away Team Goals", "score2")
        .drop("Win Conditions")
        .drop("Penalty")
        .drop("Win")
        .drop("Total Goals")
        .drop("Datetime (Brazil)")
        .withColumn("Year", functions.lit(2018));
    log.debug("There were {} games at the Soccer World Cup 2018",
        lastWorldCupScoresDf.count());
    lastWorldCupScoresDf.show(5);

    Dataset<Row> fullHistoricalScoresDf = historicalScoresDf
        .unionByName(lastWorldCupScoresDf);
    log.debug("Up to now, there were {} games at the Soccer World Cups",
        fullHistoricalScoresDf.count());
    fullHistoricalScoresDf.show(5);

    Dataset<Row> dfReverse = fullHistoricalScoresDf.withColumnRenamed(
        "country1", "x")
        .withColumnRenamed("country2", "country1")
        .withColumnRenamed("x", "country2")
        .withColumnRenamed("score1", "s")
        .withColumnRenamed("score2", "score1")
        .withColumnRenamed("s", "score2");
    Dataset<Row> combinedDf = fullHistoricalScoresDf.unionByName(dfReverse)
        .withColumnRenamed("country1", "country")
        .drop("country2")
        .withColumnRenamed("score1", "score")
        .drop("score2");

    Dataset<Row> franceScoreDf = combinedDf.filter("country='France'");
    log.debug("France played {} games at Soccer World Cups",
        franceScoreDf.count());

    Dataset<Row> mostPlayedDf = combinedDf.groupBy("country").count();
    mostPlayedDf = mostPlayedDf.orderBy(mostPlayedDf.col("count").desc());
    mostPlayedDf.show(5);

    Dataset<Row> goalsDf = combinedDf.groupBy("country").sum("score");
    goalsDf = goalsDf.orderBy(goalsDf.col("sum(score)").desc());
    goalsDf.show(5);

    Dataset<Row> attendanceDf = combinedDf.groupBy("Year").sum("Attendance");
    attendanceDf = attendanceDf
        .orderBy(attendanceDf.col("Year"))
        .withColumnRenamed("sum(Attendance)", "Attendance");
    attendanceDf.show(20);
  }
}
