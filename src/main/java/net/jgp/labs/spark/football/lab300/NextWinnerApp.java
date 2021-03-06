package net.jgp.labs.spark.football.lab300;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finding out the next WorldCup winner.
 * 
 * @author jgp
 */
public class NextWinnerApp {
  private static Logger log = LoggerFactory.getLogger(NextWinnerApp.class);

  /**
   * main() is your entry point to the application. This is meant as a joke...
   * 
   * @param args
   */
  public static void main(String[] args) {
    NextWinnerApp app = new NextWinnerApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Next winner of the World Cup")
        .master("local[*]")
        .getOrCreate();

    SparkContext sc = spark.sparkContext();
    System.out.println("Running Spark v" + sc.version());

    System.out.println("Running in " + sc.deployMode());

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> worldCupRussiaMatchesDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/Cup.Russia.Matches.csv");
    worldCupRussiaMatchesDf = worldCupRussiaMatchesDf
        .withColumnRenamed("Home Team", "country1")
        .withColumnRenamed("Away Team", "country2")
        .withColumnRenamed("Home Team Goals", "score1")
        .withColumnRenamed("Away Team Goals", "score2")
        .withColumn("date", functions.split(worldCupRussiaMatchesDf.col(
            "Datetime (Brazil)"), ".-.")
            .getItem(1))
        .drop("Datetime (Brazil)");

    log.debug("There were {} games in the Soccer World Cup 2018",
        worldCupRussiaMatchesDf.count());
    worldCupRussiaMatchesDf.show(5);

    Dataset<Row> worldCupRussiaTeamsDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/Cup.Russia.Teams.csv");
    worldCupRussiaTeamsDf.show(5);

    Dataset<Row> fixtureDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/Fixture.csv");
    fixtureDf.show(5);

    Dataset<Row> playersScoreDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/Players_Score.csv");
    playersScoreDf.show(5);

    Dataset<Row> playersStatsDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/Players_Stats.csv");
    playersStatsDf.show(5);

    Dataset<Row> playersDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/Players.csv");
    playersDf.show(5);

    Dataset<Row> teamsDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/Teams.csv");
    teamsDf.show(5);

    Dataset<Row> worldCupMatchesDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/WorldCupMatches.csv");
    worldCupMatchesDf.show(5);

    Dataset<Row> worldCupPlayersDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/WorldCupPlayers.csv");
    worldCupPlayersDf.show(5);

    Dataset<Row> worldCupsDf = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/WorldCups.csv");
    worldCupsDf.show(5);

    Dataset<Row> dfReverse = worldCupRussiaMatchesDf.withColumnRenamed(
        "country1", "x")
        .withColumnRenamed("country2", "country1")
        .withColumnRenamed("x", "country2")
        .withColumnRenamed("score1", "s")
        .withColumnRenamed("score2", "score1")
        .withColumnRenamed("s", "score2");
    dfReverse.show(5);

    Dataset<Row> combinedDf = worldCupRussiaMatchesDf.unionByName(dfReverse)
        .withColumnRenamed("country1", "country")
        .drop("country2")
        .withColumnRenamed("score1", "score")
        .drop("score2");
    combinedDf.show(5);
    log.debug("There were {} interactions in the Soccer World Cup 2018",
        combinedDf.count());

    Dataset<Row> franceScoreDf = combinedDf.filter("country='France'");
    franceScoreDf.show();

    Dataset<Row> mostPlayedDf = combinedDf.groupBy("country").count();
    mostPlayedDf = mostPlayedDf.orderBy(mostPlayedDf.col("count").desc());
    mostPlayedDf.show(5);

    Dataset<Row> goalsDf = combinedDf.groupBy("country").sum("score");
    goalsDf = goalsDf.orderBy(goalsDf.col("sum(score)").desc());
    goalsDf.show(5);

    System.out.println("And the winner is...");
    try {
      Thread.sleep(1434);
    } catch (InterruptedException e) {
      // oh well, let's ignore it
    }

    System.out.println("France");
  }
}
