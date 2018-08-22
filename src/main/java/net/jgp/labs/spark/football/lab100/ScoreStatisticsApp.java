package net.jgp.labs.spark.football.lab100;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analytics with World Cup 2018.
 * 
 * @author jgp
 */
public class ScoreStatisticsApp {
  private static Logger log = LoggerFactory.getLogger(ScoreStatisticsApp.class);

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    ScoreStatisticsApp app = new ScoreStatisticsApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("World Cup 2018 stats")
        .master("local[*]")
        .getOrCreate();

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/Cup.Russia.Matches.csv");
    df = df.withColumnRenamed("Home Team", "country1")
        .withColumnRenamed("Away Team", "country2")
        .withColumnRenamed("Home Team Goals", "score1")
        .withColumnRenamed("Away Team Goals", "score2")  
        .withColumn("date", functions.split(df.col("Datetime (Brazil)"), ".-.").getItem(1))
        .drop("Datetime (Brazil)");

    log.debug("There were {} games in the Soccer World Cup 2018", df.count());
    df.show(5);

    Dataset<Row> dfReverse = df.withColumnRenamed("country1", "x")
        .withColumnRenamed("country2", "country1")
        .withColumnRenamed("x", "country2")
        .withColumnRenamed("score1", "s")
        .withColumnRenamed("score2", "score1")
        .withColumnRenamed("s", "score2");
    dfReverse.show(5);

    Dataset<Row> combinedDf = df.unionByName(dfReverse)
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
    
    System.out.println("Goals by country");
    Dataset<Row> goalsDf = combinedDf.groupBy("country").sum("score");
    goalsDf = goalsDf.orderBy(goalsDf.col("sum(score)").desc());
    goalsDf.show(5);
  }
}
