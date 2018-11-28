package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show()

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")
    standings.createTempView("standings")
    session.sql("SELECT league, season, ROUND(AVG(goalsFor), 0) AS avg_goalsFor " +
                "FROM standings " +
                "GROUP BY league, season " +
                "ORDER BY league, season").show()

    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    standings
      .where($"league" === "Ligue 1" && $"position" === 1)
      .groupBy($"team")
      .count()
      .orderBy(desc("count"))
      .show(1)

    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")
    standings
      .where($"position" === 1)
      .groupBy($"league")
      .agg(round(avg($"points")).as("avg_points"))
      .show()

    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?

    val decade = udf[String, String](year => year.toString.dropRight(1).concat("X"))

    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")

    standings
      .where($"position" === 1 || $"position" === 10)
      .groupBy($"league", $"season")
      .agg(sum($"goalsDifference").as("sum_goalsDifference"))
      .withColumn("decade", decade($"season"))
      .groupBy($"league", $"decade")
      .agg(round(avg($"sum_goalsDifference")).as("avg_sum_goalsDifference"))
      .orderBy("league", "decade")
      .show()
  }
}
