package com.test.spark.wiki.extracts

import java.net.URL
import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)
    import session.implicits._

    getLeagues
      // TODO Q1 Transformer cette seq en dataset
      .toDS()
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
      .flatMap {
        case (season, (league, url)) =>
          try {
            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
            // ATTENTION:
            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
            //  - Il faut veillez à recuperer uniquement le classement général
            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage
            val doc = Jsoup.connect(url).get()
            var elem = new org.jsoup.select.Elements
            var found = false
            var i = 0

            var table = doc.select("div.mw-parser-output table.wikitable.gauche")

            if (table.isEmpty) {
              table = doc.select("div.mw-parser-output table.wikitable")
            }

            while (!found & i<table.size()){
              val caption = table.get(i).select("caption")
              if (!caption.isEmpty){
                if (caption.text() == "Classement"){
                  elem = table.get(i).select("tbody tr")
                  found = true
                }
              }
              i += 1
            }

            var sequence_a_retourner: Seq[LeagueStanding] = Seq()

            for (i <- 1 until elem.size()) {
              val data = elem.get(i).select("td")
              sequence_a_retourner = sequence_a_retourner :+ LeagueStanding(
                league,
                season,
                data.get(0).text().toInt,
                data.get(1).text().replaceAll("""\b\w{1}\b""","").trim(),
                data.get(2).text().toInt,
                data.get(3).text().toInt,
                data.get(4).text().toInt,
                data.get(5).text().toInt,
                data.get(6).text().toInt,
                data.get(7).text().toInt,
                data.get(8).text().toInt,
                data.get(9).text().toInt
              )
            }

            sequence_a_retourner
          } catch {
            case _: Throwable =>
              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
              // Sur un cluster EMR, dépendant de la configuration du cluster, les logs d'erreurs seront accesible soit:
              //   - sur le master node
              //   - sur un bucket S3
              logger.warn(s"Can't parse season $season from $url")
              Seq.empty
          }
      }
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?
    // Le format parquet est un format de stockage colonne.
    // Il permet notamment d'augmenter les performances de certaines requetes lorsqu'on a besoin par exemple
    // d'acceder à certaines colonnes seulement au lieu de toute la table ou un aggrégation sur une colonne particulière.

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
    // L'utilisation d'un dataset Spark est recommandé lorsque l'on travaille avec un gros volume de données.
    // Grace à un dataset Spark, la vitesse de calculs de certaines fonctions sera bien supérieur à un simple environment Scala.
  }

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    val inputStream = new File("/home/amir/IdeaProjects/spark-wiki-extracts/src/main/resources/leagues.yaml")
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty("name") name: String,
                       @JsonProperty("url") url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)
