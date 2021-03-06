package com.revature.questionfive


import org.apache.spark.sql.SparkSession
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Paths
import java.nio.file.Files
import scala.concurrent.Future
import org.apache.spark.sql.functions._
import scala.io.StdIn

/**
  * Runner program to answer Question Four of our project:
  *   What emoji do users get mentioned with the most?
  * 
  * Takes from Twitter's sampled stream to retrieve tweets in real time,
  *   and parses them down to just emojis, and gets a count per emoji.
  */
object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Team ACCE Q1")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

        var continue = true
    while (continue){
      println("Options:\n" +
        "1 - Get emoji count for all categories\n" +
        "2 - Get emoji count for a specific emoji\n" +
        "3 - Quit\n")
      val input = StdIn.readInt()
      input match {
        case (input) if (input == 1) =>{
          emojiCountAllStream(spark)
        }
        case (input) if (input == 2) =>{
          println("Which emoji would you like to search by?")
          val userEmoji = StdIn.readLine()
          mostPopularEmojiStream(spark, userEmoji)
        }
        case (input) if (input == 3) =>{
          println("Exiting. . .\n")
          System.exit(0)
        }
        case _ => {
          println("Invalid. Here are your options:")
        }
      }
    }
  }

  def emojiCountAllStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      downloadTweetStream(bearerToken, queryString = "?tweet.fields=context_annotations")
    }

    // Routine to wait for a file to appear in the twitterstream folder.
    // Note: Ends the program if no file appears after 30 seconds.
    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while (!filesFoundInDir && (System.currentTimeMillis() - start) < 30000) {
      filesFoundInDir =
        Files.list(Paths.get("twitterstream")).findFirst().isPresent()
        Thread.sleep(500)
    }
    if (!filesFoundInDir) {
      println(
        "Error: Unable to populate tweetstream after 30 seconds. Exiting. . ."
      )
      System.exit(1)
    }

    val staticDf = spark.read.json("twitterstream")
    val streamDf =
      spark.readStream.schema(staticDf.schema).json("twitterstream")

    val emoji = "[(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val notEmoji = "[^(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val regexSpace = "(\\B\uD83D.{1})|(\\B\uD83C.{1})|(\\B\uD83E.{1})"

    streamDf
      .select($"data.text", $"data.context_annotations.domain.name")
      .filter($"name".isNotNull)
      .filter($"text" rlike s"${emoji}")
      .select(regexp_replace($"text", s"${notEmoji}", "").as("text2"), $"name")
      .select(regexp_replace($"text2", s"${regexSpace}", " $1").as("split2"), $"name")
      .select(split($"split2", " ").as("split3"), $"name")
      .select($"split3", explode($"name").as("name"))
      .select(explode($"split3").as("exploded"), $"name")
      .filter($"exploded" rlike s"${emoji}")
      .filter(!$"exploded".contains("(") && !$"exploded".contains(")") && !$"exploded".contains("|"))
      .groupBy($"name", $"exploded")
      .count()
      .sort($"name", $"exploded")
      .orderBy(desc("count"), $"name")
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  def mostPopularEmojiStream(spark: SparkSession, userEmoji: String): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      downloadTweetStream(bearerToken, queryString = "?tweet.fields=context_annotations")
    }

    // Routine to wait for a file to appear in the twitterstream folder.
    // Note: Ends the program if no file appears after 30 seconds.
    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while (!filesFoundInDir && (System.currentTimeMillis() - start) < 30000) {
      filesFoundInDir =
        Files.list(Paths.get("twitterstream")).findFirst().isPresent()
        Thread.sleep(500)
    }
    if (!filesFoundInDir) {
      println(
        "Error: Unable to populate tweetstream after 30 seconds. Exiting. . ."
      )
      System.exit(1)
    }

    val staticDf = spark.read.json("twitterstream")
    val streamDf =
      spark.readStream.schema(staticDf.schema).json("twitterstream")

    val emoji = "[(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val notEmoji = "[^(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val regexSpace = "(\\B\uD83D.{1})|(\\B\uD83C.{1})|(\\B\uD83E.{1})"

    streamDf
      .select($"data.text", $"data.context_annotations.domain.name")
      .filter($"name".isNotNull)
      .filter($"text" rlike s"${emoji}")
      .select(regexp_replace($"text", s"${notEmoji}", "").as("Removed Words"), $"name")
      .select(regexp_replace($"Removed Words", s"${regexSpace}", " $1").as("Added Space"), $"name")
      .select(split($"Added Space", " ").as("Split"), $"name")
      .select($"Split", explode($"name").as("Category"))
      .select(explode($"Split").as("Emoji"), $"Category")
      .filter($"Emoji" rlike s"${userEmoji}")
      .filter(!$"Emoji".contains("(") && !$"Emoji".contains(")") && !$"Emoji".contains("|"))
      .groupBy($"Category", $"Emoji")
      .count()
      .sort($"Emoji", $"Category")
      .orderBy(desc("count"), $"Category")
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  def downloadTweetStream(
      bearerToken: String,
      dirname: String = "twitterstream",
      linesPerFile: Int = 1000,
      queryString: String = ""
  ) = {
    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
    val uriBuilder: URIBuilder = new URIBuilder(
      s"https://api.twitter.com/2/tweets/sample/stream$queryString"
    )
    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (null != entity) {
      val reader = new BufferedReader(
        new InputStreamReader(entity.getContent())
      )
      var line = reader.readLine()
      var filewWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis()
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          filewWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(
              s"$dirname/tweetstream-$millis-${lineNumber / linesPerFile}"
            )
          )
          filewWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }

        filewWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
    }
  }
}