package com.revature.questionfour

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
        "1 - Get emoji count for ALL users\n" +
        "2 - Get emoji count for a specific user\n" +
        "3 - Get emoji count by specific emoji\n" +
        "4 - Quit\n")
      val input = StdIn.readInt()
      input match {
        case (input) if (input == 1) =>{
          emojiCountAllStream(spark)
        }
        case (input) if (input == 2) =>{
          println("What user do you want to search for?")
          val user = StdIn.readLine()
          emojiCountOneStream(spark, user)
        }
        case (input) if (input == 3) =>{
          println("What emoji do you want to search for?")
          val userEmoji = StdIn.readLine()
          mostPopularEmojiStream(spark, userEmoji)
        }
        case (input) if (input == 4) =>{
          println("Exiting. . .\n")
          System.exit(0)
        }
        case _ => {
          println("Invalid. Here are your options:")
        }
      }
    }
  }

  /**
    * Gets emoji usage for ALL users in the tweet stream
    *
    * @param spark
    */
  def emojiCountAllStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      downloadTweetStream(bearerToken, queryString = "?tweet.fields=entities&expansions=entities.mentions.username")
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
      .select($"data.text", $"data.entities.mentions.username")
      .filter($"includes".isNotNull)
      .filter($"text" rlike s"${emoji}")
      .select(regexp_replace($"text", s"${notEmoji}", "").as("Removed Words"), $"username")
      .select(regexp_replace($"Removed Words", s"${regexSpace}", " $1").as("Added Space"), $"username")
      .select(split($"Added Space", " ").as("Split"), $"username")
      .select($"Split", explode($"username").as("Username"))
      .select(explode($"Split").as("Emoji"), $"Username")
      .filter($"Emoji" rlike s"${emoji}")
      .filter(!$"Emoji".contains("(") && !$"Emoji".contains(")") && !$"Emoji".contains("|"))
      .groupBy($"Username", $"Emoji")
      .count()
      .sort($"Username", $"Emoji")
      .orderBy(desc("count"), $"Username")
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  /**
    * Gets emoji usage for ONE user from the tweet stream
    *
    * @param spark
    * @param user
    */
  def emojiCountOneStream(spark: SparkSession, user: String): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      downloadTweetStream(bearerToken, queryString = "?tweet.fields=entities&expansions=entities.mentions.username")
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
      .select($"data.text", $"data.entities.mentions.username")
      .filter($"includes".isNotNull)
      .filter($"text" rlike s"${emoji}")
      .select(regexp_replace($"text", s"${notEmoji}", "").as("text2"), $"username")
      .select(regexp_replace($"text2", s"${regexSpace}", " $1").as("split2"), $"username")
      .select(split($"split2", " ").as("split3"), $"username")
      .select($"split3", explode($"username").as("username"))
      .filter($"username" rlike s"$user")
      .select(explode($"split3").as("exploded"), $"username")
      .filter($"exploded" rlike s"${emoji}")
      .filter(!$"exploded".contains("(") && !$"exploded".contains(")") && !$"exploded".contains("|"))
      .groupBy($"username", $"exploded")
      .count()
      .sort($"username", $"exploded")
      .orderBy($"username", desc("count"), $"exploded")
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  /**
    * Search for the user mentioned most with a particular emoji
    *
    * @param spark
    * @param userEmoji
    */
    def mostPopularEmojiStream(spark: SparkSession, userEmoji: String): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      downloadTweetStream(bearerToken, queryString = "?tweet.fields=entities&expansions=entities.mentions.username")
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
      .select($"data.text", $"data.entities.mentions.username")
      .filter($"includes".isNotNull)
      .filter($"text" rlike s"${emoji}")
      .select(regexp_replace($"text", s"${notEmoji}", "").as("Removed Words"), $"username")
      .select(regexp_replace($"Removed Words", s"${regexSpace}", " $1").as("Added Space"), $"username")
      .select(split($"Added Space", " ").as("Split"), $"username")
      .select($"Split", explode($"username").as("Username"))
      .select(explode($"Split").as("Emoji"), $"Username")
      .filter($"Emoji" rlike s"${userEmoji}")
      .filter(!$"Emoji".contains("(") && !$"Emoji".contains(")") && !$"Emoji".contains("|"))
      .groupBy($"Username", $"Emoji")
      .count()
      .sort($"Emoji", $"Username")
      .orderBy(desc("count"), $"Username")
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  /**
    * Downloads twitter data to the twitterstream directory
    *
    * @param bearerToken
    * @param dirname
    * @param linesPerFile
    * @param queryString
    */
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
