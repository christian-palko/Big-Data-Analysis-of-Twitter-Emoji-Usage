package com.revature.questiontwo

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

/**
  * Runner program to answer Question One of our project:
  *   What are the most used emoji currently?
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

    parseTweetStream(spark)
  }

  def parseTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      downloadTweetStream(bearerToken)
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
      .select($"data.text")
      .filter($"text" rlike s"$emoji")
      .select(regexp_replace($"text", s"$notEmoji", "").as("Removed Text"))
      .select(regexp_replace($"Removed Text", s"$regexSpace", " $1 ").as("Added Emoji Space"))
      .select(trim($"Added Emoji Space").as("Trimmed"))
      .select(split($"Trimmed", " ").as("Split"))
      .select(explode($"Split").as("Emoji"))
      .filter($"Emoji" rlike s"$emoji")
      .filter(!$"Emoji".contains("(") && !$"Emoji".contains(")") && !$"Emoji".contains("|"))
      .groupBy($"Emoji")
      .count().as("Count")
      .orderBy(desc("Count"), $"Emoji")
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
      s"https://api.twitter.com/2/tweets/sample/stream"
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