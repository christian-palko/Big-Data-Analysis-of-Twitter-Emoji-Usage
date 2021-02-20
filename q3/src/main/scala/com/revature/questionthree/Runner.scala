package com.revature.questionthree

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrame
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import scala.concurrent.Future
import scala.util.matching.Regex
import scala.io.StdIn

/**
  * Runner program to answer Question Three of our project:
  *   What is the proportion of emojis used per word?
  * Takes from Twitter's sampled stream to retrieve tweets in real time,
  *   and parses either words or emoji depending on user input.
  */
object Runner {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Hello Spark SQL")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    var continue = true
    while (continue){
      println("Options:\n" +
        "1 - Get emoji count\n" +
        "2 - Get word count\n" +
        "3 - Quit\n")
      val input = StdIn.readInt()
      input match {
        case (input) if (input == 1) =>{
          emojiTweetStream(spark)
        }
        case (input) if (input == 2) =>{
          textTweetStream(spark)
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

  def emojiTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(
        bearerToken,
        queryString = "?tweet.fields=context_annotations"
      )
    }

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

    // Count the number of emojis in the tweets
    val emoji = "[(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val notEmoji = "[^(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val regexSpace = "(\\B\uD83D.{1})|(\\B\uD83C.{1})|(\\B\uD83E.{1})"

    streamDf
      .select($"data.text")
      .select(regexp_replace($"text", s"${notEmoji}", "").as("text2"))
      .select(regexp_replace($"text2", s"${regexSpace}", " $1").as("split2"))
      .select(split($"split2", " ").as("split5"))
      .select(explode($"split5").as("exploded"))
      .filter($"exploded" rlike s"${emoji}")
      .filter(!$"exploded".contains("(") && !$"exploded".contains(")"))
      .agg(count("exploded").as("Emoji Count"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  def textTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(
        bearerToken,
        queryString = "?tweet.fields=context_annotations"
      )
    }

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

    // Count the number of words in tweets
    val randomThings = "[(\\s)(\\p{C})(\\p{Cntrl}&&[^\r\n\t])()]"
    val notWords = "^[A-Za-z0-9']+$"

    streamDf
      .select($"data.text")
      .explode("text", "word")((line: String) => line.split(" ")).as("Split")
      .select(regexp_replace($"word", s"${randomThings}", "").as("Words"))
      .filter($"Words" rlike s"${notWords}").as("Words")
      .agg(count("Words").as("Word Count"))
      .writeStream
      .outputMode("complete")
      .option("numRows", 50)
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  def tweetStreamToDir(
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
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis()
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(
              s"$dirname/tweetstream-$millis-${lineNumber / linesPerFile}"
            )
          )
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }

        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }

    }
  }

}
