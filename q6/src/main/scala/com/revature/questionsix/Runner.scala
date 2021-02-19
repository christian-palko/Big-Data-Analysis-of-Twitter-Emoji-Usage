package com.revature.questionsix

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameReader
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
import scala.io.StdIn

/**
  * Runner file to answer Question Six of the project:
  *   What emojis are used most, grouped by country?
  *   Bonus: Return a single country in the results, 
  *     or remove a country from the results.
  * 
  * Uses twitter's sample stream to get real time data.
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
        "1 - Get emoji count for all countries\n" +
        "2 - Get emoji count for ONE country\n" +
        "3 - Get emoji count for all countries BUT this one\n" +
        "4 - Quit\n")
      val input = StdIn.readInt()
      input match {
        case (input) if (input == 1) =>{
          emojiCountAllStream(spark)
        }
        case (input) if (input == 2) =>{
          println("Which country would you like to see emoji counts for?")
          val userCountry = StdIn.readLine()
          emojiCountOneStream(spark, userCountry)
        }
        case (input) if (input == 3) =>{
          println("Which country would you like to exclude from the count?")
          val userCountry = StdIn.readLine()
          emojiCountNotStream(spark, userCountry)
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

  def emojiCountAllStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
tweetStreamToDir(bearerToken, queryString = "?tweet.fields=geo&expansions=geo.place_id&place.fields=country")    }

    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000){
      filesFoundInDir = Files.list(Paths.get("twitterstream")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir){
      println("Error: Unable to populate tweetstream after 30 seconds. Exiting. . .")
      System.exit(1)
    }

    val staticDf = spark.read.json("twitterstream")

    val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstream")

    val emoji = "[(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val notEmoji = "[^(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val regexSpace = "(\\B\uD83D.{1})|(\\B\uD83C.{1})|(\\B\uD83E.{1})"

    streamDf
      .select($"data.text", $"includes.places.country")
      .filter($"includes".isNotNull)
      .select(regexp_replace($"text", s"${notEmoji}", "").as("Removed Words"), $"Country")
      .select(regexp_replace($"Removed Words", s"${regexSpace}", " $1").as("Added Spaces"), $"Country")
      .select(split($"Added Spaces", " ").as("Split"), $"Country")
      .select(explode($"Split").as("Emoji"), $"Country")
      .filter($"Emoji" rlike s"${emoji}")
      .filter(!$"Emoji".contains("(") && !$"Emoji".contains(")") && !$"Emoji".contains("|"))
      .groupBy($"Country", $"Emoji")
      .agg(count("Emoji").as("Count"))
      .sort($"Country", $"Count")
      .orderBy(desc("Count"), $"Country")
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

    def emojiCountOneStream(spark: SparkSession, userCountry: String): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
tweetStreamToDir(bearerToken, queryString = "?tweet.fields=geo&expansions=geo.place_id&place.fields=country")    }

    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000){
      filesFoundInDir = Files.list(Paths.get("twitterstream")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir){
      println("Error: Unable to populate tweetstream after 30 seconds. Exiting. . .")
      System.exit(1)
    }

    val staticDf = spark.read.json("twitterstream")

    val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstream")

    val emoji = "[(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val notEmoji = "[^(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val regexSpace = "(\\B\uD83D.{1})|(\\B\uD83C.{1})|(\\B\uD83E.{1})"

    streamDf
      .select($"data.text", $"includes.places.country")
      .filter($"includes".isNotNull)
      .select(regexp_replace($"text", s"${notEmoji}", "").as("Removed Words"), $"Country")
      .select(regexp_replace($"Removed Words", s"${regexSpace}", " $1").as("Added Spaces"), $"Country")
      .select(split($"Added Spaces", " ").as("Split"), $"Country")
      .select(explode($"Split").as("Emoji"), $"Country")
      .filter($"Emoji" rlike s"${emoji}")
      .filter(!$"Emoji".contains("(") && !$"Emoji".contains(")") && !$"Emoji".contains("|"))
      .select(explode($"Country").as("Country"), $"Emoji")
      .filter($"Country" rlike s"${userCountry}")
      .groupBy($"Country", $"Emoji")
      .agg(count("Emoji").as("Count"))
      .orderBy(desc("Count"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

      def emojiCountNotStream(spark: SparkSession, userCountry: String): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
tweetStreamToDir(bearerToken, queryString = "?tweet.fields=geo&expansions=geo.place_id&place.fields=country")    }

    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000){
      filesFoundInDir = Files.list(Paths.get("twitterstream")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir){
      println("Error: Unable to populate tweetstream after 30 seconds. Exiting. . .")
      System.exit(1)
    }

    val staticDf = spark.read.json("twitterstream")

    val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstream")

    val emoji = "[(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val notEmoji = "[^(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val regexSpace = "(\\B\uD83D.{1})|(\\B\uD83C.{1})|(\\B\uD83E.{1})"

    streamDf
      .select($"data.text", $"includes.places.country")
      .filter($"includes".isNotNull)
      .select(regexp_replace($"text", s"${notEmoji}", "").as("Removed Words"), $"Country")
      .select(regexp_replace($"Removed Words", s"${regexSpace}", " $1").as("Added Spaces"), $"Country")
      .select(split($"Added Spaces", " ").as("Split"), $"Country")
      .select(explode($"Split").as("Emoji"), $"Country")
      .filter($"Emoji" rlike s"${emoji}")
      .filter(!$"Emoji".contains("(") && !$"Emoji".contains(")") && !$"Emoji".contains("|"))
      .select(explode($"Country").as("Country"), $"Emoji")
      .filter(!$"Country".contains(s"${userCountry}"))
      .groupBy($"Country", $"Emoji")
      .agg(count("Emoji").as("Count"))
      .orderBy(desc("Count"))
      .writeStream
      .outputMode("complete")
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
            Paths.get(s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"))
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }

        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }

    }
  }

}