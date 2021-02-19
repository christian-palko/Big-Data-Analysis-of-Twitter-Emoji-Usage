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

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Hello Spark SQL")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    helloTweetStream(spark)

  }

  def helloTweetStream(spark: SparkSession): Unit = {
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