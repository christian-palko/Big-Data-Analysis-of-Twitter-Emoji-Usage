package com.revature.questionone

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
import org.apache.spark.sql.functions

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Team ACCE Q1")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    tweetStream(spark)
  }

  def tweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      emojiTweetTest(bearerToken)
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

    val pattern = ".*([^\u0000-\uFFFF]).*".r
    streamDf
      .select($"data.text")
      .as[String]
      .flatMap(text => {
        text match {
          case pattern(emote) => { Some(emote) }
          case notFound        => None
        }
      })
      .groupBy("value")
      .count()
      .sort(functions.desc("count"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()

  }

  def emojiTweetTest(
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
