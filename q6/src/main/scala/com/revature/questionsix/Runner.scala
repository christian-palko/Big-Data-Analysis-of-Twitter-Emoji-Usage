package com.revature.questionsix

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
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

    //Display location name as tweets occur there
    val pattern = """.*([^\\u0000-\\uFFFF]).*""".r
    var countryTweetDF = staticDf
      .filter(!functions.isnull($"includes.places"))
      .select(functions.element_at($"includes.places", 1)("country").as("Country"), $"data.text")
      //.as[String]
      // .flatMap(text => {
      //   text match {
      //     case pattern(emote) => { Some(emote) }
      //     case notFound        => None
      //   }
      // })
      // .groupBy("value")
      // .count()
      // .sort(functions.desc("count"))
      // .writeStream
      // .outputMode("append")
      // .format("console")
      // .option("truncate", false)
      // .start()
      // .awaitTermination()

    countryTweetDF.createOrReplaceTempView("countryView")

    val regex = """.*([^\\u0000-\\uFFFF]).*""".r
    val notRegex = """.*([\\u0000-\\uFFFF]).*""".r

    spark.sql("SELECT Emoji, COUNT(Emoji) " +
      "FROM (" +
      "SELECT REGEXP_EXTRACT(text, '.*([^\\u0000-\\uFFFF]).*') AS Emoji " +
      "FROM countryView " +
      "WHERE text RLIKE '.*([^\\u0000-\\uFFFF]).*' " +
      ")" +
      "GROUP BY Emoji").show()

    spark.sql("SELECT LEN(text) - LEN(REPLACE(text, '${regex}', ''))")


    // val pattern = ".*([^\u0000-\uFFFF]).*".r
    // streamDf
    //   .select($"data.text")
    //   .as[String]
    //   .flatMap(text => {
    //     text match {
    //       case pattern(emote) => { Some(emote) }
    //       case notFound        => None
    //     }
    //   })
    //   .groupBy("value")
    //   .count()
    //   .sort(functions.desc("count"))
    //   .writeStream
    //   .outputMode("complete")
    //   .format("console")
    //   .start()
    //   .awaitTermination()

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