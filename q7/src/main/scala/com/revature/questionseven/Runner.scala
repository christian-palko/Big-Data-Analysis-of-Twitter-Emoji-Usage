package com.revature.questionseven

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
  * Runner program to answer Question Seven of our project:
  *   What did emoji usage look like in historial data sets?
  * 
  * Runs emoji analysis on static dataframes of rehydrated Twitter data.
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

    // Routine to wait for a file to appear in the twitterstream folder.
    // Note: Ends the program if no file appears after 30 seconds.
    var continue = true
    while (continue){
      println("Options:\n" +
        "1 - Pull data from the 2006 - 2009 set\n" +
        "2 - Pull data from the 2015 set\n" +
        "3 - Quit")
      val input = StdIn.readInt()
      input match {
        case (input) if (input == 1) =>{
          parse2006to2009(spark)
        }
        case (input) if (input == 2) =>{
          parse2015(spark)
        }
        case (input) if (input == 3) =>{
          println("Exiting . . .")
          System.exit(0)
        }
        case _ => {
          println("Invalid. Here are your options:")
        }
      }
    }
  }

  def parse2006to2009(spark: SparkSession): Unit = {
    import spark.implicits._
    val staticDf = spark.read.json("2006-2009")

    val emoji = "[(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val notEmoji = "[^(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val regexSpace = "(\\B\uD83D.{1})|(\\B\uD83C.{1})|(\\B\uD83E.{1})"

    // full_text = 0

    staticDf
      .select($"full_text")
      .filter($"full_text" rlike s"$emoji")
      .select(regexp_replace($"full_text", s"$notEmoji", "").as("Removed Text"))
      .select(regexp_replace($"Removed Text", s"$regexSpace", " $1").as("Added Emoji Space"))
      .select(split($"Added Emoji Space", " ").as("Split"))
      .select(explode($"Split").as("Emoji"))
      .filter($"Emoji" rlike s"$emoji")
      .filter(!$"Emoji".contains("(") && !$"Emoji".contains(")") && !$"Emoji".contains("|"))
      .groupBy($"Emoji")
      .count().as("Count")
      .orderBy(desc("Count"), $"Emoji")
      .show()
  }

    def parse2015(spark: SparkSession): Unit = {
    import spark.implicits._
    val staticDf = spark.read.json("2015")

    val emoji = "[(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val notEmoji = "[^(\uD83D\uDE00-\uD83D\uDE4F)|(\uD83C\uDF00-\uD83D\uDDFF)|(\uD83E\uDD00-\uD83E\uDDFF)]"
    val regexSpace = "(\\B\uD83D.{1})|(\\B\uD83C.{1})|(\\B\uD83E.{1})"
    
    staticDf
      .select($"text")
      .filter($"text" rlike s"$emoji")
      .select(regexp_replace($"text", s"$notEmoji", "").as("Removed Text"))
      .select(regexp_replace($"Removed Text", s"$regexSpace", " $1").as("Added Emoji Space"))
      .select(split($"Added Emoji Space", " ").as("Split"))
      .select(explode($"Split").as("Emoji"))
      .filter($"Emoji" rlike s"$emoji")
      .filter(!$"Emoji".contains("(") && !$"Emoji".contains(")") && !$"Emoji".contains("|"))
      .groupBy($"Emoji")
      .count().as("Count")
      .orderBy(desc("Count"), $"Emoji")
      .show()
  }
}