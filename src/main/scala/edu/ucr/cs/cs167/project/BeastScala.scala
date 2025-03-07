package edu.ucr.cs.cs167.project

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "count-by-county" =>
        // Sample program arguments: count-by-county Tweets_1k.tsv
        // TODO count the total number of tweets for each county and display on the screen
        case "convert" =>
          val outputFile = args(2)
        // TODO add a CountyID column to the tweets, parse the text into keywords, and write back as a Parquet file
        case "count-by-keyword" =>
          val keyword: String = args(2)
        // TODO count the number of occurrences of each keyword per county and display on the screen
        case "choropleth-map" =>
          val keyword: String = args(2)
          val outputFile: String = args(3)
        // TODO write a Shapefile that contains the count of the given keyword by county
        case _ => validOperation = false
      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}