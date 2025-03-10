package edu.ucr.cs.cs167.project

// Do not miss this line

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task4 {

  def main(args: Array[String]): Unit = {


    if (args.length < 7) {
      System.err.println(
        s"""
           |Usage: spark-submit Task4.jar <startDate> <endDate> <x_min> <y_min> <x_max> <y_max> <inputfile>
           |   e.g.: spark-submit Task4.jar 03/15/2018 03/31/2018 -87.9 41.7 -87.6 41.9 ChicagoCrimes.parquet
           |""".stripMargin);
      System.exit(1);
    }

    val startDate = args(0);
    val endDate = args(1);
    val xMin = args(2).toDouble;
    val yMin = args(3).toDouble;
    val xMax = args(4).toDouble;
    val yMax = args(5).toDouble;
    val inputFile = args(6);

    val outputFile = "RangeReportResult";

    val conf = new SparkConf();
    if (!conf.contains("spark.master")) {
      // If no master set, default to local[*]
      conf.setMaster("local[*]")
    }
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Project - Task 4 (Spatio-Temporal)")
      .config(conf)
      .getOrCreate()

    val df: DataFrame = {
      if (inputFile.endsWith(".json")) {
        spark.read.json(inputFile)
      } else {
        // default to parquet
        spark.read.parquet(inputFile)
      }
    }

    df.createOrReplaceTempView("chicago_crimes")

    println(s"Querying crimes between [$startDate] and [$endDate] within [$xMin, $yMin, $xMax, $yMax].")

    val query =
      s"""
         |SELECT
         |  X,
         |  Y,
         |  `Case Number` AS CaseNumber,
         |  TO_TIMESTAMP(Date, 'MM/dd/yyyy hh:mm:ss a') AS Date
         |FROM chicago_crimes
         |WHERE
         |  TO_TIMESTAMP(Date, 'MM/dd/yyyy hh:mm:ss a')
         |    BETWEEN TO_DATE('$startDate', 'MM/dd/yyyy')
         |        AND TO_DATE('$endDate',   'MM/dd/yyyy')
         |  AND X >= $xMin AND X <= $xMax
         |  AND Y >= $yMin AND Y <= $yMax
         |""".stripMargin

    val resultDF = spark.sql(query)

    resultDF.show(25, truncate = false)

    resultDF.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputFile + ".csv")

    println(s"Results saved to: $outputFile.csv (check the folder/part file)")

    spark.stop()

  }
}