package edu.ucr.cs.cs167.project

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.SparkSession

object Task2 {

  def main(args: Array[String]): Unit = {
    val inputFile: String = args(0)
    val zipcodeBoundaries: String = args(1)
    val outputFile: String = "ZIPCodeCrimeCount"

    val conf = new SparkConf

    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val sparkSession = SparkSession
                .builder()
                .appName("CS167 Project - Section 22-3 - Task 2(Choropleth Map)")
                .config(conf)
                .getOrCreate()

    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    try {

      // Load the dataset in the Parquet format
      sparkSession.read.parquet(inputFile).createOrReplaceTempView("crime")

      // Run a grouped-aggregate SQL query that computes the total number of crimes per ZIP code
      sparkSession.sql(
        s"""
        SELECT ZIPCode, COUNT(*) AS count
        FROM crime
        GROUP BY ZIPCode
        """).createOrReplaceTempView("crime_count")

      // Load the ZIP Code dataset using Beast and convert it to an RDD
      sparkSession.read.format("shapefile").load(zipcodeBoundaries)
        .createOrReplaceTempView("zipcodes")

      //** Join with equi-join query and stored output to single shapefile
      sparkSession.sql(
        s"""
        SELECT c.ZIPCode, z.geometry, c.count
        FROM zipcodes z, crime_count c
        WHERE c.ZIPCode = z.ZCTA5CE10
        """).coalesce(1).write.format("shapefile").save(outputFile)

    } finally {
      sparkSession.stop()
    }
  }
}