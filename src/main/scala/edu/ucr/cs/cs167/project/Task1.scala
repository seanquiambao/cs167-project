package edu.ucr.cs.cs167.project

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Map

object Task1 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context
    val conf = new SparkConf().setAppName("Beast Example")
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
      val t1 = System.nanoTime()
      var validOperation = true

      // Parse and load the CSV file using the Dataframe API
      val df: DataFrame = sparkSession.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputFile)

      df.printSchema()
      df.show(5)

      // Rename columns with spaces
      val renamedDF = df.columns.foldLeft(df) { (tempDF, colName) =>
        tempDF.withColumnRenamed(colName, colName.replaceAll(" ", "_"))
      }

      renamedDF.printSchema()
      renamedDF.show(5)

      // Introduce a geometry attribute
      val spatialDF = renamedDF.selectExpr("*", "ST_CreatePoint(x, y) AS geometry")
      spatialDF.printSchema()
      spatialDF.show(5)

      // Convert DataFrame to SpatialRDD
      val spatialRDD = spatialDF.toSpatialRDD

      // Load the ZIP Code dataset using Beast
      val zipDF: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510")

      // Run a spatial join query to find the ZIP code of each crime
      val spatialJoin: RDD[(IFeature, IFeature)] = spatialRDD.spatialJoin(zipDF)

      val crimeWithZipRDD = spatialJoin.map { case (crimeFeature, zipFeature) =>
        // Get the ZIP code from the ZIP feature
        val zipCode = zipFeature.getAs[String]("ZCTA5CE10")
        val crimeWithZipFeature = Feature.append(crimeFeature, zipCode, "ZIPCode")

        crimeWithZipFeature
      }

      // Convert the result back to a DataFrame and drop unnecessary columns
      val crimeWithZipDF = crimeWithZipRDD.toDataFrame(sparkSession).drop("geometry")

      // Write the output as a Parquet file named Chicago_Crimes_ZIP
      crimeWithZipDF.write.mode("overwrite").parquet("Chicago_Crimes_ZIP")

      // Debugging to confirm results
      // crimeWithZipDF.show()
      // val selectedRows = crimeWithZipDF.select("*").where("x = -87.677483468")
      // selectedRows.show()
      crimeWithZipDF.show(5)
      crimeWithZipDF.printSchema()

    } finally {
      sparkSession.stop()
    }
  }
}
