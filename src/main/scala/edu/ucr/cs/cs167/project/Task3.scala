package edu.ucr.cs.cs167.project // Do not miss this line

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, to_date, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object Task3 {

  def main(args: Array[String]) {
    val start: String = args(0) //  command to be run
    val end: String = args(1)
    val inputfile: String = args(2) //  input file name
    val outputfile: String = "crime-between-dates" //  output file name




    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Project - Section22-3 - Task 3")
      .config(conf)
      .getOrCreate()

    if (!(inputfile.endsWith(".json") || inputfile.endsWith(".parquet"))) {
      Console.err.println(s"Unexpected input format. Expected file name to end with either '.json' or '.parquet'.")
    }
    var df : DataFrame = if (inputfile.endsWith(".json")) {
      spark.read.json(inputfile)
    } else {
      spark.read.parquet(inputfile)
    }
    df.createOrReplaceTempView("chicago_crimes")

    println(s"Testing ${start} and ${end}")
    try {
      val t1 = System.nanoTime
      df = spark.sql(s"SELECT PrimaryType, COUNT(*) as CrimeCount FROM chicago_crimes WHERE TO_TIMESTAMP(Date, 'MM/dd/yyyy hh:mm:ss a') BETWEEN TO_DATE('${start}', 'MM/dd/yyyy') AND TO_DATE('${end}', 'MM/dd/yyyy') GROUP BY PrimaryType ORDER BY CrimeCount DESC")

      df.show(25);

      df.coalesce(1);
      df.write.mode("overwrite").csv(outputfile + ".csv")
      val t2 = System.nanoTime
    } finally {
      spark.stop
    }
  }
}