package com.luxoft.dt.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, lit, max, min}
import org.apache.spark.storage.StorageLevel

object SparkApp {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    if (args.length < 1) {
      println("File path should be used as argument")
      System.exit(-1)
    }

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkApp")
      .getOrCreate()

    val df = spark.read.csv(args(0))

    print("Num of processed files: ")
    println(df.where(col("_c0").equalTo("sensor-id")).count())

    val cleanedDf = df.where(col("_c0").notEqual("sensor-id"))
      .select(col("_c0").as("sensor-id"), col("_c1").as("humidity"))
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    print("Num of processed measurements: ")
    println(cleanedDf.where(col("humidity").notEqual("NaN")).count())

    print("Num of failed measurements: ")
    println(cleanedDf.where(col("humidity").equalTo("NaN")).count())

    println("Sensors with highest avg humidity:")
    cleanedDf.where(col("humidity").notEqual("NaN"))
      .groupBy("sensor-id")
      .agg(min("humidity").as("min"), avg("humidity").as("avg"),
        max("humidity").as("max")).sort(col("avg").desc)
      .union(cleanedDf.select(col("sensor-id")).distinct().where(col("humidity").equalTo("NaN"))
        .withColumn("min", lit("NaN"))
        .withColumn("avg", lit("NaN"))
        .withColumn("max", lit("NaN"))).show()

    cleanedDf.unpersist()

    spark.stop()
  }
}
