package com.analysis


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object StockAnalysis extends App {

  val filePath = "./src/resources/stock_prices.csv"

  val spark = SparkUtil.createSpark("stockAnalysis")

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  df.printSchema()
  df.describe().show(false)
  df.orderBy(col("date")).show(20)

  val windowSpec = Window
    .partitionBy( "ticker")
    .orderBy(col("date"))

  val dateAverageReturn = ((col("close") - lag("close", 1).over(windowSpec)) / lag("close", 1).over(windowSpec)) * 100.00

  df.withColumn("date_average_return", dateAverageReturn)
    //.where(col("ticker") === "GOOG")
    .orderBy(col("date"))
    .show(20)


}
