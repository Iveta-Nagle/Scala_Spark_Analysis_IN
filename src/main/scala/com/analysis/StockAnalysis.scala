package com.analysis

import org.apache.spark
import org.apache.spark.sql.SparkSession

object StockAnalysis extends App {

  val filePath = "./src/resources/stock_prices.csv"

  val spark = SparkUtil.createSpark("stockAnalysis")

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  df.printSchema()
  df.describe().show(false)
  df.show()





}
