package com.analysis


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, col, round, stddev, sum, to_date, year}
import org.apache.spark.sql.types.LongType

import scala.language.postfixOps

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


  val dfWithReturn = df
  .withColumn("return", round((col("close") - col("open"))/col("open")*100,2))

  println("Stocks with daily return:")
  dfWithReturn.show(20)

  val ndf = dfWithReturn
              .groupBy("date")
              .agg(round(avg("return"),2).alias("daily_return"))
              .orderBy(col("date"))

  println("Return of all stocks by date:")
  ndf.show(20)

  val parquetFilePath = "./src/resources/parquet/stock_returns"

  ndf.na
    .drop
    .coalesce(1)
    .write
    .format("parquet")
    .mode("overwrite")
    .save(parquetFilePath)

  val csvFilePath = "./src/resources/csv/stock_returns"

  ndf.na
    .drop
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .option("header", "true")
    .save(csvFilePath)


  val stockSpec = Window
    .partitionBy( "ticker")
    .orderBy(col("date"))
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)


  val frequency = col("close") * functions.avg("volume").over(stockSpec)


  println("Most frequently traded stocks (closing price * avg volume):")

  dfWithReturn
    .withColumn("frequency", frequency.cast(LongType))
    .groupBy("ticker")
    .agg(sum("frequency"))
    .orderBy(col("sum(frequency)")desc)
    .show(20, truncate = false)


  //https://financetrain.com/calculate-annualized-standard-deviation/
  //Annualized Standard Deviation = Standard Deviation of Daily Returns * Square Root (trading days in the year)


  //https://en.wikipedia.org/wiki/Trading_day
  //"There are exactly 252 trading days in 2016"
  // "The NYSE and NASDAQ average about 253 trading days a year"
  val sqrt = math.sqrt(252)

  println("The most volatile stocks:")

  dfWithReturn.withColumn("year", year(to_date(col("date"), "yyyy-MM-dd")))
    .groupBy("year", "ticker")
    .agg(
      avg(col("return")),
      stddev(col("return")),
    )
    .withColumn("annualized_volatility", round(col("stddev_samp(return)") * sqrt,2))
    .orderBy(col("annualized_volatility").desc)
    .show(20, truncate = false)




}
