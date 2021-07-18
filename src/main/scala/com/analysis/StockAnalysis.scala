package com.analysis


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, col, lag, round, stddev, sum, to_date, year}
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

  val windowSpec = Window
    .partitionBy( "ticker")
    .orderBy(col("date"))


  val dateAverageReturn = ((col("close") - lag("close", 1).over(windowSpec)) / lag("close", 1).over(windowSpec)) * 100.00

  val ndf = df.withColumn("date_average_return", round(dateAverageReturn,2))
              .orderBy(col("date"))

  ndf.show(20)

  val parquetFilePath = "./src/resources/parquet/stock_returns"

  ndf.na
    .drop
    .write
    .format("parquet")
    .mode("overwrite")
    .save(parquetFilePath)

  val csvFilePath = "./src/resources/csv/stock_returns"

  ndf.na
    .drop
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

  df
    .withColumn("frequency", frequency.cast(LongType))
    .groupBy("ticker")
    .agg(sum("frequency"))
    .orderBy(col("sum(frequency)")desc)
    .show(20, truncate = false)


  //https://financetrain.com/calculate-annualized-standard-deviation/
  //Annualized Standard Deviation = Standard Deviation of Daily Returns * Square Root (trading days in the year)

  val sqrt = math.sqrt(252)

  println("The most volatile stocks:")

  ndf.withColumn("year", year(to_date(col("date"), "yyyy-MM-dd")))
    .groupBy("year", "ticker")
    .agg(
      avg(col("date_average_return")),
      stddev(col("date_average_return")),
    )
    .withColumn("annualized_volatility", round(col("stddev_samp(date_average_return)") * sqrt,2))
    .orderBy(col("annualized_volatility").desc)
    .show(20, truncate = false)




}
