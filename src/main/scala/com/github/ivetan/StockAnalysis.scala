package com.github.ivetan

import com.github.ivetan.Utilities.writeFile
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{avg, col, round, stddev, sum, to_date, year}
import org.apache.spark.sql.types.LongType

import scala.language.postfixOps

object StockAnalysis extends App {

  val filePath = "./src/resources/stock_prices.csv"

  val spark = Utilities.createSpark("stockAnalysis")

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
  writeFile(ndf,parquetFilePath,"parquet", header = false)

  val csvFilePath = "./src/resources/csv/stock_returns"
  writeFile(ndf, csvFilePath, "csv", header = true)


  /** Writes data with daily returns by every ticker in CSV file.
   * Will be used for regression in ML.
   */
  val csvFilePath2 = "./src/resources/csv/stock_returns_tickers"
  writeFile(dfWithReturn, csvFilePath2, "csv", header = true)

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


  /**  Calculates square root from trading days in the year.
   * There are exactly 252 trading days in 2016.
   * The NYSE and NASDAQ average about 253 trading days a year.
   * @see See https://en.wikipedia.org/wiki/Trading_day
   */
  val sqrt = math.sqrt(252)

  println("The most volatile stocks:")

  /**  Finds volatility (standard deviation) of stocks.
   * Split dates in years.
   * Calculates annualized standard deviation = Standard Deviation of Daily Returns * Square Root (trading days in the year)
   * @see See https://financetrain.com/calculate-annualized-standard-deviation/
   */
  dfWithReturn.withColumn("year", year(to_date(col("date"), "yyyy-MM-dd")))
    .groupBy("year", "ticker")
    .agg(
      round(avg("return"),2).alias("average_return"),
      round(stddev("return"),2).alias("stddev"),
    )
    .withColumn("annualized_volatility", round(col("stddev") * sqrt,2))
    .orderBy(col("annualized_volatility").desc)
    .show(20, truncate = false)




}
