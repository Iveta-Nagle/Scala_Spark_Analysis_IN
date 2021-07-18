package com.analysis

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, round}

object Regression extends App {

  val spark = SparkUtil.createSpark("ML")
  val filePath = "./src/resources/csv/stock_returns"

  val csvDF = spark
    .read
    .format("CSV")
    .option("header", true)
    .option("inferSchema", true)
    .load(filePath)

  csvDF.printSchema()
  csvDF.describe().show(false)
  csvDF.show(10,false)

  val stockSpec = Window
    .partitionBy( "ticker")
    .orderBy(col("date"))
    .rowsBetween(-30, Window.currentRow)

  val avgClosePrice = functions.avg("close").over(stockSpec)

  val avgPriceDF = csvDF.withColumn("avg_close_price", round(avgClosePrice,2))

  avgPriceDF.show(10)

  val supervised = new RFormula()
    .setFormula("close ~ open + high + volume + ticker + date_average_return + avg_close_price")

  val ndf = supervised
    .fit(avgPriceDF)
    .transform(avgPriceDF)

  ndf.show(10)

  val linReg = new LinearRegression()

  val lrModel = linReg.fit(ndf)

  val intercept = lrModel.intercept
  val coefficients = lrModel.coefficients
  val x1 = coefficients(0)
  val x2 = coefficients(1)

  println(s"Intercept: $intercept and coefficient for x1 is $x1 and for x2 is $x2")

}
