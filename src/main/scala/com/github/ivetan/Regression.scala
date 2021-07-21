package com.github.ivetan

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, expr, round}

object Regression extends App {

  val spark = Utilities.createSpark("ML")
  val filePath = "./src/resources/csv/stock_returns_tickers"

  val csvDF = spark
    .read
    .format("CSV")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .load(filePath)

  csvDF.printSchema()
  csvDF.describe().show(false)
  csvDF.show(10,truncate = false)

  val stockSpec = Window
    .partitionBy( "ticker")
    .orderBy(col("date"))
    .rowsBetween(-30, Window.currentRow)

  val avgClosePrice = functions.avg("close").over(stockSpec)

  val newDf= csvDF.withColumn("prevOpen", expr("" +
    "LAG (open,1,0) " +
    "OVER (PARTITION BY ticker " +
    "ORDER BY date )"))
    .withColumn("prevClose", expr("" +
      "LAG (close,1,0) " +
      "OVER (PARTITION BY ticker " +
      "ORDER BY date )"))
    .withColumn("avg_close_price", round(avgClosePrice,2))

  println("Stocks with last 30 day average price and previous day open and close price:")
  newDf.show(20)

  val supervised = new RFormula()
    .setFormula("open ~ prevOpen + prevClose + avg_close_price")

  val ndf = supervised
    .fit(newDf)
    .transform(newDf)
  ndf.show(10)

  val cleanDf = ndf.where("prevOpen != 0.0")
  cleanDf.show(10, truncate = false)

  val Array(train,test) = cleanDf.randomSplit(Array(0.75,0.25))

  val linReg = new LinearRegression()

  val lrModel = linReg.fit(train)

  val intercept = lrModel.intercept
  val coefficients = lrModel.coefficients
  val x1 = coefficients(0)
  val x2 = coefficients(1)

  println(s"Intercept: $intercept and coefficient for x1 is $x1 and for x2 is $x2")

  val summary = lrModel.summary

  //Residuals are the differences between observed and predicted values of data.
  //They are a diagnostic measure used when assessing the quality of a model.
  //They are also known as errors.
  summary.residuals.show()

  //the square root of the mean of the square of all of the error.
  println(s"Root mean squared error (RMSE) is ${summary.rootMeanSquaredError} \n")

  //R-squared is a statistical measure that represents the goodness of fit of a regression model.
  // The more the value of r-square near to 1, the better is the model.
  println(s"R-squared (R2) is ${summary.r2} \n")

  //Variance is a measure of how far observed values differ from the average of predicted values,
  // i.e., their difference from the predicted value mean.
  println(s"Explained variance is ${summary.explainedVariance} \n")

  val predictedDf = lrModel.transform(test)
  predictedDf.show(10, truncate = false)


}
