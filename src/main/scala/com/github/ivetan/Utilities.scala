package com.github.ivetan

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Utilities object holding methods for [[com.github.ivetan]]
 *
 * A singleton (single instance) object of all my Utilities functions
 * @author Iveta Nagle
 *         author is Iveta Nagle
 * @version 0.93
 */

object Utilities {

  /** Creates Spark session
   *
   * @param appName sets a name for the application
   * @param verbose provides additional details on version session will run
   * @param master sets the Spark master URL to connect to
   */

  def createSpark(appName:String, verbose:Boolean = true, master: String= "local"): SparkSession = {
    if (verbose) println(s"$appName with Scala version: ${util.Properties.versionNumberString}")

    val spark = SparkSession.builder().appName(appName).master(master).getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    if (verbose) println(s"Session started on Spark version ${spark.version}")
    spark
  }

  /**  Writes and saves DataFrame to external file.
   * If file already exists, it will be overwritten with updated data.
   *
   * @param df data frame to be written in the file
   * @param filePath url for the new file
   * @param fileFormat sets the format for the new file with in which data frame will be saved into the new file
   * @param header adds header for data in the file
   */
  def writeFile(df: DataFrame, filePath: String, fileFormat: String, header: Boolean): Unit = {
    df
      .coalesce(1)
      .write
      .format(fileFormat)
      .mode("overwrite")
      .option("header", header)
      .save(filePath)
  }
}
