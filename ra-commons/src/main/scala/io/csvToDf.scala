package io

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Following object contains a method that
  * reads the csv file and returns a dataframe
  * containing the contents of the csv file.
  */
object csvToDf extends Spark{
  /**
    * This method uses the csv format to load the file
    * along with header.
    * @param filePath String that specifies the path of the csv file
    * @return Dataframe containing the contents of csv file
    */
  def csvToDf(filePath : String) : DataFrame = {
    val df = spark.read.format("com.databricks.spark.csv").option("header","true").load(filePath)
    df
  }
}
