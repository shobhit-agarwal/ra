package io

import org.apache.spark.sql.DataFrame

/**
  * The following object contains a method that converts
  * parquet file into dataframe.
  */
object parquetToDf extends Spark {
  /**
    * This method converts a parquet file into dataframe
    * for transformations.
    * @param filePath String specifying the path of the parquet file
    * @return Dataframe has to be used in the further transformations
    */
  def parquetToDf(filePath : String) : DataFrame = {
    val df = spark.read.parquet(filePath)
    df
  }

}
