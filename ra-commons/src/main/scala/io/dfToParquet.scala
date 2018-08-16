package io

import org.apache.spark.sql.DataFrame

/**
  * The following object contains a method that
  * converts dataframe into a parquet file.
  */
object dfToParquet  {
  /**
    * The following method converts an existing dataframe
    * into a parquet to temporarily store the dataframe for
    * using in the next transformation
    * @param filePath String specifying the parquet file location
    * @param dataFrame Dataframe that has to be saved
    */
  def dfToParquet(filePath : String, dataFrame : DataFrame) : Unit = {
    dataFrame.write.mode("overwrite").parquet(filePath)
  }
}
