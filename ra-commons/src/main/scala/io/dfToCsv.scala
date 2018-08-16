package io

import org.apache.spark.sql.DataFrame

/**
  * Following object contains a method that
  * converts an existing dataframe into a folder
  * that contains the csv file.
  */
object dfToCsv {
  /**
    * The following method converts the dataframe into
    * csv file using the coalesce method.
    * @param filePath String specifying the path of csv folder
    * @param dataFrame Dataframe that has to be saved
    */
  def dfToCsv(filePath : String , dataFrame : DataFrame) : Unit = {
    dataFrame.coalesce(1).write.csv(filePath)
  }
}
