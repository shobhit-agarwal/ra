package utils.mathutils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, struct}

/**
  * This object contains a method that computes
  * the mode for a particular column in a dataframe.
  */
object mode {
  /**
    * The dataframe is grouped based on two columns
    * and then aggregated using the count and max functions.
    * @param dataFrame Input dataframe with column for which mode has to be computed.
    * @param col1 col2 vlaues are grouped based on this column col1.
    * @param col2 Column that contains the values for which mode has to be computed.
    * @return Dataframe that contains mode corresponding to each entry in col1.
    */
  def modeDf(dataFrame : DataFrame, col1 : String , col2 : String) : DataFrame = {
    // Input dataframe is grouped based on the
    val dfDateGrouped = dataFrame.groupBy(col1,col2)
    val dfDate = dfDateGrouped.count()

    //
    val dfModes = dfDate.groupBy(col1)
      .agg(max(struct(col("count"),col(col2))).alias("max"))
      .select(col1,"max."+col2)
    dfModes
  }
}
