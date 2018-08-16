package transformations

import org.apache.spark.sql.functions.mean
import io.Spark
import utils.mathutils.mode.modeDf
import io.dfToParquet._
import io.parquetToDf._
import io.dfToCsv._

/**
  * The following object contains the method for finding
  * the modes of different columns that had been added
  * in the previous transformation t2.scala
  */
object findModes extends Spark{
  def main(args: Array[String]): Unit = {
    // Loading the parquet file into the dataframe
    val df = parquetToDf("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/2.parquet")

    // grouping the dataframe based on custormerid to find the average quantity and unitprice
    val dfGrouped = df.groupBy("CustomerID")
    var dfMean = dfGrouped.agg(mean("UnitPrice").alias("avgUnitprice"),
      mean("Quantity").alias("avgQuantity"))

    // mode for day_of_week, day_of_month, and Time
    val dfDayOfWeekModes = modeDf(df,"CustomerID","dayOfWeek")
    val dfDayOfMonthModes = modeDf(df,"CustomerID","dayOfMonth")
    val dfTimeModes = modeDf(df,"CustomerID","Time")

    // joining the modes of dates and times with df
    dfMean = dfMean.join(dfDayOfMonthModes,Seq("CustomerID"))
      .join(dfDayOfWeekModes,Seq("CustomerID"))
      .join(dfTimeModes,Seq("CustomerID"))

    // Loading the dataframe into the parquet file
    dfToParquet("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/3.parquet",dfMean)
    dfToCsv("dfmean.csv",dfMean)
  }
}
