package transformations

import org.apache.spark.sql.functions.{col, split, udf}
import io.parquetToDf._
import io.dfToParquet._
import utils.stringutils.dateTime._

/**
  * The following object adds the necessary columns for the
  * initial dataframe.
  */
object modifyColumns {
  def main(args: Array[String]): Unit = {
    // Loading the parquet file into the dataframe
    var df = parquetToDf("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/1.parquet")

    // Defining all the necessary functions using the functions defined in utils
    def splitDayOfMonthUdf = udf[Int,String](splitDayOfMonth)
    def splitDayOfWeekUdf = udf[Int,String](splitDayOfWeek)
    def splitTimeUdf = udf[Int,String](splitTime)
    def dateValueUdf = udf[Double,String](dateValue)
    def splitColInvoicedate = split(df("InvoiceDate"), " ")

    // Converting date and time into blocks and adding them as columns
    df = df.withColumn("Date", splitColInvoicedate.getItem(0))
      .withColumn("Time", splitColInvoicedate.getItem(1))
      .withColumn("dayOfMonth",splitDayOfMonthUdf(col("Date")))
      .withColumn("dayOfWeek",splitDayOfWeekUdf(col("Date")))
      .withColumn("Time",splitTimeUdf(col("Time")))
      .withColumn("datevalue",dateValueUdf(col("Date")))

    // Loading the dataframe into the second parquet file
    dfToParquet("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/2.parquet",df)
  }
}
