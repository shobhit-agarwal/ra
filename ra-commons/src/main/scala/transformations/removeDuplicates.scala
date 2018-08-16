package transformations

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.SparkSession
import io.csvToDf._
import io.dfToParquet._

/**
  * The following object transforms the data in such a way that
  * each set of invoiceno, stockcode and unitprice for a
  * particular customer is unique
  */
object removeDuplicates {
  def main(Args: Array[String]) : Unit = {
    // reading the csv file into dataframe
    var df = csvToDf("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/retail-data.csv")

    // Adding the quantities and prices for the items that are repeated
    val df_grouped = df.groupBy("InvoiceNo","StockCode","UnitPrice")
    val df_sum = df_grouped.agg(sum("Quantity").alias("Quantity"))
    df = df.drop("Quantity").
      join(df_sum,Seq("InvoiceNo","StockCode","UnitPrice")).
      dropDuplicates(Seq("InvoiceNo","StockCode","UnitPrice"))

    // writing the dataframe into a parquet file
    dfToParquet("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/1.parquet",df)
  }
}