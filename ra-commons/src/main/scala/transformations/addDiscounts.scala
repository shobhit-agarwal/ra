package transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, sum, udf}
import io.parquetToDf._
import io.dfToParquet._
import io.Spark

/**
  *
  */
object addDiscounts extends Spark{
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    //
    var df = parquetToDf("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/2.parquet")

    // Defining necessary functions using functions in utils
    def multUdf = udf{(x:Int,y:Int) => x*y}
    val minusOneUdf = udf{s : Int => s-1 }

    // Partitioning the dataframe based on the CustomerId and ranking for datevalue
    val window = Window.partitionBy("CustomerID").orderBy("datevalue")
    df = df.
      select(
        $"CustomerID", $"InvoiceNo",
        $"InvoiceDate", $"Description",
        $"datevalue",$"StockCode",
        $"Country",$"Date",
        $"Time",$"dayOfMonth",
        $"dayOfWeek", $"Quantity", $"UnitPrice",
        count("datevalue").over(window).alias("count"))

    /*
    Adding the discounts of rows with Stockcode to the previous purchase
    into a new column with value as product of quantity and unitprice.
     */
    val dfDiscount = df.filter(col("StockCode") === "D")
      .withColumn("Discountvalue",multUdf(col("Quantity"),col("UnitPrice")))
      .groupBy("count","CustomerID","InvoiceNo")
      .agg(sum("Discountvalue").alias("Discountvalue"))
      .withColumn("count",minusOneUdf(col("count")))
      .drop("CustomerID","InvoiceNo")
    df = df.join(dfDiscount,Seq("count")).drop("count")

    // Loading the dataframe into parquet files
    dfToParquet("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/4.parquet",df)
  }
}
