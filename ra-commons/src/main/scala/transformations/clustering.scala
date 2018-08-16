package transformations

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import io.Spark
import io.parquetToDf._
import io.dfToCsv._

/**
  * The following object applies the KMeans Clustering algorithm
  * on the selected columns as features and CustomerId as label.
  */
object clustering extends Spark{
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    // Loading the dataframe from 3.parquet and 4.parquet
    var df = parquetToDf("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/4.parquet")
    val dfMean = parquetToDf("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/3.parquet")

    /*
    Selecting the necessary columns used for the clustering
    and converting them into dense vector as only that datatype
    can be used while applying the algorithm.
     */
    var dfForClustering = dfMean.select("avgQuantity","avgUnitprice","dayOfWeek","dayOfMonth","Time","CustomerID")
    val dfWithFeatures = dfForClustering.rdd.
      map(l => (Vectors.dense(l(0).toString.toDouble,l(1).toString.toDouble,l(2).toString.toDouble
        ,l(3).toString.toDouble,l(4).toString.toDouble),l(5).toString.toDouble)).
      toDF("features","label")

    // building the model
    val kmeans = new KMeans().setK(40)
    val model = kmeans.fit(dfWithFeatures)

    // adding the centers of clusters to df_with_id
    val centers = model.transform(dfWithFeatures)
    centers.show()
    dfForClustering = dfForClustering.join(centers,centers("label")===dfForClustering("CustomerID")).drop(col("label"))
    df = df.join(centers,centers("label")===df("CustomerID")).drop("label","datevalue","dayOfWeek","dayOfMonth"
      ,"Description","Country","features")

    // loading the final dataframe into csv file
    dfToCsv("clustercenters.csv",centers.drop("features"))
    dfToCsv("useridclusters.csv",df)
  }
}
