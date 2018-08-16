package utils.mlutils

import io.Spark
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import io.csvToDf.csvToDf

/**
  * The following object contains method that selects the given columns
  * in the input and transforms such that the output dataframe can be
  * used in other ml library related algorithms(converting the each row of
  * featureCol into dense vector).
  */
object filterDf extends Spark{
  import spark.implicits._

  /**
    * The method takes the dataframe as input along with featuesCol and
    * keyCol(optional input). It first selects all the necessasry columns
    * and convert the featuresCol into dense vector.
    * @param dataFrame Input dataframe
    * @param featuresCol Columns containing all the features.
    * @param keyCol Indexing for the features column.
    * @return Dataframe with filtered columns.
    */
  def filterDf(dataFrame: DataFrame, featuresCol : Seq[String], keyCol : String = "") : DataFrame = {
    if(keyCol == "")
    {
      /*
      Select all the necessary columns from the dataframe that are going
      to be used for further transformations.
       */
      val dfForClustering : DataFrame = dataFrame.select(featuresCol.head,featuresCol.tail:_*)

      /*
      Converting the columns of the dataframe dfForClustering into dense
      vectors as only dense vecotrs can be used in ml library algorithms implementation.
       */
      val dfFeaturesRdd = dfForClustering.rdd.map(l =>
        Vectors.
          dense(l.
            toSeq.
            drop(1).
            map(x=>x.toString.toDouble).
            toArray[Double]
          )
      )

      // The rdd has to be converted to DatasetHolder as it cannot be directly converted to Dataframe.
      val dfWithFeatures = dfFeaturesRdd.map(Tuple1(_)).toDF("features")

      // Return the filtered dataframe
      dfWithFeatures
    }
    else
    {
      /*
      Select all the necessary columns from the dataframe that are going
      to be used for further transformations.
      */
      val dfForClustering : DataFrame = dataFrame.select(keyCol,featuresCol:_*)

      /*
      Converting the columns of the dataframe dfForClustering into dense
      vectors as only dense vecotrs can be used in ml library algorithms implementation.
       */
      val dfWithFeatures = dfForClustering.rdd.map(l =>
        (Vectors.
          dense(l.
            toSeq.
            drop(1).
            map(x=>x.toString.toDouble).
            toArray[Double])
          ,l(0).toString.toDouble))
        .toDF("features","key")

      // Return the filtered dataframe
      dfWithFeatures
    }
  }

  def main(args: Array[String]): Unit = {
    val df = csvToDf("/home/avinash/IdeaProjects/sparkexample/spark-warehouse/regression.data")

    val filteredDf = filterDf(df,Array("Quantity","UnitPrice","Discount"))
    filteredDf.show(false)
  }
}
