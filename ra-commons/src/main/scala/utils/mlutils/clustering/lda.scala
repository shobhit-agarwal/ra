package utils.mlutils.clustering

import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.sql.DataFrame
import utils.mlutils.filterDf.filterDf

/**
  * This object contains method that applies LDA
  * clustering algorithm on the given dataframe
  * with features as the input string sequence featuresCol.
  */
object lda {
  /**
    * This method takes a dataframe as input along the neccessary columns like
    * features and keys and applies LDA clustering algorithm on the feature
    * column and returns the LDAModel.
    * @param dataFrame Input DataFrame.
    * @param featuresCol Column that contains the indices of datapoints.
    * @param keyCol Column that contains the features.
    * @param K Number of clusters.
    * @return LDAModel.
    */
  def dfToModel(dataFrame : DataFrame, featuresCol : Seq[String], keyCol : String, K : Int) : LDAModel = {
    // Creating the dataframe using the utils function filterDf
    val dfWithFeatures = filterDf(dataFrame, featuresCol, keyCol)

    // Instantiating the class and creating the model *model*.
    val kmeans = new LDA().setK(K)
    val model = kmeans.fit(dfWithFeatures)
    model
  }
}
