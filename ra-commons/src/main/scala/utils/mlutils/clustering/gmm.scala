package utils.mlutils.clustering

import org.apache.spark.ml.clustering.{GaussianMixture,GaussianMixtureModel}
import org.apache.spark.sql.DataFrame
import utils.mlutils.filterDf.filterDf

/**
  * This object contains method that applies GMM
  * clustering algorithm on the given dataframe
  * with features as the input string sequence featuresCol.
  */
object gmm {
  /**
    * This method takes a dataframe as input along the neccessary columns like
    * features and keys and applies GMM clustering algorithm on the feature
    * column and returns the GausssianMixtureModel.
    * @param dataFrame Input DataFrame.
    * @param keyCol Column that contains the indices of datapoints.
    * @param featuresCol Column that contains the features.
    * @return GaussianMixtureModel.
    */
  def dfToModel(dataFrame : DataFrame, keyCol: String, featuresCol : Seq[String],K : Int) : GaussianMixtureModel = {
    // Creating the dataframe using the utils function filterDf
    val dfWithFeatures = filterDf(dataFrame,featuresCol,keyCol)

    // Instantiating the class and creating the model *model*.
    val gmm = new GaussianMixture().setK(K)
    val model = gmm.fit(dfWithFeatures)
    model
  }
}
