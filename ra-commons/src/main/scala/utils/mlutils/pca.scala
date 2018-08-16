package utils.mlutils

import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.DataFrame

/**
  * The following object contains a method that uses PCA
  * to reduce the number of features that are going to be
  * used in the further algorithms.
  */
object pca{

  /**
    * The method first selects the necessary columns and creates a
    * dataframe dfWithFeatures, and using the specified value K, it
    * ouputs another dataframe that has exactly K dimensional features.
    * @param dataFrame Input dataFrame on which PCA is going to be applied.
    * @param keyCol Column that is used for as index for each feature.
    * @param featuresCol Column that contains the features.
    * @return DataFrame that contains features with K dimensions.
    */
  def pca(dataFrame: DataFrame, keyCol : String, featuresCol : Seq[String],K : Int) : DataFrame = {
    // Creating the dataframe using the utils function filterDf
    val dfWithFeatures = filterDf(dataFrame, featuresCol, keyCol)

    // Instantiating PCA with K value as input K and dataframe dfWithFeatures.
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(K)
      .fit(dfWithFeatures)

    // Creating the new dataframe having features with reduced dimensions.
    val reducedDf = pca.transform(dfWithFeatures).select("key","pcaFeatures")
    reducedDf
  }
}
