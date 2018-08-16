package utils.mlutils

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.DataFrame

/**
  * The following object contains a method that applies
  * feature scaling on the feature column of the transformed
  * input dataframe.
  */
object standardScaler{
  /**
    * The method first creates the dataframe containing only the necessary
    * columns given in the input and applies feature scaling on the features
    * column of that dataframe.
    * @param dataFrame Input dataframe that has to be scaled.
    * @param keyCol Index column for the features.
    * @param featuresCol Column containing all the features.
    * @return Dataframe after scaling the features.
    */
  def standardScaler(dataFrame : DataFrame, keyCol : String, featuresCol : Seq[String]) : DataFrame = {
    // Creating the dataframe using the utils function filterDf
    val dfWithFeatures = filterDf(dataFrame, featuresCol, keyCol)

    // Instantiating the StandardScaler and fitting the dataframe
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithMean(false)
      .setWithStd(true)
    val scalerModel = scaler.fit(dfWithFeatures)

    // Normalizing each feature using scalerModel
    val scaledDf = scalerModel.transform(dfWithFeatures)
    scaledDf.show(false)
    scaledDf
  }
}
