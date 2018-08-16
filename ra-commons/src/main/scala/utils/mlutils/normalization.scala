package utils.mlutils

import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.sql.DataFrame

/**
  * The following object contains a method that normalizes each
  * feature row from the transformed dataframe.
  */
object normalization{
  /**
    * The method first creates the dataframe containing only the necessary
    * columns given in the input and normalizes each feature of that dataframe.
    * @param dataFrame Input dataframe that has to be normalized
    * @param keyCol Index column for the features.
    * @param featuresCol Column containing the features.
    * @return Dataframe with normalized features.
    */
  def normalize(dataFrame : DataFrame, keyCol : String, featuresCol : Seq[String]) : DataFrame = {
    // Creating the dataframe using the utils function filterDf
    val dfWithFeatures = filterDf(dataFrame, featuresCol, keyCol)

    // Instantiating the Normalizer and applying it on the above dataframe.
    val normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures").setP(1.0)
    val normDataFrame = normalizer.transform(dfWithFeatures)
    normDataFrame.show(false)
    normDataFrame
  }
}
