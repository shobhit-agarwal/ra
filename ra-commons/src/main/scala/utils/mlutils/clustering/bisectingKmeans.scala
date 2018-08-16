package utils.mlutils.clustering

import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.sql.DataFrame
import utils.mlutils.filterDf.filterDf

/**
  * The following object contains different methods
  * like clustering a given dataframe or predicting
  * the cluster of new datapoint,etc.
  */
object bisectingKmeans{
  /**
    * The dfToModel takes a dataframe, names of the label and feature
    * columns as input and clusters them using BisectingKMeans along with an
    * optional parameter K(default optimal value is used if no input is given).
    * This gives a NullPointerException if any of the input columns contains
    * null values. So make sure to filter those before calling this method.
    * @param dataFrame Input DataFrame.
    * @param keyCol Column that contains the indices of datapoints.
    * @param featuresCol Column that contains the features.
    * @param K Integer that specifies the number of clusters.
    * @return BisectingKMeansModel.
    */
  def dfToModel(dataFrame : DataFrame, keyCol : String , featuresCol : Seq[String] , K : Int = 0) :BisectingKMeansModel = {
    // Creating the dataframe using the utils function filterDf
    val dfWithFeatures = filterDf(dataFrame, featuresCol, keyCol)

    // Creating a model using KMeans class and necessary parameters.
    if(K == 0)
    {
      /*
      If the value of K is not given as input, BisectingKMeans considers
      the optimal number of clusters and creates the model.
       */
      val bkmeans = new BisectingKMeans
      val model = bkmeans.fit(dfWithFeatures)
      model.transform(dfWithFeatures).show
      model
    }
    else
    {
      /*
      If the value of K is given along with other values, BisectingKMeans
      uses that value and creates the model.
      */
      val bkmeans = new BisectingKMeans().setK(K)
      val model = bkmeans.fit(dfWithFeatures)
      model.transform(dfWithFeatures).show
      model
    }
  }
}
