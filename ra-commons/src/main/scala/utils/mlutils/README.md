# Machine Learning Utils
The **mlutils** folder contains different machine learning
algorithm wrappers for clustering, PCA,etc. Each of them
converts a dataframe into respective model.
### filterDf
filterDf is a method that selects the specified columns and converts them into a single column dense vectors.
## Clustering
There are four clustering algorithms available in spark ml
library that can be applied on dataframes. They are:
### BisectingKmeans
Method *dfToModel* in this object uses a type of  
Hierarchical clustering named Bisecting K-Means. It also 
optimises the number of clusters. [More info](http://mlwiki.org/index.php/K-Means#Bisecting_K-Means)
### GMM
Method *dfToModel* in this object uses Gaussian Mixture 
Model algorithm which considers each clusters contains the
datapoints that belong to a gaussian distribution. [More info](http://scikit-learn.org/stable/modules/mixture.html)
### Kmeans
Method *dfToModel* in this object applies normal K-Means
clustering algorithm on the given data.
### LDA
Method *dfToModel* in this object uses Latent Dirichlet 
Allocation on the given input dataframe. [More info](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)
## Normalization
This normalizes each row of the given input dataframe. This
is different from scaling. [Check this](https://stackoverflow.com/questions/34234817/feature-normalization-algorithm-in-spark)
## PCA
This applies Principal Component Analysis on the input
dataframe and outputs the reduced dataframe. [More info](https://en.wikipedia.org/wiki/Principal_component_analysis)
## StandardScaler
*dfToModel* in this object scales the columns of input 
dataframe with either mean or standard deviation or both
based on the parameters. [More info](https://stackoverflow.com/questions/40758562/can-anyone-explain-me-standardscaler)