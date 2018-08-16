package io

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Spark class defines a sparksession which is extended to use the
  * functions in spark.implicits.
  */
class Spark {
  // Log for ERROR only
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark : SparkSession = SparkSession.builder().config("spark.master","local").appName("sparksession").getOrCreate()
}
