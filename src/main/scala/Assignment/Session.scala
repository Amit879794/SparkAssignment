package Assignment

import org.apache.spark.sql.SparkSession

class Session {

  val spark = SparkSession
    .builder
    .appName("SparkSQL")
    .master("local[*]")
    .getOrCreate()

  def getSession(): SparkSession=spark

}
