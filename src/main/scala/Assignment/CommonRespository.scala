package Assignment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class CommonRespository {

  val spark = new Session().getSession()
  def readVideoDatasetAsRdd(): RDD[Row] = readVideoDatasetAsDataFrame().rdd

  def readVideoDatasetAsDataFrame(): DataFrame = {
    spark
      .read
      .option("header", true)
      .option("inferschema", true)
      .csv("src/main/Resource/USvideos.csv")
  }

  def createCSVFile(name : String): Unit ={

  }

  def readVideoDatasetAsDataSet(): Dataset[Row] = readVideoDatasetAsDataFrame().as("videoDataSet")

  def readVideoDatasetAsSql(): Unit={readVideoDatasetAsDataFrame().createOrReplaceTempView("videoTable")}
}
