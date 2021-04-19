package Assignment.dataset

import Assignment.{CommonRespository, Session}
import org.apache.log4j.{Level, Logger}
import scala.io.StdIn.readLine

class DatasetSolution2 {

  def solution2dataset(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = new Session().getSession()

    val videoDf = new CommonRespository().readVideoDatasetAsDataSet()

    import spark.implicits._

    /**
     * SOLUTION 2
     */
    println("Enter 2 channel title")
    val channelTitle = List(readLine, readLine)
    println("Enter tag key")
    val tagkey = List(readLine)
    println("Enter two category ID")
    val categoryID = List(readLine, readLine)
    val filteredDf = videoDf.filter($"channel_title".isin(channelTitle: _*) || $"category_id".isin(categoryID: _*) || $"tags".isin(tagkey))
    val selectedColDf = filteredDf.select(
      "video_id",
      "title",
      "channel_title",
      "category_id",
      "likes",
      "dislikes",
      "comment_count",
      "views"
    )
    selectedColDf
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("src/main/Resource/Solutin2DataSet")
  }

}
