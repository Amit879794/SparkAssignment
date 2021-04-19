package Assignment.dataset

import Assignment.{CommonRespository, Session}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.desc
import scala.io.StdIn.readLine

class DatasetSolution4 {
  def solution4dataset(): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = new Session().getSession()
    val videoDf = new CommonRespository().readVideoDatasetAsDataSet()

    import spark.implicits._

    /**
     * SOLUTION 4
     */
    println("Enter two category ID")
    val categoryId = List(readLine, readLine)
    val filteredDf = videoDf.filter($"category_id".isin(categoryId: _*))
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

    val result4Df = selectedColDf.orderBy(desc("dislikes")).limit(10)
    result4Df.show()
    result4Df
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("src/main/Resource/Solutin4DataSet")
  }
}
