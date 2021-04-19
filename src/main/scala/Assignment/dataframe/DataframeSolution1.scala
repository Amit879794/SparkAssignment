package Assignment.dataframe

import Assignment.{CommonRespository, Session}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.desc

import scala.io.StdIn.readLine

class DataframeSolution1 {

  def solution1dataframe(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = new Session().getSession()
    val videoDf = new CommonRespository().readVideoDatasetAsDataFrame()
    import spark.implicits._
    /**
     * SOLUTION 1
     */
    println("Enter 3 video id")
    val inputVideoIds = List(readLine(), readLine(), readLine())
    val filteredDf = videoDf.filter($"video_id".isin(inputVideoIds: _*))
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
    val result1Df = selectedColDf.orderBy(desc("views")).limit(50)
    val resultjson = result1Df.toJSON
    resultjson.show()
    resultjson.write.json("src/main/Resource/Solution1Dataframe")
  }

}
