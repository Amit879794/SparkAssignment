package Assignment.dataframe

import Assignment.{CommonRespository, Session}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.desc

import scala.io.StdIn.readLine

class DataframeSolution3 {

  def solution3dataframe(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = new Session().getSession()
    val videoDf = new CommonRespository().readVideoDatasetAsDataFrame()
    import spark.implicits._
    /**
     * SOLUTION 3
     */
    println("Enter Channel Title")
    val channelTitle = List(readLine, readLine)
    val filteredDf = videoDf.filter($"channel_title".isin(channelTitle: _*))
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
    val aggColDf = selectedColDf.
      withColumn("aggregated", $"likes" + $"comment_count" + $"views")
    val result2Df = aggColDf.orderBy(desc("aggregated")).limit(10)
    result2Df.show()
    result2Df.write.parquet("src/main/Resource/Soultion3Dataframe")
  }
}
