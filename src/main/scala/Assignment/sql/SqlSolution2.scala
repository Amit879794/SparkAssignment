package Assignment.sql

import Assignment.{CommonRespository, Session}
import org.apache.log4j.{Level, Logger}

import scala.io.StdIn.readLine

class SqlSolution2 {

  def solution2sql(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = new Session().getSession()
    new CommonRespository().readVideoDatasetAsSql()

    /**
     * SOLUTION 4
     */
    println("Enter 2 channel title")
    val channelTitle = List(readLine, readLine)

    println("Enter two category ID")
    val categoryId = List(readLine, readLine)

    val result4 = spark.sql(
      s"""select
         |video_id,title,
         |channel_title,
         |category_id,
         |likes,dislikes,
         |comment_count,
         |views
         |from videoTable
         |where category_id in (${categoryId.map(x => "'" + x + "'").mkString(",")}) or channel_title in (${channelTitle.map(x => "'" + x + "'").mkString(",")})
       """.stripMargin
    )

    result4.show()
    result4
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("src/main/Resource/Solutin2SQL")
  }
}
