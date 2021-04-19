package Assignment.sql

import Assignment.{CommonRespository, Session}
import org.apache.log4j.{Level, Logger}

import scala.io.StdIn.readLine

class SqlSolution1 {
  def solution1sql(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = new Session().getSession()

    new CommonRespository().readVideoDatasetAsSql()
    println("Enter 3 video id")
    val inputVideoIds = List(readLine, readLine, readLine)
    val result1 = spark.sql(
      s"""select
         |video_id,title,
         |channel_title,
         |category_id,
         |likes,dislikes,
         |comment_count,
         |views from videoTable
         |where video_id in (${inputVideoIds.map(x => "'" + x + "'").mkString(",")})
         |order by views desc
         |limit 50""".stripMargin
    )

    //  result1.toJSON.show(false)
    val resultjson = result1.toJSON
    resultjson.show()
    resultjson.write.json("src/main/Resource/Solution1SQL")
  }
}
