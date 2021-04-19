package Assignment.sql

import Assignment.{CommonRespository, Session}
import org.apache.log4j.{Level, Logger}

import scala.io.StdIn.readLine

class SqlSolution3 {
  def solution3sql(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = new Session().getSession()

    new CommonRespository().readVideoDatasetAsSql()

    /**
     * SOLUTION 3
     */
    println("Enter Channel Title")
    val channelTitle = List(readLine, readLine)
    val result3 = spark.sql(
      s"""select
         |video_id,title,
         |channel_title,
         |category_id,
         |likes,dislikes,
         |comment_count,
         |views,
         |(likes + comment_count + views) as aggregated
         |from videoTable
         |where channel_title in (${channelTitle.map(x => "'" + x + "'").mkString(",")})
         |order by aggregated desc
         |limit 10""".stripMargin
    )

    result3.show()

    result3.write.parquet("src/main/Resource/Soultion3SQL")
  }
}
