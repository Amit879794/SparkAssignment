package Assignment.rdd

import Assignment.{CommonRespository, Session}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row

import scala.io.StdIn.readLine

class RddSolution2 {
  def solution2rdd(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = new Session().getSession()

    val videoRdd = new CommonRespository().readVideoDatasetAsRdd()

    println("Enter 2 channel title")
    val channelTitle = List(readLine, readLine)
    println("Enter tag key")
    val tagkey = List(readLine)
    println("Enter two category ID")
    val categoryID = List(readLine, readLine)
    val videoFilteredRdd = videoRdd.filter { row =>
      (categoryID contains row.getAs("category_id")) || (channelTitle contains row.getAs("channel_title")) || (tagkey contains row.getAs("tags"))
    }
    val videoSelectedColRdd = videoFilteredRdd
      .map { row =>
        Row(
          row.getAs("video_id"),
          row.getAs("title"),
          row.getAs("channel_title"),
          row.getAs("category_id"),
          row.getAs("likes"),
          row.getAs("dislikes"),
          row.getAs("comment_count"),
          row.getAs("views")
        )

      }
    var csvdata = "video_id,title,channel_title,category_id,views,likes,dislikes,comment_count\n"

    videoSelectedColRdd.foreach {
      row =>
        csvdata +=
          s"""${row.get(0)},${row.get(1)},${row.get(2)},${row.get(3)},${row.get(7)},${row.get(4)},${row.get(5)},${row.get(6)}
             |""".stripMargin
    }
    reflect.io.File("src/main/Resource/solution2rdd.csv").writeAll(csvdata)
    println(csvdata)
  }
}
