package Assignment.rdd

import Assignment.CommonRespository
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row

import scala.io.StdIn.readLine

class RddSolution1 {
  def solution1rdd(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val videoRdd = new CommonRespository().readVideoDatasetAsRdd()

    println("Enter 3 video id")
    val inputVideoIds = List(readLine, readLine, readLine)
    val videoFilteredRdd = videoRdd.filter { row => inputVideoIds contains row.getAs("video_id") }
    //  val videoFilteredRdd = videoRdd.filter($"video_id".isin(inputVideoIds))
    val videoSelectedColRdd = videoFilteredRdd
      .map { row =>
        (Row(
          row.getAs("video_id"),
          row.getAs("title"),
          row.getAs("channel_title"),
          row.getAs("category_id"),
          row.getAs("likes"),
          row.getAs("dislikes"),
          row.getAs("comment_count")), row.getAs("views").toString)
      }

    val result1 = videoSelectedColRdd.sortBy(_._2)
    val top50 = result1.take(50).reverse
    var jsonstr = ""
    var c = 0

    top50.foreach {
      row =>
        if (c == top50.length - 1)
          jsonstr +=
            s"""{"value":"{"video_id":"${row._1.get(0)}","title":"${row._1.get(1)}",
               |"channel_title":"${row._1.get(2)}","category_id":"${row._1.get(3)}","likes":"${row._1.get(4)}",
               |"dislikes":"${row._1.get(5)}","comment_count":"${row._1.get(6)}","views":"${row._2}"}"}
               |""".stripMargin
        else
          jsonstr +=
            s"""{"value":"{"video_id":"${row._1.get(0)}","title":"${row._1.get(1)}",
               |"channel_title":"${row._1.get(2)}","category_id":"${row._1.get(3)}","likes":"${row._1.get(4)}",
               |"dislikes":"${row._1.get(5)}","comment_count":"${row._1.get(6)}","views":"${row._2}"}"},
               |""".stripMargin
        c = c + 1

    }
    reflect.io.File("src/main/Resource/solution1rdd.json").writeAll(jsonstr)
    println(jsonstr)
  }
}
