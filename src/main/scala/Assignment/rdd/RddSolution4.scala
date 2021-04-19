package Assignment.rdd

import Assignment.CommonRespository
import org.apache.spark.sql.Row

import java.util.logging.{Level, Logger}
import scala.io.StdIn.readLine

class RddSolution4 {
  def solution4rdd(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val videoRdd = new CommonRespository().readVideoDatasetAsRdd()

    println("Enter two category ID")
    val categoryId = List(readLine, readLine)
    val videoFilteredRdd = videoRdd.filter { row => categoryId contains row.getAs("category_id") }
    val videoSelectedColRdd = videoFilteredRdd
      .map { row =>
        (Row(
          row.getAs("video_id"),
          row.getAs("title"),
          row.getAs("channel_title"),
          row.getAs("category_id"),
          row.getAs("likes"),
          row.getAs("dislikes"),
          row.getAs("comment_count"),
          row.getAs("views")), Integer.parseInt(row.getAs("dislikes")))
      }

    val result1 = videoSelectedColRdd.sortBy(_._2)
    val top10 = result1.take(10).reverse
    var csvdata = "video_id,title,channel_title,category_id,views,likes,dislikes,comment_count\n"

    top10.foreach {
      row =>
        csvdata +=
          s"""${row._1.get(0)},${row._1.get(1)},${row._1.get(2)},${row._1.get(3)},${row._1.get(7)},${row._1.get(4)},${row._1.get(5)},${row._1.get(6)}
             |""".stripMargin
        println(row._1)
    }
    reflect.io.File("src/main/Resource/solution4rdd.csv").writeAll(csvdata)
  }

}
