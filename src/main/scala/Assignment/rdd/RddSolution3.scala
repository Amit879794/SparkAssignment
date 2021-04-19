package Assignment.rdd

import Assignment.{CommonRespository, Session}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row

import scala.io.StdIn.readLine

class RddSolution3 {

  def solution3rdd(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = new Session().getSession()
    val videoRdd = new CommonRespository().readVideoDatasetAsRdd()

    println("Enter Channel Title")
    val channelTitle = List(readLine, readLine)
    val videoFilteredRdd = videoRdd.filter { row => channelTitle contains row.getAs("channel_title") }
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
          row.getAs("views")),
          (Integer.parseInt(row.getAs("likes")) + Integer.parseInt(row.getAs("comment_count"))
            + Integer.parseInt(row.getAs("views"))))
      }

    val result1 = videoSelectedColRdd.sortBy(_._2)
    val top10 = result1.take(10).reverse
    top10.foreach(row => println(row._1))
    var csvdata = "video_id,title,channel_title,category_id,views,likes,dislikes,comment_count\n"

    top10.foreach {
      row =>
        csvdata +=
          s"""${row._1.get(0)},${row._1.get(1)},${row._1.get(2)},${row._1.get(3)},${row._1.get(7)},${row._1.get(4)},${row._1.get(5)},${row._1.get(6)}
             |""".stripMargin
    }
    reflect.io.File("src/main/Resource/solution3rdd.csv").writeAll(csvdata)
    val prompFile = spark
      .read
      .option("header", true)
      .option("inferschema", true)
      .csv("src/main/Resource/solution3rdd.csv")
    prompFile.write.parquet("src/main/Resource/Soultion3rddparquet")
  }

}
