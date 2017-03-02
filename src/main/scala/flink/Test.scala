package flink

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

import scala.io.Source


object Test {

  val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")

  def main(args: Array[String]): Unit = {
    val msg = "2017-03-02 14:04:46 870"

    val date = inputFormat.parse(msg)

    println(msg)
    println(date)
    println(date.getTime)

  }

}
