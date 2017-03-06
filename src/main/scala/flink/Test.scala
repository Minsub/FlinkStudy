package flink

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

import scala.io.Source
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.io.Source

object Test {

  val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")

  def main(args: Array[String]): Unit = {
    val msg = "2017-03-02 14:04:46 870"

    val date = inputFormat.parse(msg)

    println(msg)
    println(date)
    println(date.getTime)

    val text = "#\uad11\ud654\ubb38 #\uc2e0\uc0ac\ub3d9 #\uac15\ub0a8 \ucf54\ub4dc 2580 \uccab\ucda910% \ub9e4\ucda9 5%"
    println(text)

    val bytes = text.getBytes(StandardCharsets.UTF_8)
    val encoded = Source.fromBytes(bytes, "UTF-8").mkString

    println(encoded)


    val mapper = new ObjectMapper() with ScalaObjectMapper with Serializable
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val lines = Source.fromFile("src/main/resources/sample.json").getLines().toList

    for (line <- lines) {
      try {
        val json = mapper.readValue[Map[String,Any]](line)
        println(json)
      } catch {
        case e: JsonParseException => Unit
      }
    }



  }

}
