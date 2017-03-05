package flink.sample.json

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object JsonMessage {

  val PATH = "src/main/resources/sample.json"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.readTextFile(PATH)

    stream
      .map(s => {
        val mapper = new ObjectMapper() with ScalaObjectMapper with Serializable
        mapper.registerModule(DefaultScalaModule)
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.readValue[Map[String,Any]](s)
      })
      .print()

    env.execute("Example json stream 1")
  }
}
