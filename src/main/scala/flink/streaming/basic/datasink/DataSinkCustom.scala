package flink.streaming.basic.datasink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object DataSinkCustom {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val input = env.fromCollection(List.range(1, 10))

    input.addSink(new DataSinkCustom())

    env.execute("Example dataSink custom")

    val stream = env.fromCollection(List("A","B","C"))
    stream.writeToSocket("localhost", 9999, new SimpleStringSchema())
  }

  class DataSinkCustom extends RichSinkFunction[Int] {
    override def invoke(value: Int): Unit = {
      println(s"Custom Sink: $value")
    }
  }

}
