package flink.streaming.basic

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

object StreamDataCode {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    env.fromCollection(StreamCreator.source(List.range(1, 10), 1000)).setBufferTimeout(500).print()


    env.execute("Example basic iteration")
  }


}
