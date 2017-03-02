package flink.streaming.window

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowSession {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(4)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 4), 500)).map(_.toString)

    // Session: elements의 입력 gap의 시간 설정으로 windows 단위를 나눔
    stream
      .windowAll(EventTimeSessionWindows.withGap(Time.milliseconds(800)))
      .apply(Operators.appendAllFunction)
      .print()
      .setParallelism(1)




    env.execute("Example window session 1")
  }



}
