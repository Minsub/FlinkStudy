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
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 10), 300)).map(_.toString)

    // Session: elements의 입력 gap의 시간 설정으로 windows 단위를 나눔
    stream
      .windowAll(EventTimeSessionWindows.withGap(Time.milliseconds(500)))
      .apply(Operators.appendAllFunction)
      .print()

    env.execute("Example window session 1")
  }
}
