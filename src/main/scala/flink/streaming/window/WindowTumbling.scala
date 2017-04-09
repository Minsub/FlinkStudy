package flink.streaming.window

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTumbling {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 10), 500)).map(_.toString)

    // Tumbling: 중복 element 없이 일정 시간 단위 안에 있는 데이터를 처리함
    stream
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply(Operators.appendAllFunction)
      .print()

    env.execute("Example window tumbling 1")
  }
}
