package flink.streaming.window

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowSliding {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(4)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 9), 430)).map(_.toString)

    // Sliding: element 중복처리됨. n시간 단위로 y시간 동안 들어온 데이터를 처리함
    stream
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1))) //1초마다 2초간 들어온 모든 데이터
      .apply(Operators.appendAllFunction)
      .print()
      .setParallelism(1)


    env.execute("Example window sliding 1")
  }



}
