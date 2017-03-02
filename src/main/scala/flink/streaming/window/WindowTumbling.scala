package flink.streaming.window

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

object WindowTumbling {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(4)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 9), 430)).map(_.toString)

    // Tumbling: 중복 element 없이 일정 시간 단위 안에 있는 데이터를 처리함
    stream
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply(Operators.appendAllFunction)
      .print()
      .setParallelism(1)


    env.execute("Example window tumbling 1")
  }



}
