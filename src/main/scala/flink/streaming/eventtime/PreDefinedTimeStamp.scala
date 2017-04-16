package flink.streaming.eventtime

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PreDefinedTimeStamp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.sourceWithTimestamp(List.range(1, 10), 500))

    stream
      .assignAscendingTimestamps(_._3)
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply((time: TimeWindow, input: Iterable[(Int, String, Long)], out: Collector[String]) => {
        val time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss SSS"))
        out.collect(s"time: $time, Count: ${input.size} : ${input.map(_._1.toString).reduce(_+"-"+_)}")
      })
      .print()

    env.execute("Example event time 1")
  }

}
