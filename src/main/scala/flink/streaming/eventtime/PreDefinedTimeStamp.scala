package flink.streaming.eventtime

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PreDefinedTimeStamp {
  val PATH = "src/main/resources/myEvent.csv"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.sourceWithTimestamp(List.range(1, 10), 500))

    stream
      .assignAscendingTimestamps(_._3)
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply((time: TimeWindow, input: Iterable[(Int, String, Long)], out: Collector[String]) => {
        var count = 0L
        val sb = new StringBuilder
        for (in <- input) {
          count += 1
          sb.append("-" + in._1)
        }
        out.collect(s"Window Count: $count : ${sb.toString()}")
      })
      .print()

    env.execute("Example event time 1")
  }

}
