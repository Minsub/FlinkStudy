package flink.streaming.window

import flink.streaming.basic.datasource.StreamCreator
import flink.streaming.window.Operators.AppendWindowFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WindowFunctions {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 9), 300)).map(_.toString)
    val keyedStream = stream.keyBy(v => if(v.toInt % 2 == 0) "even" else "odd")
    val windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))


    // #1. reduce
    windowedStream.reduce((v1, v2) => s"$v1-$v2")
      //.print()

    // #2. fold
    windowedStream.fold[String]("Start")((start, v) => s"$start-$v")
      //.print()

    // #.3 WindowFunction (Generic case)
    windowedStream.apply(new AppendWindowFunction)
      //.print()

    env.execute("Example window functions")
  }


}
