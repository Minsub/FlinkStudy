package flink.streaming.basic.transformation

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Trasnformation2 {

  case class WC(word:String, count:Int)

  // String 데이터를 append 시키는 window apply 함수
  val applyAppend:(String, TimeWindow, Iterable[String], Collector[String]) => Unit = (key, window, input, out) => {
    var count = 0L
    val sb = new StringBuilder()
    for (in <- input) {
      count += 1
      sb.append(in)
    }
    out.collect(s"Window Count: $count -> ${sb.toString()}")
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 9), 500))
    val keyStream = stream.map(_.toString).keyBy(v => "key")

    /* Stream -> Stream */
    // #1. window
    keyStream
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(applyAppend)
      .print()

    keyStream
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new MyWindowFunction)
      //.print()

    env.execute("Example transformation 2")
  }

  // Custom Window Function
  class MyWindowFunction extends WindowFunction[String, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[String], out: Collector[String]): Unit = applyAppend
  }
}
