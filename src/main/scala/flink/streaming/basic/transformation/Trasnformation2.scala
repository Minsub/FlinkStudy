package flink.streaming.basic.transformation

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Trasnformation2 {
  case class WC(word:String, count:Int)

  // String 데이터를 append 시키는 window apply 함수
  val applyAppendKey = (key: String, window: TimeWindow, input: Iterable[String], out: Collector[String]) => {
    var count = 0L
    val sb = new StringBuilder()
    for (in <- input) {
      count += 1
      sb.append(in)
    }
    out.collect(s"Key: $key, Window Count: $count -> ${sb.toString()}")
  }

  val applyAppend = (window: TimeWindow, input: Iterable[String], out: Collector[String]) => {
    applyAppendKey("[empty]", window, input, out)
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 9), 500)).map(_.toString)
    val keyStream = stream.keyBy(v => if (v.toInt % 2 == 0 ) "even" else "odd")

    /* KeyedStream -> WindowedStream */
    // #1. window
    val streamWindow = keyStream.window(TumblingEventTimeWindows.of(Time.seconds(3)))

    /* DataStream -> AllWindowedStream */
    // #2. windowAll
    val streamWindowAll = stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    /* WindowedStream / AllWindowedStream -> DataStream */
    // #3. apply
    streamWindow.apply(applyAppendKey)
      //.print()
    streamWindowAll.apply(applyAppend)
      //.print()

    // #4. (Window) reduce
    streamWindowAll.reduce(_ + _)
      //.print()

    // #5. (Window) fold
    streamWindow.fold("start")((str, v) => s"$str - $v")
      //.print()

    // #6. (Window) aggregations
    env.fromCollection(StreamCreator.source(List.range(1, 9), 300)).map(Integer.valueOf(_))
      .keyBy[String](v => if (v % 2 == 0 ) "even" else "odd")
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .sum(0)
      //.print()

    env.execute("Example transformation 2")
  }

  // Custom Window Function
  class MyWindowFunction extends WindowFunction[String, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[String], out: Collector[String]): Unit = {
      applyAppendKey(key, window, input, out)
    }
  }
}
