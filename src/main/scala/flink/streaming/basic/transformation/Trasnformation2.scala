package flink.streaming.basic.transformation

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Trasnformation2 {

  case class WC(word:String, count:Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 10), 500))
    val keyStream = stream.map(i => (i % 2, i)).keyBy(0)

    /* Stream -> Stream */
    // #1. window
    val streamWindow = keyStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))

    streamWindow.apply((conf: Tuple, time:TimeWindow, input: Iterable[(Int, Int)], out: Collector[(Int, Int)]) => {
      var sum = 0
      var key = 0
      for (in <- input) {
        key = in._1
        sum += in._2
      }
      out.collect((key, sum))
    }).print()



    env.execute("Example transformation 2")
  }

}
