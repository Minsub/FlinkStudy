package flink.streaming.basic.transformation

import org.apache.flink.streaming.api.scala.extensions._
import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Trasnformation3 {

  val oddOrEven = (v: String) => if(v.toInt % 2 == 0 ) "even" else "odd"

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

    val stream1 = env.fromCollection(StreamCreator.source(List.range(1, 9), 500)).map(_.toString)
    val stream2 = env.fromCollection(StreamCreator.source(List.range(31, 39), 500)).map(_.toString)
    val keyedStream1 = stream1.keyBy(oddOrEven)
    val keyedStream2 = stream2.keyBy(oddOrEven)

    /* DataStream* -> DataStream */
    // #1. Union
    stream1.union(stream2)
      //.print()

    /* DataStream, DataStream -> DataStream */
    // #2. join: (row 별로 단순 조인)
    stream1.join(stream2)
      .where(oddOrEven).equalTo(oddOrEven)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply((_, _))
      //.print()

    // #3. coGroup: (Window 단위로 Iterator + Iterator 조합)
    stream1.coGroup(stream2)
      .where(oddOrEven).equalTo(oddOrEven)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply((a,b) => (a.toList, b.toList))
      //.print()

    /* DataStream, DataStream -> ConnectedStream */
    // #4. Connect
    val connectedStream = stream1.map("sss"+_).connect(stream2.map(_.toInt))
    // #4-1. (Connected) map
    connectedStream.map(v1 => v1, v2=> v2.toString)
      //.print()

    /* DataStream -> SplitStream */
    // #5. split
    val splitStream = stream1.split((v: String) => {
      (v.toInt % 2) match {
        case 0 => List("even","all")
        case 1 => List("odd","all")
      }
    })

    splitStream.select("all").print()



    //val keyStream = stream.keyBy(v => if (v.toInt % 2 == 0 ) "even" else "odd")

    env.execute("Example transformation 3")
  }
}
