package flink.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object ExampleWindowAPI {
  case class WordCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val streamWC = env.fromElements(WordCount("H",1), WordCount("A",4), WordCount("H", 5))

    val streamSocket = env.socketTextStream("localhost", 9999)
    val keyedStreamWord = streamSocket.flatMap(_.split(" ")).map((_, 1)).keyBy(0)

    // window: element를 처리하는 범위를 지정
    //keyedStreamWord.window(TumblingEventTimeWindows.of(Time.seconds(3))).sum(1).print()
    //keyedStreamWord.timeWindow(Time.seconds(5), Time.seconds(1)).sum(1).print()
    //TODO: 여기서부터! https://ci.apache.org/projects/flink/flink-docs-release-1.1/apis/streaming/index.html


    // windowAll: non-parallel transformation 으로 하나의 task로 처리
    //streamWord.map((_,1)).keyBy(0).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))

    // [window] apply: 커스텀 함수
    //keyedStreamWord.timeWindow(Time.seconds(5), Time.seconds(1)).apply(WindowFunction)

    // [window] reduce: reduce 랑 동일
    keyedStreamWord.timeWindow(Time.seconds(5), Time.seconds(1)).reduce((x,y) => (x._1, x._2 + y._2))

    // [window] fold: fold랑 동일

    // [window] aggregations

    // union

    // coGroup

    // connect

    // [connectedStream] coMap

    // [connectedStream] coFlatMap

    // split

    // select

    // iterate

    // extract timestamps

    env.execute("Example Window APIs")
  }

}
