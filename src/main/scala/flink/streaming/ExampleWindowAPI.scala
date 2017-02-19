package flink.streaming

import flink.generater.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object ExampleWindowAPI {
  case class WC(word: String, count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)


    val input = env.fromCollection(StreamCreator.source(List.range(1, 10), 100))

    val keyedStream = input.map(_.toString+"-").keyBy(x => "key")

    // tumbling window: n시간만큼의 window size
    val tumblingWindow = TumblingEventTimeWindows.of(Time.seconds(3))
    // sliding window: tubple을 중복으러 처리함. x 마다 y 만큼 시간 동안 들어온 데이터를 기준으로 계산
    val slidingWindow = SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(3))
    // session window: 데이터가 들어오지 않는 interval 타임까지의 데이터가 window size
    val sessionWindow = EventTimeSessionWindows.withGap(Time.milliseconds(200))
    // global window:
    val globalWindow = GlobalWindows.create()


    val windowedStream = keyedStream.window(sessionWindow)
    //val windowedStream = keyedStream.window(globalWindow)

    // allowedLateness: late data를 처리
//    windowedStream.allowedLateness(Time.milliseconds(100))

    // trigger
//    windowedStream.trigger(new MyTrigger())

//    val output = windowedStream.reduce((x,y) => x + y)
    val output =  windowedStream.apply[String](new MyWindowFunction())
//    val output =  windowedStream.apply[String](mergeText)


    //output.print()


    // None-keyed Windowing: windowAll
    val nkWindowStream = input.map(_.toString+"-").windowAll(sessionWindow)
    nkWindowStream.apply((time: TimeWindow, input: Iterable[String], out: Collector[String]) => {
      var count = 0L
      val sb = new StringBuilder
      for (in <- input) {
        count += 1
        sb.append(in)
      }
      out.collect(s"Window Count: $count -> ${sb.toString()}")
    }).print()

    env.execute("Example Window APIs")
  }

  val mergeText =
    (key: String, window: TimeWindow, input: Iterable[String], out: Collector[String]) => {
    var count = 0L
    val sb = new StringBuilder()
    for (in <- input) {
      count += 1
      sb.append(in)
    }
    out.collect(s"Window Count: $count -> ${sb.toString()}")
  }

  // Custom Window Function
  class MyWindowFunction extends WindowFunction[String, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[String], out: Collector[String]): Unit = {
      var count = 0L
      val sb = new StringBuilder()
      for (in <- input) {
        count += 1
        sb.append(in)
      }
      out.collect(s"Window Count: $count -> ${sb.toString()}")
    }
  }

  class MyTrigger extends Trigger[String, TimeWindow] {
    override def onElement(element: String, timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = ???

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = ???

    override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = ???

    override def clear(window: TimeWindow, ctx: TriggerContext): Unit = ???
  }
}
