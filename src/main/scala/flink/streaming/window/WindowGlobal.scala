package flink.streaming.window

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowGlobal {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 10), 1000)).map(_.toString)

    // Global: trigger, evictor 를 사용하기 위한 window
    // trigger는 가져올 data에 대한 정의고 evicotr 버릴 데이터에 대한 정의
    stream
      .windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of(3))
      //.evictor(CountEvictor.of(5))
      .apply((window: GlobalWindow, input: Iterable[String], out: Collector[String]) => {
        var count = 0L
        val sb = new StringBuilder()
        for (in <- input) {
          count += 1
          sb.append(in)
        }
        val time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss SSS"))
        out.collect(s"time: $time, Count: $count -> ${sb.toString()}")
      })
      .print()

    env.execute("Example window global 1")
  }
}
