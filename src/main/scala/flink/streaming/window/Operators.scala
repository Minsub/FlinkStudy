package flink.streaming.window

import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by hmm1115222 on 2017-03-02.
  */
object Operators {

  // String 데이터를 append 시키는 window apply 함수
  val appendFunction = (key: String, window: TimeWindow, input: Iterable[String], out: Collector[String]) => {
    var count = 0L
    val sb = new StringBuilder()
    for (in <- input) {
      count += 1
      sb.append(in)
    }
    out.collect(s"Key: $key, Window Count: $count -> ${sb.toString()}")
  }

  val appendAllFunction = (window: TimeWindow, input: Iterable[String], out: Collector[String]) => {
    appendFunction("[empty]", window, input, out)
  }

  class AppendAllWindowFunction extends AllWindowFunction[String, String, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[String], out: Collector[String]): Unit = {
      appendAllFunction(window, input, out)
    }
  }

  class AppendWindowFunction extends WindowFunction[String, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[String], out: Collector[String]): Unit = {
      appendFunction(key, window, input, out)
    }
  }
}
