package flink.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {

  case class WordCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val count = text
      .flatMap(_.split(" "))
      .map(WordCount(_, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    count.print().setParallelism(1)

    env.execute("Socket WordCount")

    //nc -lk 9999
  }

}
