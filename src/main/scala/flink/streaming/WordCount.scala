package flink.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val count = text
        .flatMap(_.split(" "))
        .map((_, 1))
        .keyBy(0)
        .timeWindow(Time.seconds(5), Time.seconds(1))
        .sum(1)

    count.print().setParallelism(1)

    env.execute("Socket WordCount")

    //nc -lk 9999
  }

}
