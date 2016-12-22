package flink.sample

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SampleApp2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val text = env.socketTextStream("localhost", 9999)

    val count = text
        .flatMap(_.split(" "))
        .map((_, 1))
        .keyBy(0)
        .timeWindow(Time.seconds(5), Time.seconds(1))
        .sum(1)

    count.print()

    env.execute("Socket WordCount Sample1")

    //nc -lk 9999
  }

}
