package flink.sample

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SampleApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val input = env.fromCollection[Int](StreamCreator.source[Int](List.range(1, 10), 200))

    input.map((_, 1))
      .timeWindowAll(Time.seconds(5), Time.seconds(2))
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
      .print()

    env.execute("Flink Run Test1")
  }

}
