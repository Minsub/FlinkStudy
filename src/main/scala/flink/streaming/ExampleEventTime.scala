package flink.streaming

import flink.generater.StreamCreator
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object ExampleEventTime {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)


    val streamText = env.fromCollection[String](StreamCreator.source[String](List("Apple","Banana","Crona"), 500))
    val streamNumber = env.fromCollection[Int](StreamCreator.source[Int](List(100,200,300,400,500), 400))

    //streamNumber.timeWindowAll(Time.seconds(3))
    streamNumber.map((_,1)).keyBy(0).timeWindow(Time.seconds(5)).sum(0)

    streamNumber.map((1,_)).keyBy(0).timeWindow(Time.seconds(3)).sum(1).map(_._2).print()

    env.execute("Example Event Time")
  }

}
