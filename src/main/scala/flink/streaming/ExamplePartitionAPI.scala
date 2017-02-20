package flink.streaming

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object ExamplePartitionAPI {

  case class User(name:String, age: Int)
  case class Addr(name: String, addr: String)
  case class Person(name: String, age:Int, addr:String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(3)


    val streamText = env.fromCollection[String](StreamCreator.source[String](List("Apple","Banana","Crona"), 500))
    val streamNumber = env.fromCollection[Int](StreamCreator.source[Int](List(100,200,300,400,500), 400))

    // Normal
//    1> 200
//    2> 300
//    3> 400
//    1> 500
//    2> 100
//    3> 200

    // shuffle: partition에 순서대로 처리하지않고 shuffling함
    streamNumber.shuffle
//    2> 200
//    1> 300
//    2> 400
//    1> 500
//    2> 100
//    2> 200
//    3> 300

    // rebalance: shuffling된 순서를 다시 순서대로 바꿈
    streamNumber.map(_ / 10).shuffle.map(_ * 10).rebalance
//    1> 200
//    2> 300
//    3> 400
//    1> 500
//    2> 100
//    3> 200

    // rescale: (잘모름)
    streamNumber.rescale

    // broadcast: 모든 partition으로 data를 보냄
    streamNumber.broadcast
//    1> 200
//    2> 200
//    3> 200
//    1> 300
//    2> 300
//    3> 300

    // chaining functions

    // startNewChain
    streamNumber.map(_ * 2).startNewChain().map(_ / 2)

    // disableChaining

    //slotSharingGroup
    //streamNumber.filter(_<4).slotSharingGroup("slot1")

    env.execute("Example Partition Stream APIs")
  }

}
