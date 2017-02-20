package flink.streaming

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object ExampleMultipleStreamAPI {

  case class User(name:String, age: Int)
  case class Addr(name: String, addr: String)
  case class Person(name: String, age:Int, addr:String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val list1 = List(("A",1),("B",2),("C",3))
    val list2 = List(("Apple",100),("Banana",230))
    val streamTuple1 = env.fromCollection[(String, Int)](StreamCreator.source[(String, Int)](list1, 200))
    val streamTuple2 = env.fromCollection[(String, Int)](StreamCreator.source[(String, Int)](list2, 100))

    val streamUser = env.fromCollection[User](StreamCreator.source[User](List(User("MS",21),User("AA",45), User("MS",33)), 300))
    val streamAddr = env.fromCollection[Addr](StreamCreator.source[Addr](List(Addr("MS","Seoul"),Addr("AA","USA")), 500))

    val streamText = env.fromCollection[String](StreamCreator.source[String](List("Apple","Banana","Crona"), 500))
    val streamNumber = env.fromCollection[Int](StreamCreator.source[Int](List(1,2,3,4,5), 400))

    // union : 동일한 element 타입을 가지는 Stream을 합침
    streamTuple1.union(streamTuple2)


    // join: inner join
    streamUser.join(streamAddr)
      .where(_.name)
      .equalTo(_.name)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply((user, addr) => Person(user.name, user.age, addr.addr))


    // coGroup: join과 같이 두 스트림을 합치지만 iterator를 받아서 grouping하는 동작을 만들 수 있음
    streamUser.coGroup(streamAddr)
      .where(_.name).equalTo(_.name)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply((iterUser, iterAddr) => {
        var count = 0
        var ageSum = 0

        iterUser.foreach(u => {
          count += 1
          ageSum += u.age
        })

        val addrSum = iterAddr.map[String](_.addr).reduce(_+ "-" + _)
        (count, ageSum, addrSum)
      })


    // connect
    val connectedStream = streamText.connect(streamNumber)

    // [connectedStream] coMap (ConnectedStreams -> DataStream) : 두개 다른 element 타입을 가진 stream을 합쳐서 동일한 타입의 output
    connectedStream.map(x => ("String", x), y => ("Int", y.toString))

    // [connectedStream] coFlatMap (ConnectedStreams -> DataStream) : 기본 map 과 flatMap의 차이와 동일
    connectedStream.flatMap((x: String) => x.split(" "), (y:Int) => Array(y.toString))


    // split : 2개 이상의 기준 로직을 만듬
    val splitStream = streamNumber.split((x: Int) => {
      (x % 2) match {
        case 0 => List("even")
        case 1 => List("odd")
      }
    })

    // [SplitStream]select (SplitStream -> DataStream) : split 기준으로 filtering
    splitStream.select("even")
    splitStream.select("even", "odd")

    // TODO:
    // iterate: 잘모르겠음
    splitStream.iterate(iter => {
      (iter.filter(_ < 3), iter.filter(_ >= 3))
    })


    // extract timestamps:
    streamText.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[String] {
      override def getCurrentWatermark: Watermark = {
        new Watermark(100)
        //TODO
      }

      override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
        previousElementTimestamp + 1
        //TODO
      }
    })

    env.execute("Example Multiple Stream APIs")
  }

}
