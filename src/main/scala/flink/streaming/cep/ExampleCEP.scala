package flink.streaming.cep

import flink.generater.StreamCreator
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created by hmm1115222 on 2016-12-14.
  */
object ExampleCEP {
  case class Event(id: Int, name: String)
  case class TimeEvent(id: Int, name: String, timeStamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(2)

    val input = env.fromCollection(StreamCreator.source(List.range(1, 10), 500))

    val names = List("Apple", "Banana", "Corona")
    val streamEvent = input.map(x => Event(x, names(x%3)))

    //create pattern
    val pattern = Pattern
      .begin[Event]("start").where(_.id < 5)
      .next("end").where(_.name == "Apple").within(Time.seconds(3))


    // create patternStream
    val patternStream = CEP.pattern[Event](streamEvent, pattern)

    // select
    val selectStream = patternStream.select(pattern => {
      val start = pattern.get("start").get
      val end = pattern.get("end").get
      (start, end)
    })

    // flatSelect
    val flatSelectStream = patternStream.flatSelect((pattern, collector: Collector[(Event, Event)]) => {
      val start = pattern.get("start").get
      val end = pattern.get("end").get
      for (i <- 0 to 3) {
        collector.collect((start, end))
      }
    })

//    val timeSelectStream = patternStream.select((pattern, timestamp) => {
//      val event = pattern.get("end").get
//      TimeEvent(event.id, event.name, timestamp)
//    })  { pattern: scala.collection.mutable.Map[String, Event] =>  pattern.get("end").get }
//
//    timeSelectStream.print()



    env.execute("Flink CEP")
  }
}
