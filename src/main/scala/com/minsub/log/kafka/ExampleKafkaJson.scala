package com.minsub.log.kafka

import java.util.Properties

import org.apache.flink.streaming.api.windowing.time.Time
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization.{JSONDeserializationSchema, _}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

object ExampleKafkaJson {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000) // required for commit offset by consumer group
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test-group");
    properties.put("key.deserializer", classOf[StringDeserializer].getName)
    properties.put("value.deserializer", classOf[StringDeserializer].getName)

    val consumer = new FlinkKafkaConsumer09[ObjectNode]("test", new KafkaJsonDeserialization(), properties)
    val input = env.addSource(consumer)

//    input.map((value:ObjectNode) => {
//      "Flink: " + value.getClass + " / " + value.toString
//    }).print()

    input
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
      .apply((time: TimeWindow, input: Iterable[ObjectNode], out: Collector[String]) => {
        var count = 0L
        var sum = 0L

        for (in <- input) {
          try {
            if (!in.isNull()) {
              println(in)
              count += 1
              sum += in.get("point").asLong()
            }
          } catch {
            case e: Exception => Unit
          }
        }
        out.collect(s"Window Count: $count , Sum: ${sum}")
      })
      .print()


    env.execute("Flink Kafka Consumer")
  }
}
