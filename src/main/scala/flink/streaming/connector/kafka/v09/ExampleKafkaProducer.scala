package flink.streaming.connector.kafka.v09

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * Created by hmm1115222 on 2016-12-14.
  */
object ExampleKafkaProducer {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(2)

    val input = env.fromElements("[START] this is test message [END]")

    val stream = input.flatMap(_.split(" "))

    stream.addSink(new FlinkKafkaProducer09[String]("localhost:9092", "test", new SimpleStringSchema()))

    env.execute("Flink Kafka Producer")
  }
}
