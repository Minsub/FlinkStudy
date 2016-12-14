package flink.streaming.connector

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * Created by hmm1115222 on 2016-12-14.
  */
object ExampleKafkaComsumer {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(2)

    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181");
    properties.setProperty("group.id", "Flink Connector");
    val consumer = new FlinkKafkaConsumer09[String]("test", new SimpleStringSchema(), properties)
    val stream = env.addSource(consumer)


    stream.print()

    env.execute("Flink Kafka Consumer")
  }
}
