package flink.streaming.connector.kafka.v010

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.common.serialization.StringDeserializer

object ExampleKafka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000) // required for commit offset by consumer group
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(3)

    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test-group");
    //properties.put("key.deserializer", classOf[StringDeserializer].getName)
    //properties.put("value.deserializer", classOf[StringDeserializer].getName)

    val consumer = new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), properties)
    val input = env.addSource(consumer)

    input
      .map(_ + " by Flink")
      //.addSink(new FlinkKafkaProducer010[String]("localhost:9092", "test-flink", new SimpleStringSchema()))
      .print()

    env.execute("Flink Kafka Consumer")
  }
}
