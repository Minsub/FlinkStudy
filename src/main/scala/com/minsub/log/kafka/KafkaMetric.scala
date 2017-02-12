package com.minsub.log.kafka

import java.util.Properties

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.common.serialization.StringDeserializer

import scala.util.parsing.json.JSON

object KafkaMetric {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000) // required for commit offset by consumer group
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(3)

    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "192.168.0.32:9092,192.168.0.33:9092");
    properties.setProperty("group.id", "metric-group-flink");
    properties.put("key.deserializer", classOf[StringDeserializer].getName)
    properties.put("value.deserializer", classOf[StringDeserializer].getName)

    val consumer = new FlinkKafkaConsumer09[String]("metric", new SimpleStringSchema(), properties)
    val input = env.addSource(consumer)

    //input.map(JSON.parseFull(_)).print()
    input.map((value: String) => {
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.readValue[Map[String,Any]](value)
    }).print()

    env.execute("metric-kafka")
  }
}
