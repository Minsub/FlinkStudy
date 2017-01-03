package flink.streaming.connector.elasticsearch

//import java.util

//import org.apache.flink.api.common.functions.RuntimeContext
import java.net.{InetAddress, InetSocketAddress}
import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch2.{ElasticsearchSink, ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.client.Requests


object ExampleElasticsearchSink {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(2)
//
    val input = env.fromElements("[START] this is test message [END]")
    val stream  = input.flatMap(_.split(" "))

    val transports = new util.ArrayList[InetSocketAddress]
    transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"),9200))


    val config = new util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", "elasticsearch")

    stream.addSink(new ElasticsearchSink[String](config, transports, new ElasticsearchSinkFunction[String] {
      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        val json = new util.HashMap[String, AnyRef]
        json.put("data", element)
        println("SENDING: " + element)
        Requests.indexRequest.index("flink-1").`type`("my-type").source(json)
      }
    }))


    env.execute("Flink Elasticsearch sink")
  }
}
