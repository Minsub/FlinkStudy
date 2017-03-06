package flink.streaming.connector.twitter

import java.util.Properties

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StreamingEndpoint, Location}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.twitter.TwitterSource

import scala.collection.JavaConverters._

object ExampleTwitter extends App {

//  class FilterEndpoint(terms: List[String], locations: List[Location]) extends TwitterSource.EndpointInitializer with Serializable{
  //    override def createEndpoint(): StreamingEndpoint = {
  //      val endpoint = new StatusesFilterEndpoint()
  //      endpoint.trackTerms(terms.asJava)
  //      if (locations != null) {
  //        endpoint.locations(locations.asJava)
  //      }
  //      return endpoint
  //    }
  //  }



  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.enableCheckpointing(2000)
  env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
  env.setParallelism(1)

  val props = new Properties();
  props.setProperty(TwitterSource.CONSUMER_KEY, "qJwZvBabbjcQqCf76gyrxtE79");
  props.setProperty(TwitterSource.CONSUMER_SECRET, "dgidakMEVPO2E38yfPn9dneX3shOAmnuTUOMfjyzSmLVhOhwFH");
  props.setProperty(TwitterSource.TOKEN, "137619569-k5NGuABmcqOujeLTuEorus5r4P2bYJbqcCSsrUUe");
  props.setProperty(TwitterSource.TOKEN_SECRET, "UDsCromQ4non3qrIu2jCOvbc1T11IcSrPEBbEA6AZqNs0");

  val source = new TwitterSource(props)
  val terms = List("starbucks")
  val locationSeoul = new Location(new Location.Coordinate(126.82, 37.47), new Location.Coordinate(127.11, 37.61))
  val locations = List(locationSeoul)

  source.setCustomEndpointInitializer(new FilterEndpoint)
  val streamTwitter = env.addSource(source);

  streamTwitter
    .map(msg => {
      try {
        val mapper = new ObjectMapper() with ScalaObjectMapper with Serializable
        mapper.registerModule(DefaultScalaModule)
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.readValue[Map[String,Any]](msg)
      } catch {
        case jpe: JsonParseException => Map()
        case e: Exception => Map()
      }
    })
    .filter(_.nonEmpty)
    .print()

  env.execute("Flink twitter connector")

  class FilterEndpoint() extends TwitterSource.EndpointInitializer with Serializable {
    override def createEndpoint(): StreamingEndpoint = {
      val endpoint = new StatusesFilterEndpoint()
      endpoint.trackTerms(terms.asJava)  // keyword filter
      endpoint.locations(locations.asJava)  // location filter
      return endpoint
    }
  }
}


