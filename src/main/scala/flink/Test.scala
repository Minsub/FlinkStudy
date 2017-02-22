package flink

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


object Test {

  val person1 = """{
  "name": "Joe Doe",
  "age": 45,
  "kids": ["Frank", "Marta", "Joan"],
  "info": {
    "addr":"seoul",
    "phone":"010-1234-1234"
  }
}"""

  def main(args: Array[String]): Unit = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val node = mapper.readValue[JsonNode](person1)

    println(node)

    println(node.get("info").get("addr"))

  }

}
