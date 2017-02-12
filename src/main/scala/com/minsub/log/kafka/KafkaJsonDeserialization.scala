package com.minsub.log.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema

class KafkaJsonDeserialization extends JSONDeserializationSchema {
  override def deserialize(message: Array[Byte]): ObjectNode = {
    try {
      super.deserialize(message)
    } catch {
      //case e: Exception => new ObjectMapper().createObjectNode()
      case e: Exception => null
    }
  }
}
