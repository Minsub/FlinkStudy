package flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Test {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)



    env.execute("Test job print")
  }



}
