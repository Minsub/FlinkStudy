package flink.batch

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration


object ExampleDataSinks {
  val PATH = "src/main/resources/"

  def main(args: Array[String]): Unit = {
    val env  = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


  }
}
