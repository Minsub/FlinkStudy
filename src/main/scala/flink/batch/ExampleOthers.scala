package flink.batch

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration


object ExampleOthers {
  val PATH = "src/main/resources/"

  def main(args: Array[String]): Unit = {
    val env  = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val toFilter = env.fromElements(1, 2, 3, 4, 5)

    val config = new Configuration()
    config.setInteger("limit", 2)

    // set global configuration
    //env.getConfig.setGlobalJobParameters(config)

    val result = toFilter.filter(new RichFilterFunction[Int]() {
      var limit = 0
      override def open(config: Configuration): Unit = {
        limit = config.getInteger("limit", 0)
      }

      def filter(in: Int): Boolean = {
        in > limit
      }
    }).withParameters(config)
  }
}
