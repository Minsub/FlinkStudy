package flink.batch

import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("src/main/resources/README_FLINK.md")
    val count = text
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    count.print()
  }
}
