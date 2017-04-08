package flink

import org.apache.flink.streaming.api.scala._

object Test2 {
  val PATH = "src/main/resources/log.txt"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // File로 부터 text 를 읽음
    val inputText = env.readTextFile(PATH)

    inputText.print()

    env.execute("example-readfile")
  }

}
