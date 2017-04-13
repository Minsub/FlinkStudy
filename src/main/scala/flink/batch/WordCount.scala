package flink.batch

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object WordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // read from file
    val text = env.readTextFile("src/main/resources/README_FLINK.md")
    val count = text
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .filter(_._2 > 10)

    // write to file
    count.writeAsText("src/main/resources/wordcount.txt", WriteMode.OVERWRITE);

    env.execute("flink batch example")
  }
}
