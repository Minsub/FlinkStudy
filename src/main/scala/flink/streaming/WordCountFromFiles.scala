package flink.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCountFromFiles {

  case class WordCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.readFileStream("src/main/resources")

    val count = text
      .flatMap(_.split(" "))
      .map(WordCount(_, 1))
      .keyBy("word")
      .sum("count")

    count.print().setParallelism(1)

    env.execute("File WordCount")
  }

}
