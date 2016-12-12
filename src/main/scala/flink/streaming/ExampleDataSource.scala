package flink.streaming

import java.util

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.streaming.api.functions.source.{FilePathFilter, FileProcessingMode, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.SplittableIterator

object ExampleDataSource {
  case class WordCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // Custom Input Datasource:
//    env.addSource(SourceFunction)  // None-Parallel
//    env.addSource(ParallelSourceFuntion)  //Parallel


    // readTextFile
    val streamFile = env.readTextFile("src/main/resources/log.txt")
    val wordCount = streamFile.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    // readFile
    //env.readFile(FileInputFormat, "path...", FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, FilePathFilter.createDefaultFilter())

    // fromCollection
    env.fromCollection(List(1,2,3,4,5))

    // fromElements
    env.fromElements("A","B","C")

    // fromParallelCollection
    //env.fromParallelCollection(SplittableIterator)



    // Data Sunks
    // writeAsText
//    wordCount.writeAsText("src/main/resources/out/wordCount")

    // writeAsCsv
//    wordCount.writeAsCsv("src/main/resources/out/wordCount")

    // writeUsingOutputFormat

    // writeToSocket

    // addSink


    env.execute("Example DataSource")
  }

}
