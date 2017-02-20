package flink.streaming.basic.datasink

import org.apache.flink.api.java.io.{TextInputFormat, TextOutputFormat}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._

object DataSinkToFile {
  val PATH = "src/main/resources/"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val input = env.fromCollection(List.range(1, 10))
    val inputTuple = env.fromCollection(List(("A",1), ("B",2), ("C", 3)))

    // write to text file
    input.writeAsText(PATH + "sampleText.txt")

    // write to text file using fileOutputFormat
    input.writeUsingOutputFormat(new TextOutputFormat[Int](new Path(PATH + "sampleText2.txt")))

    // write to csv file
    inputTuple.writeAsCsv(PATH + "sampleCsv.csv")

    env.execute("Example dataSink to file")
  }

}
