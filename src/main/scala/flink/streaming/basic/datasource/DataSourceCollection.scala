package flink.streaming.basic.datasource

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._

object DataSourceCollection {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    // stream once from seq
    val streamOnce = env.fromCollection[Int](List(1,2,3,4,5))

    // continuous stream from iterator
    val streamContinuous = env.fromCollection(StreamCreator.source(List.range(1, 10), 1000))

    streamContinuous.print()

    env.execute("Example DataSource from collection")
  }

}
