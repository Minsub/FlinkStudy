package flink.streaming.basic.datasource

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._

object DataSourceFromFile {
  val PATH = "src/main/resources/log.txt"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(2)

    // File로 부터 text 를 읽음
    val inputText = env.readTextFile(PATH)
    val inputText2 = env.readFile(new TextInputFormat(new Path(PATH)), PATH)

    // 1000 interval 마다 파일이 변화가 생길 때 마다 모두 읽음
    val inputTextWatcher = env.readFile(new TextInputFormat(new Path(PATH)), PATH
            , FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)

    inputTextWatcher.print()

    env.execute("Example DataSource from file")
  }

}
