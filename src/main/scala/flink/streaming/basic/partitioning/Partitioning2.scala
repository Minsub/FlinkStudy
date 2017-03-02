package flink.streaming.basic.partitioning

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object Partitioning2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(2)

    // #1. startNewChain

    // #2. disableChaining

    // #3. slotSharingGroup

    env.execute("Example partitioning 2")
  }
}
