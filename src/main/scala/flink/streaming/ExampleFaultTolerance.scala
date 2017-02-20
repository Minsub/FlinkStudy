package flink.streaming

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ExampleFaultTolerance {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(2)

    // start a checkpoint every 1000 ms
    env.enableCheckpointing(1000)

    // advanced options:
    // set mode to exactly-once (this is the default)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // checkpoints have to complete within one minute, or are discarded
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)



    val streamNumber = env.fromCollection[Int](StreamCreator.source[Int](List(100,200,300,400,500), 400))

    streamNumber.map((_,1)).keyBy(0).timeWindow(Time.seconds(5)).sum(0)

    streamNumber.map((1,_)).keyBy(0).timeWindow(Time.seconds(3)).sum(1).map(_._2).print()

    env.execute("Example FaultTolerance")
  }

}
