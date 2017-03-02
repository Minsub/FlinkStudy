package flink.streaming.basic.partitioning

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object Partitioning1 {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(3)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 10), 500))
    val evenStream = stream.map(v => if (v % 2 == 0 ) ("even",v) else ("odd",v))

    // #1. partitionCustom: key를 통해 partion을 custom하기 지정 가능
    stream
      .partitionCustom(new Partitioner[Int] {
        override def partition(key: Int, numPartitions: Int): Int = key % numPartitions
      }, v => v)
      //.print()

    // #2. shuffle: random으로 partitioning함
    stream.shuffle

    // #3. rebalance: performance 에 따라 최적화된 partitioning (round-robin)
    stream.rebalance

    // #4. rescale
    stream.rescale

    // #5. broadcast: 모든 partition으로 데이터를 전달
    stream.broadcast.print()

    env.execute("Example partitioning 1")
  }
}
