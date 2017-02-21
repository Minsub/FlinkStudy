package flink.streaming.basic.transformation

import flink.streaming.basic.datasource.StreamCreator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object Trasnformation1 {

  case class WC(word:String, count:Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.fromCollection(StreamCreator.source(List.range(1, 10), 500))

    /* Stream -> Stream */
    // #1. Map
    val streamMap = stream.map(_ * 2).map(_ + " Hello")

    // #2. Flat Map
    val streamFlatMap = stream.flatMap(i => Array(i, i+1))

    // #3. Filter
    val streamFilter = stream.filter(_ < 10)


    /* Stream -> KeyedStream */
    val streamTuple = stream.map(i => (i, s"data$i"))
    val streamWC = stream.map(i => WC(s"word$i", i))

    // #4 keyBy
    val streamKeyBy = streamTuple.keyBy(0)
    val streamKeyBy2 = streamWC.keyBy("word")

    // #5 Reduce
    val streamReduce = stream.map((_,1)).keyBy(0).reduce((a,b) => (a._1, a._2+b._2))

    // #6 Fold
    val streamFold = stream.keyBy(i => i).fold("start")((str, i) => (str + i))


    /* keyedStream -> Stream */
    // #7 Aggregations
    val streamSum = stream.map(i => (s"key$i", i)).keyBy(0).sum(1)
    val streamMax = stream.map(i => (s"key$i", i)).keyBy(0).max(1)

    env.execute("Example transformation 1")
  }

}
