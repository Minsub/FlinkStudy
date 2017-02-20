package flink.streaming.basic.datasource

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

object DataSourceCustom {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val stream = env.addSource(new CustomContinueSource(1, 10))

    stream.print()

    env.execute("Example DataSource from custom source")
  }

  class CustomContinueSource(from:Int, to:Int) extends RichSourceFunction[Int] {
    val list = List.range(from, to)

    override def cancel(): Unit = ???

    override def run(ctx: SourceContext[Int]): Unit = {
      while(true) {
        for (i <- list) {
          Thread.sleep(1000)
          ctx.collect(i)
        }
      }
    }
  }
}
