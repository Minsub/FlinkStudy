package flink.table

import flink.generater.StreamCreator
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.TableEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object ExampleStreamSQL {
  case class WC(word:String, count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.fromCollection(StreamCreator.source(List.range(1, 10), 1000))
    tableEnv.registerDataStream("numbers", input, 'num)

    // SQL
    val streamSql = tableEnv.sql("SELECT STREAM num * 10 FROM numbers ").toDataStream[Int]

    streamSql.print()

    env.execute("Flink Test Table SQL")
  }
}
