package flink.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.TableEnvironment


object ExampleAPI {
  case class WC(word:String, count: Int)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataWC = env.fromElements("Hello World Flink Tables again Hello")
                      .flatMap(_.split(" ")).map(WC(_, 1))

    val resultWC = dataWC.toTable(tableEnv)
        .groupBy('word)
        .select("word, count.sum as count")
      .toDataSet[WC]



//    val dataTuple = env.fromElements(List(1,2,3,4,5)).map((_, "A"))
//    val resultTuple = dataTuple.toTable(tableEnv,'num, 'text).where("num < 4").select("num, text").toDataSet[(Int, String)]
//
//    resultTuple.print()

  }
}
