package flink.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object ExampleSQL {
  case class WC(word:String, count: Int)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val inputTuple = env.fromCollection(List(("A",1,10),("B",2,20),("A",3,100),("C",4,500)))
    tableEnv.registerTable("tuples", inputTuple.toTable(tableEnv, 'a, 'b, 'c))

    // SQL
    val dataSum = tableEnv.sql("SELECT SUM(c) FROM tuples WHERE a='A' ").toDataSet[Int]

    dataSum.print()
  }
}
