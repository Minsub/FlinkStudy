package flink.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._


object ExampleOperatorAPI {
  case class WC(word:String, count: Int)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.fromElements("Hello World Flink Tables again Hello").flatMap(_.split(" ")).map(WC(_, 1))
    val inputNumbers = env.fromCollection(List.range(1, 10))
    val inputTuple = env.fromCollection(List(("A",1,10),("B",2,20),("A",3,100),("C",4,500)))
    val inputTuple2 = env.fromCollection(List(("A","Apple"),("B","Banana")))

    // table로 변경
    val tableWC = input.toTable(tableEnv)

    // table을 등록
    tableEnv.registerTable("words", tableWC)
    tableEnv.registerTable("numbers", inputNumbers.toTable(tableEnv, 'num))
    tableEnv.registerTable("tuples", inputTuple.toTable(tableEnv, 'a, 'b, 'c))
    tableEnv.registerTable("codes", inputTuple2.toTable(tableEnv, 'code, 'name))


    // registerTable된 table을 이름으로 가져옴
    val dataWC = tableEnv.scan("words").groupBy("word").select("word, count.sum as count").toDataSet[WC]


    // select 에서 toTable에서 선산한 alias로 컨트롤 가능
    val dataSelect = tableEnv.scan("tuples").select("a, b as dd").toDataSet[(String, Int)]

    // alias를 as 로 지정가능
    val dataAs = tableEnv.scan("tuples").as('a1, 'a2, 'a3).select("a2, a3").toDataSet[(Int, Int)]

    // filter & where
    val dataFilterAndWhere = tableEnv.scan("tuples").where('a === "A").filter('b >= 3).toDataSet[(String, Int, Int)]

    // group by
    val dataGroupBy = tableEnv.scan("tuples").groupBy('a).select('a, 'b.sum as 'b, 'c.sum as 'c).toDataSet[(String, Int, Int)]


    val left = tableEnv.scan("tuples")
    val right = tableEnv.scan("codes")

    // join
    val dataJoin = left.join(right).where('a === 'code).select("a, b, c, name").toDataSet[(String, Int, Int, String)]

    // left outer join, right outer join, full outer join
    val dataLeftOuterJoin = left.leftOuterJoin(right, 'a === 'code)
          .where("name.isNotNull")
          .select("a, b, c, name")
          .toDataSet[(String, Int, Int, String)]

    // union, unionAll, Intersect, IntersectAll, Minus, MinusAll
    val dataUnion = left.select("a as a1, a as a2").union(right).toDataSet[(String, String)]

    // distinct
    val dataDistinct = left.select('a).distinct().toDataSet[String]

    // order by
    val dataOrderBy = left.orderBy("a.desc, c.desc").toDataSet[(String, Int, Int)]

  }
}
