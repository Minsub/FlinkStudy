package flink.batch

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._


object ExampleAPI {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val dataText = env.fromElements("Hello","World","Flink")
    val dataNumber = env.fromCollection(List.range(1, 10) :::  List.range(5,15))
    val dataChar = env.fromCollection(List("H","W","F"))

    val left = env.fromCollection(List("1","2","3")).map((_, "Left"))
    val right = env.fromCollection(List("2","3","4")).map((_, "Right"))

    // map
    val mapData = dataText.map("Hi "+_) // Hi를 붙히기

    // flatMap
    val flatMapData = mapData.flatMap(_.toCharArray) // 단어별로 자름

    // filter
    val filterData = flatMapData.filter(_.toString.trim != "").map(_.toString) // 빈문자 제거

    // mapPartition
    val mapPartitionData = filterData.mapPartition(_.map((_, 1)))  // tuple을 만들기

    // reduce
    val reduceData = filterData.reduce(_+_)  // 다시 char를 붙히기

    // reduceGroup
    val reduceGroupData = dataNumber.reduceGroup(_.sum)  // 1~9의 합

    // Aggregate
    val aggregateData = filterData.map((_, 1)).aggregate(Aggregations.SUM, 1).aggregate(Aggregations.MAX, 0)

    // distinct
    val distinctData = filterData.distinct()

    // join
    val joinData = filterData.map((_,1)).join(dataChar.map((_, 2))).where(_._1).equalTo(_._1)

    // outer join
    val outerJoinData = filterData.map((_, 1)).leftOuterJoin(dataChar.map((_, 2)))
          .where(_._1).equalTo(_._1)
          .apply((left, right) => (left._1, left._2, if(right==null) 0 else right._2))

    // coGroup
    val coGroupData = left.coGroup(right).where(_._1).equalTo(_._1)

    // cross
    val crossData = left.cross(right)

    // union
    val union = left.union(right)

    // rebalacne
    val rebalacneData = left.rebalance().map(x => (x._1, x._2 * 2))

    // hash-Partition
    left.partitionByHash(0)

    // range-Partition
    left.partitionByRange(0)

    // custom-Partition
    left.partitionCustom(new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = numPartitions
    }, 0)

    // sort-Partition
    left.sortPartition(0, Order.ASCENDING)

    // first-n
    val result1 = left.first(2)
    val result2 = left.groupBy(0).first(1)
    val result3 = left.groupBy(0).sortGroup(1, Order.ASCENDING).first(1)

    // max & maxBy
    dataNumber.map(x =>(x, x * 2)).max(0)
    val maxData = dataNumber.map(x =>(x, x * 2)).groupBy(0).sum(1)
    val maxByData = dataNumber.map(x =>(x, x * 2)).maxBy(0, 1)


  }
}
