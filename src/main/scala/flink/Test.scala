package flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Test {

  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val text = env.socketTextStream("localhost", 9999)

//    env.execute("Test job print")

    val list = ttt[String](List("A"))
    println(list)

    val s = List(1,2,3,4)
    print(s.map(_.toString).reduce(_+_))
  }

  def ttt[T](list: Seq[T]): List[T] = {
    val t = new Test[T](list)
    List(t.p())
  }

  class Test[T](list: Seq[T]) {
    def p(): T = {
      list(0)
    }
  }

}
