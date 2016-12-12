package flink.streaming

import org.apache.flink.streaming.api.scala._

object ExampleAPI {
  case class WordCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // create Stream Samples
    val streamWord = env.fromElements("A","B","C")
    val streamLine = env.fromElements("Hello World", "Hello Flink", "Apache Flink", "Streaming")
    val streamNumber = env.fromElements(1,2,3,4,5)
    val streamTuple = env.fromElements((1, "A"),(2, "B"),(1, "C"))
    val streamWC = env.fromElements(WordCount("H",1), WordCount("A",4), WordCount("H", 5))

    // map: 하나의 element를 받아서 하나의 element로 반환. 타입 변경이 가능
    streamNumber.map(x => x * 2) // (1,2,3,4,5) -> (2,4,6,8,10)

    // flatMap: 하나의 element를 받아 동일 타입의 여러 element로 만듭
    streamLine.flatMap(_.split(" ")) // ("Hello World","Hello Flink") -> ("Hello","World","Hello","Flink")

    // filter: 필터
    streamNumber.filter(x => x < 3) // (1,2,3,4,5) -> (1,2)

    // keyBy (DataStream -> KeyedStream): key를 지정해 KeyedStream으로 변경. 단 Tuple, case class 등의 타입이여야만 함.
    val kstreamTuple = streamTuple.keyBy(0)  // key by number of Field
    val kstreamWC = streamWC.keyBy("word") // key by name

    // reduce (KeyedStream -> DataStream): 같은 키 기준으로 previous 와 next 엘리먼트를 컴바인.
    kstreamTuple.reduce((x,y) => (x._1, x._2 + y._2))  // ((1, "A"),(2, "B"),(1, "C")) -> ((1, "AC"),(2, "B"))
    kstreamWC.reduce((x,y) => WordCount(x.word, x.count + y.count)) // (WordCount("H",1), WordCount("A",4), WordCount("H", 5)) -> (WordCount("H",6), WordCount("A",4))

    // fold (KeyedStream -> DataStream): 초기값을 가지고 reduce 를 실행 것과 동일
    kstreamTuple.fold[String]("Hello ")((str, x) => str + x._2) // ((1, "A"),(2, "B"),(1, "C")) -> ("Hello A","Hello B","Hello AC")

    // aggregations (KeyedStream -> DataStream) : sum, max, min, minBy, maxBy
    kstreamTuple.sum(0)
    kstreamWC.max("count")
    // TODO: keyedStream이 아닌 일반 stream에서 되는지, max와 maxBy 차이점


    // project: Select 문과 유사. 일정 컬럼만 필터링


    env.execute("Example APIs")
  }

}
