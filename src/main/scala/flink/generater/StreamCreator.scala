package flink.generater

import java.io.Serializable

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object StreamCreator {

  def source[T](seq: Seq[T], interval: Long): Iterator[T] = {
    new StreamSource[T](seq, interval)
  }

  // 왜 안되니..
//  def of[T](env: StreamExecutionEnvironment, seq: Seq[T], interval: Long): DataStream[T] = {
//    env.fromCollection[T](new StreamSource[T](seq, interval))
//  }

  class StreamSource[T](seq: Seq[T], interval: Long) extends Iterator[T] with Serializable {
    var idx = 0
    override def hasNext: Boolean = true
    override def next(): T = {
      idx += 1
      if (idx >= seq.size) {
        idx = 0
      }
      Thread.sleep(interval)
      seq(idx)
    }
  }
}
