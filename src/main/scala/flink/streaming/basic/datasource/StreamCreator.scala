package flink.streaming.basic.datasource

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.Date


object StreamCreator {

  def source[T](seq: Seq[T], interval: Long): Iterator[T] = {
    new StreamSource[T](seq, interval)
  }

  def sourceWithTimestamp[T](seq: Seq[T], interval: Long): Iterator[(T, String, Long)] = {
    new StreamSourceTimestamp[T](seq, interval)
  }

  class StreamSource[T](seq: Seq[T], interval: Long) extends Iterator[T] with Serializable {
    var idx = 0
    override def hasNext: Boolean = true
    override def next(): T = {
      idx += 1
      if (idx > seq.size) {
        idx = 1
        Thread.sleep(interval * 2)
      }
      Thread.sleep(interval)
      seq(idx - 1)
    }
  }

  class StreamSourceTimestamp[T](seq: Seq[T], interval: Long) extends Iterator[(T, String, Long)] with Serializable {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")
    var idx = 0
    override def hasNext: Boolean = true
    override def next(): (T, String, Long) = {
      idx += 1
      if (idx > seq.size) {
        idx = 1
        Thread.sleep(interval * 2)
      }
      Thread.sleep(interval)
      val date = new Date()
      (seq(idx - 1), dateFormatter.format(date), date.getTime)
    }
  }
}
