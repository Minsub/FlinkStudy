package flink.streaming.eventtime

import java.text.SimpleDateFormat
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.io.Source

object GeneratingTimeStampAndWartermarks {
  val PATH = "src/main/resources/myEvent.csv"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val input = env.readFile(new MyFileInputFormat(PATH), PATH)
    input
      .assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks)
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply((time: TimeWindow, input: Iterable[MyEvent], out: Collector[String]) => {
        var count = 0L
        val sb = new StringBuilder
        for (in <- input) {
          count += 1
          sb.append("-" + in.index)
        }
        out.collect(s"Window Count: $count -> ${sb.toString()}")
      })
      .print()

    env.execute("Example event time 1")
  }

  case class MyEvent(index: Long, msg:String, timestamp: String, timestamp_ms: Long)

  class MyFileInputFormat(path: String) extends FileInputFormat[MyEvent] {
    val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")

    val lines = Source.fromFile(path).getLines().toList
    var count = 0
    val maxCount = lines.length

    override def nextRecord(reuse: MyEvent): MyEvent = {
      val columns = lines(count).split(",")
      val record = MyEvent(columns(0).toLong, columns(1), columns(2), inputFormat.parse(columns(2)).getTime)
      count += 1
      record
    }
    override def reachedEnd(): Boolean = count >= maxCount
  }

  class MyAssignerWithPeriodicWatermarks extends AssignerWithPeriodicWatermarks[MyEvent] {
    val maxTimeLag  = 3500L; // 3.5 seconds
    override def extractTimestamp(element: MyEvent, previousElementTimestamp: Long): Long = element.timestamp_ms
    override def getCurrentWatermark: Watermark = new Watermark(System.currentTimeMillis() - maxTimeLag )
  }
}
