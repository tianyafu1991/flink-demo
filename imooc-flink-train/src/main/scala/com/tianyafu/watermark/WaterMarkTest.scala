package com.tianyafu.watermark

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 本demo按照以下链接中给定的demo来测试watermark的原理
  * 详见 https://blog.csdn.net/lmalds/article/details/52704170
  *
  *
  * 000001,1461756862000
  * 000001,1461756866000
  * 000001,1461756872000
  * 000001,1461756873000
  * 000001,1461756870000
  * 000001,1461756874000
  *
  */
object WaterMarkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataSource = env.socketTextStream("master",9999)


    val watermark = dataSource.map(x => {
      val strs = x.split(",")
      val code = strs(0)
      val time = strs(1).toLong
      (code, time)
    }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {

      var currentMaxTimestamp = 0L

      val maxOutOfOrderness = 10000L

      var watermark: Watermark = null

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        watermark
      }

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println(
          "timestamp:" + element._1 + "," + element._2 + "|" + format.format(element._2) + "," + currentMaxTimestamp + "|" + format.format(currentMaxTimestamp) + "," + watermark.toString
        )
        timestamp
      }
    })

    watermark.keyBy(_._1).timeWindow(Time.seconds(3)).apply(new WindowFunctionTest).print()






    env.execute("WaterMarkTest")

  }

}

class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String,String),String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int, String, String, String, String)]): Unit = {
    val list = input.toList.sortBy(_._2)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))
  }
}