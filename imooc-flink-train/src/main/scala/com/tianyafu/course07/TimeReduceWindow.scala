package com.tianyafu.course07

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeReduceWindow {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataSource = env.socketTextStream("master",9999)

    dataSource.flatMap(_.split(","))
      .map(x=>(1,x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce((x,y)=>{
        println(x+"......."+y)
        (x._1,x._2+y._2)
      })
      .print().setParallelism(1)


    env.execute("TimeReduceWindow")
  }

}
