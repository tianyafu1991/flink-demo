package com.tianyafu.course07

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWindowApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataSource = env.socketTextStream("192.168.101.217",9999)

    dataSource.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1).print()
      .setParallelism(1)




    env.execute()
  }

}
