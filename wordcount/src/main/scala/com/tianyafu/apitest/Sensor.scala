package com.tianyafu.apitest

import org.apache.flink.streaming.api.scala._

//定义一个数据样例类，传感器id，采集时间戳，传感器温度
case class SensorReading(id:String,timestamp:Long,temperature:Double)

object Sensor {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.source
    // source1 ： 从集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1",1547718199,35.80018327300259),
      SensorReading("sensor_6",1547718201,15.402984393403084),
      SensorReading("sensor_7",1547718202,6.720945201171228),
      SensorReading("sensor_10",1547718205,38.101067604893444)
    ))

    //source2 : 从文件中读取数据
    val inputPath = "E:\\WorkSpace\\IDEAWorkspace\\flinkdemo\\wordcount\\src\\main\\resource\\sensor.txt"
    val stream2 = env.readTextFile(inputPath)
    //tranformation


    //sink
    stream1.print("stream1").setParallelism(1)
    stream2.print("stream2").setParallelism(1)

    env.execute("api test")

  }

}
