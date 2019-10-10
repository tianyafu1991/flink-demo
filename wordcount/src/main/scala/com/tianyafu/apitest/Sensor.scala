package com.tianyafu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer



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
    val inputPath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\wordcount\\src\\main\\resource\\sensor.txt"
    val stream2 = env.readTextFile(inputPath)

    //source3 ；从kafka中读取数据源
    val properties  = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092,slave01:9092,slave02:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")

    val stream3 = env.addSource(new FlinkKafkaConsumer[String]("test",new SimpleStringSchema(),properties))

    //tranformation

    val dataStream = stream2.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    }).keyBy("id").sum("temperature")

    val dataStream2 = stream2.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    }).keyBy("id").reduce((x,y) =>SensorReading(x.id,x.timestamp+y.timestamp,x.temperature+y.temperature))


    //sink
    stream1.print("stream1").setParallelism(1)
    stream2.print("stream2").setParallelism(1)
    stream3.print("stream3").setParallelism(1)
    dataStream.print("dataStream").setParallelism(1)
    dataStream2.print("dataStream2").setParallelism(1)

    env.execute("api test")

  }

}

//定义一个数据样例类，传感器id，采集时间戳，传感器温度
case class SensorReading(id:String,timestamp:Long,temperature:Double)
