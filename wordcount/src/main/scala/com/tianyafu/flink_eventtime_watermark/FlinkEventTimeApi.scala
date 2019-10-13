package com.tianyafu.flink_eventtime_watermark

import java.util.Properties

import com.tianyafu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object FlinkEventTimeApi {

  def main(args: Array[String]): Unit = {
    //env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //按照事件时间来处理
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //source
    val kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.setProperty("bootstrap.servers", "master:9092,slave01:9092,slave02:9092")
    kafkaConsumerProperties.setProperty("group.id", "kafka-consumer-group")
    kafkaConsumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaConsumerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaConsumerProperties.setProperty("auto.offset.reset", "latest")
    val sourceTopic = "flink_source_test"
    val sourceStream = env.addSource(new FlinkKafkaConsumer[String](sourceTopic,new SimpleStringSchema(),kafkaConsumerProperties))

    //tranformation
    val dataStream = sourceStream.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
/*      //升序数据分配时间戳 即有序数据
        .assignAscendingTimestamps(_.timestamp * 1000)*/
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000  //这里要的是毫秒数 所以乘以1000
    })

    //统计每个传感器每15秒内最小温度，每隔5秒滑动一个窗口
    val minTemperaturePerWindow = dataStream
      .map(data =>(data.id,data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15),Time.seconds(5))
      .min(1)

    //sink
    minTemperaturePerWindow.print().setParallelism(1)

    //execute
    env.execute()
  }

}
