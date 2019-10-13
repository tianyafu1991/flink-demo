package com.tianyafu.fink_window_api

import java.util.Properties

import com.tianyafu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SlidingCountWindowApi {

  def main(args: Array[String]): Unit = {
    //env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

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
    val dataStream = sourceStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    //统计每个传感器每15秒内的最小温度,每隔5秒滑动一次窗口
    val minTemperaturePerWindow = dataStream.map(data =>(data.id,data.temperature))
      .keyBy(_._1)
      .countWindow(3,2)
      .min(1)

    //sink
    minTemperaturePerWindow.print().setParallelism(1)

    //execute
    env.execute()
  }

}
