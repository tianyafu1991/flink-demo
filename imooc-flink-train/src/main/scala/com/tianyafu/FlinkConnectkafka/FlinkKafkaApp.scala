package com.tianyafu.FlinkConnectkafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

/**
  * 本示例代码适用于kafka版本 >=1.0.0
  */
object FlinkKafkaApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //启用checkpoint
    env.enableCheckpointing(4000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val consumeTopic = ""
    val consumeProperties = new Properties()
    consumeProperties.put("bootstrap.servers","")
    consumeProperties.put("group.id","")

    val dataSource = env.addSource(new FlinkKafkaConsumer[String](consumeTopic,new SimpleStringSchema(),consumeProperties))




    val producerTopic = ""
    val producerProperties = new Properties()
//    dataSource.addSink(new FlinkKafkaProducer(producerTopic,new SimpleStringSchema(),producerProperties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))





    env.execute("FlinkKafkaApp")
  }

}
