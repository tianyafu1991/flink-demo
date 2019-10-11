package com.tianyafu.flink_kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, FlinkKafkaProducer011}

/**
  * fink 整合 kafka
  * see https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html
  */
object FlinkKafkaApi {

  def main(args: Array[String]): Unit = {
    //获取执行环境
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

    // tranformation

    val tupleStream = sourceStream.flatMap(_.split(" ")).map(WordCountObject(_,1).toString)
    //sink
    val sinkTopic = "flink_sink_test"
    val kafkaProducerProperties = new Properties()
    kafkaProducerProperties.setProperty("bootstrap.servers", "master:9092,slave01:9092,slave02:9092")
    tupleStream.addSink(new FlinkKafkaProducer(sinkTopic,new SimpleStringSchema(),kafkaProducerProperties))

    //执行
    env.execute()
  }

}

case class WordCountObject(word:String,count:Int)
