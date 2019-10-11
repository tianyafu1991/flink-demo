package com.tianyafu.flink_redis

import java.util.Properties

import com.tianyafu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object FlinkRedisApi {

  def main(args: Array[String]): Unit = {
    // env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //source
    val sourceProperties = new Properties()
    sourceProperties.setProperty("bootstrap.servers", "master:9092,slave01:9092,slave02:9092")
    sourceProperties.setProperty("group.id", "kafka-consumer-group")
    sourceProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    sourceProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    sourceProperties.setProperty("auto.offset.reset", "latest")
    val sourceTopic = "flink_source_test"

    val sourceStream = env.addSource(new FlinkKafkaConsumer[String](sourceTopic, new SimpleStringSchema(),sourceProperties))

    //tranformation

    val sensorReadingStream = sourceStream.map(_.split(",")).map(data =>SensorReading(data(0).trim,data(1).trim.toLong,data(2).trim.toDouble))

    //sink
    val conf = new FlinkJedisPoolConfig.Builder().setHost("master").setPort(6379).build()
    sensorReadingStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))
    //execute
    env.execute()
  }

}

class MyRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    //定义保存到redis的命令 hset sensor_temp sensor_id temperature
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }

  override def getKeyFromData(t: SensorReading): String = t.id

  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
