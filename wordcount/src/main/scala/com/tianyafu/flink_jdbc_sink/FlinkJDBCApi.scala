package com.tianyafu.flink_jdbc_sink

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.tianyafu.apitest.SensorReading
import javax.sql.DataSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object FlinkJDBCApi {

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
    val tupleStream = sourceStream.map(data=>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
    //sink
    val jdbcSink = new MyJdbcSink("insert into sensor_reading(sensor_id,sensor_timestamp,sensor_temperature) values(?,?,?)")
    tupleStream.map(data=>Array(data.id,data.timestamp,data.temperature)).addSink(jdbcSink)

    //execute
    env.execute()

  }

}

class MyJdbcSink(sql:String ) extends  RichSinkFunction[Array[Any]] {

  val driver="com.mysql.jdbc.Driver"

  val url="jdbc:mysql://master:3306/test?useSSL=false"

  val username="root"

  val password="root"

  val maxActive="20"

  var connection:Connection=null;

  //创建连接
  override def open(parameters: Configuration): Unit = {
    val properties = new Properties()
    properties.put("driverClassName",driver)
    properties.put("url",url)
    properties.put("username",username)
    properties.put("password",password)
    properties.put("maxActive",maxActive)


    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
    connection = dataSource.getConnection()
  }

  //反复调用连接，执行sql
  override def invoke(values: Array[Any]): Unit = {
    // 预编译器
    val ps: PreparedStatement = connection.prepareStatement(sql )
    println(values.mkString(","))
    for (i <- 0 until values.length) {
      // 坐标从1开始
      ps.setObject(i + 1, values(i))
    }
    // 执行操作
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if(connection!=null){
      connection.close()
    }
  }
}
