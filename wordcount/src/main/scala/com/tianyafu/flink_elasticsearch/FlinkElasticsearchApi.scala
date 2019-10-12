package com.tianyafu.flink_elasticsearch

import java.util
import java.util.Properties

import com.tianyafu.apitest.SensorReading
import com.tianyafu.flink_kafka.WordCountObject
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * Flink 整合 Elasticsearch
  * see https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/elasticsearch.html
  */
object FlinkElasticsearchApi {

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
    val httpHosts = new java.util.ArrayList[HttpHost]
//    httpHosts.add(new HttpHost("master", 9200,"http"))
    httpHosts.add(new HttpHost("master", 9200))

    /*
    //可以成功写入
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        def createIndexRequest(t: SensorReading): IndexRequest = {
          val hashMap = new java.util.HashMap[String, String]
          hashMap.put("sensor_id",t.id)
          hashMap.put("timestamp",t.timestamp.toString)
          hashMap.put("temperature",t.temperature.toString)

          return Requests.indexRequest()
            .index("sensor")
            .`type`("_doc")
            .source(hashMap)
        }

        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: "+t)
          val indexRequest = createIndexRequest(t)
          requestIndexer.add(indexRequest)
          println("save success")
        }
      }
    )*/


    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: "+t)
          //要往es中写数据  数据必须是jsonObject或者是map的形式
          val hashMap = new util.HashMap[String,String]()
          hashMap.put("sensor_id",t.id)
          hashMap.put("timestamp",t.timestamp.toString)
          hashMap.put("temperature",t.temperature.toString)
          //创建 index request
          val indexRequest = Requests.indexRequest().index("sensor").`type`("_doc").source(hashMap)
          requestIndexer.add(indexRequest)
          println("save success")
        }
      }
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)

    // provide a RestClientFactory for custom configuration on the internally created REST client
    /*esSinkBuilder.setRestClientFactory(
      restClientBuilder => {
        restClientBuilder.setDefaultHeaders(...)
        restClientBuilder.setMaxRetryTimeoutMillis(...)
        restClientBuilder.setPathPrefix(...)
        restClientBuilder.setHttpClientConfigCallback(...)
      }
    )*/

    // finally, build and add the sink to the job's pipeline
    tupleStream.addSink(esSinkBuilder.build)


    //executer

    env.execute()
  }

}
