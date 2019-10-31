package com.tianyafu.course08

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink

/**
  * 这里没有调试通
  */
object FlinkHdfsSinkApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataSource = env.socketTextStream("192.168.101.217",9999)

    val hdfsPath ="hdfs://dse:8022/tmp/tianyafu/FlinkHdfs_tianya.txt"

    /**
      * This will create a streaming sink that creates hourly buckets and uses a default rolling policy.
      * The default bucket assigner is DateTimeBucketAssigner and the default rolling policy is DefaultRollingPolicy
      *
      *
      *
      * This policy rolls a part file if:
      *
      * there is no open part file,
      * the current file has reached the maximum bucket size (by default 128MB),
      * the current file is older than the roll over interval (by default 60 sec), or
      * the current file has not been written to for more than the allowed inactivityTime (by default 60 sec).
      */
    /*dataSource.addSink(StreamingFileSink
      .forRowFormat(new Path(hdfsPath), new SimpleStringEncoder[String]("UTF-8"))
      .build())*/
    dataSource.addSink(new BucketingSink[String](hdfsPath))




    env.execute("FlinkHdfsSinkApp")
  }

}
