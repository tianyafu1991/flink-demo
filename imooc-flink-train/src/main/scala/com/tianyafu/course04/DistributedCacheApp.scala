package com.tianyafu.course04

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._

object DistributedCacheApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\hello.txt"
    //step1. 注册分布式缓存文件/目录  可以是本地的 也可以是HDFS的
    env.registerCachedFile(filePath,"distributeCache-scala")

    val list = List("spark","flink","hadoop","elasticsearch","hbase")

    val dataSource = env.fromCollection(list)

    val mapOperator = dataSource.map(new RichMapFunction[String, String] {

      override def open(parameters: Configuration): Unit = {
        val file = getRuntimeContext.getDistributedCache.getFile("distributeCache-scala")
        val lines = FileUtils.readLines(file)

        for (ele <- lines.asScala) {
          print(ele)
        }
      }

      override def map(in: String): String = {
        in
      }
    })
    mapOperator.print()


  }

}
