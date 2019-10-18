package com.tianyafu.wordcount

import org.apache.flink.api.scala._

/**
  * 读取文件格式的Wordcount
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    //source 读取数据
    val inputPath= "F:\\tianyafu\\tianyafu_github\\flink-demo\\wordcount\\src\\main\\resource\\hello.txt"
    val inputDS = env.readTextFile(inputPath)
    //transformation 转换
    val wordCountDS = inputDS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)


    //sink 输出
    wordCountDS.print()
  }

}
