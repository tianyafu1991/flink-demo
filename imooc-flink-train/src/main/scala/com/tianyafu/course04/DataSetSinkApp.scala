package com.tianyafu.course04

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object DataSetSinkApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = 1 to 10
    //如果并行度为1 这后面的路径中 sink为文件  如果并行度大于1 则sink为目录
    val datasSource = env.fromCollection(data).setParallelism(2)

    val filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\sink"
    datasSource.writeAsText(filePath,WriteMode.OVERWRITE)

    env.execute()
  }

}
