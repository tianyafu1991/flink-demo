package com.tianyafu.course04

import org.apache.flink.api.scala._

object DataSetDataSourceApp {

  def main(args: Array[String]): Unit = {
    val env  = ExecutionEnvironment.getExecutionEnvironment
//    fromCollection(env)
    fromTextFile(env)

  }


  def fromCollection(env:ExecutionEnvironment): Unit ={
    val data = 1 to 10
    env.fromCollection(data).print()
  }

  /**
    * 可以读指定的文件 或者是指定的文件夹
    * @param env
    */
  def fromTextFile(env:ExecutionEnvironment): Unit={
//    val filePath = "E:\\WorkSpace\\IDEAWorkspace\\flinkdemo\\imooc-flink-train\\src\\main\\resource\\hello.txt"
    val filePath = "E:\\WorkSpace\\IDEAWorkspace\\flinkdemo\\imooc-flink-train\\src\\main\\resource"
    env.readTextFile(filePath).print()
  }

}
