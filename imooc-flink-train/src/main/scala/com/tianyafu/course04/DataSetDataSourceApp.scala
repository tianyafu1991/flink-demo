package com.tianyafu.course04

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object DataSetDataSourceApp {

  def main(args: Array[String]): Unit = {
    val env  = ExecutionEnvironment.getExecutionEnvironment
//    fromCollection(env)
//    fromTextFile(env)
//    fromCsvFile(env)
//    fromRecursiveFile(env)
    fromCompressionFiles(env)
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

  def fromCsvFile(env:ExecutionEnvironment): Unit ={
    val filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\user.csv"
//    env.readCsvFile[(String,String,String)](filePath,ignoreFirstLine=true,includedFields = Array(1,2,3)).print()
    env.readCsvFile[MyUser](filePath,ignoreFirstLine = true,includedFields = Array(1,2,3)).print()
  }


  def fromRecursiveFile(env:ExecutionEnvironment): Unit ={
    val filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource"
    // create a configuration object
    val parameters = new Configuration()
    // set the recursive enumeration parameter
    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(filePath).withParameters(parameters).print()
  }


  def fromCompressionFiles(env:ExecutionEnvironment): Unit ={
    val filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\userCsv.gz"
    env.readTextFile(filePath).print()
  }

}

case class MyUser(name:String,sex:String,school:String)
