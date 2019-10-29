package com.tianyafu.course05

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala._

object DataStreamingDataSourceApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

        fromSocket(env)
    //    nonParallelSourceFunction(env)
    //    parallelSourceFunction(env)
    //    richParallelSourceFunction(env)
//    unionFunction(env)
//    splitSelectFunction(env)
    env.execute()
  }

  def splitSelectFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomerNonParallelSourceFunction).setParallelism(1)
    val splitStream = data.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("odd")
        }
        list
      }
    })

    splitStream.select("odd").print()

  }


  def unionFunction(env: StreamExecutionEnvironment) = {
    val data1 = env.addSource(new CustomerNonParallelSourceFunction).setParallelism(1)
    val data2 = env.addSource(new CustomerNonParallelSourceFunction).setParallelism(1)

    val unionStream = data1.union(data2)
    unionStream.print()
  }

  def richParallelSourceFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomerRichParallelSourceFunction).setParallelism(3)
    data.print()
  }

  def parallelSourceFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomerParallelSourceFunction).setParallelism(3)
    data.print().setParallelism(3)
  }

  def nonParallelSourceFunction(env: StreamExecutionEnvironment) = {
    val nonParallelSourceFunction =new CustomerNonParallelSourceFunction
    val data = env.addSource(nonParallelSourceFunction).setParallelism(1)
    data.print().setParallelism(1)
  }



  def fromSocket(env:StreamExecutionEnvironment): Unit ={
      val dataSource = env.socketTextStream("192.168.101.217",9999)
    dataSource.print().setParallelism(1)
  }
}
