package com.tianyafu.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object DataSetTranformationApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    firstFunction(env)

  }

  def firstFunction(env:ExecutionEnvironment): Unit ={
    val data = ListBuffer[(Int,String)]()
    data.append((1,"Flink"))
    data.append((1,"hadoop"))
    data.append((1,"spark"))
    data.append((2,"java"))
    data.append((2,"springboot"))
    data.append((3,"linux"))
    data.append((4,"vue"))

    val dataSource = env.fromCollection(data)
//    dataSource.first(3).print()
//    dataSource.groupBy(0).first(2).print()
    dataSource.groupBy(0).sortGroup(1,Order.ASCENDING).first(2).print()
  }

}
