package com.tianyafu.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object DataSetTranformationApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
//    firstFunction(env)
//    joinFunction(env)
//    outerJoinFunction(env)
    crossFunction(env)
  }

  /**
    * 笛卡尔积
    * @param env
    */
  def crossFunction(env:ExecutionEnvironment): Unit ={
    val list1 = List("曼联","曼城")
    val list2 = List(3,1,0)

    val data1 = env.fromCollection(list1)
    val data2 = env.fromCollection(list2)

    data1.cross(data2).print()
  }

  /**
    * 外连接
    * @param env
    */
  def outerJoinFunction(env:ExecutionEnvironment): Unit ={
    val info1  = ListBuffer[(Int,String)]()
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"香蕉哥"))
    info1.append((4,"猪头呼"))

    val info2 = ListBuffer[(Int,String)]()
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    //左外连接
   /* data1.leftOuterJoin(data2).where(0).equalTo(0).apply(
      (first,second)=>{
      if(second ==null){
        (first._1,first._2,"-")
      }else{
        (first._1,first._2,second._2)
      }
    }).print()*/

    //右外连接
    /*data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{
      if(first==null){
        (second._1,"-",second._2)
      }else{
        (first._1,first._2,second._2)
      }
    }).print()*/

    //全外连接
    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{
      if(second==null){
        (first._1,first._2,"-")
      }else if (first==null){
        (second._1,"-",second._2)
      }else{
        (first._1,first._2,second._2)
      }

    }).print()
  }

  /**
    * 内连接
    * @param env
    */
  def joinFunction(env:ExecutionEnvironment): Unit ={
    val info1  = ListBuffer[(Int,String)]()
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"香蕉哥"))
    info1.append((4,"猪头呼"))

    val info2 = ListBuffer[(Int,String)]()
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((first,second)=>(first._1,first._2,second._2)).print()

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
