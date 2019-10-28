package com.tianyafu.course04

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * 计数器
  * 1.获取计数器
  * 2.注册计数器
  * 3.获取计数器结果
  */
object CounterApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val list = List("hadoop","flink","spark","elasticsearch","hbase")

    val dataSource = env.fromCollection(list)

    //当并行度大于1的时候  计数器就出现了问题
    /*dataSource.map(new RichMapFunction[String,Long] {
      var counter = 0L
      override def map(in: String): Long = {
        counter = counter +1
        print("counter:"+counter)
        counter

      }
    }).setParallelism(5).print()*/

    val info = dataSource.map(new RichMapFunction[String, String] {
      //step1.获取计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        //step2.注册计数器
        getRuntimeContext.addAccumulator("scala-counter", counter)
      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })
    val filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\sink-scala"
    info.writeAsText(filePath,WriteMode.OVERWRITE).setParallelism(5)
    val jobResult = env.execute()
    // step3.获取计数器结果
    val num = jobResult.getAccumulatorResult[Long]("scala-counter")

    print("num:"+num)



  }

}
