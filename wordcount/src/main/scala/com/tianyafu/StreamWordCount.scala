package com.tianyafu

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * 读取流形式的Wordcount
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    //创建一个流式的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从流中动态的读取数据
    val inputPath = "E:\\WorkSpace\\IDEAWorkspace\\flinkdemo\\wordcount\\src\\main\\resource\\streamwordcount.conf"
    val param = ParameterTool.fromPropertiesFile(inputPath)
    val host = param.get("host")
    val port = param.getInt("port")
    val socketDStream = env.socketTextStream(host,port)

    // tranformation 转换
    val wordCountDS = socketDStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    //输出
    wordCountDS.print()

    //启动executor
    env.execute()

  }

}
