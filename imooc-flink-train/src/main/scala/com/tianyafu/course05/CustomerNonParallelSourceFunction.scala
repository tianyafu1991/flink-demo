package com.tianyafu.course05

import org.apache.flink.streaming.api.functions.source.SourceFunction

class CustomerNonParallelSourceFunction extends SourceFunction[Long]{

  var count = 1L

  var isRunning = true

  def main(args: Array[String]): Unit = {

  }

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      ctx.collect(count)
      count +=1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
