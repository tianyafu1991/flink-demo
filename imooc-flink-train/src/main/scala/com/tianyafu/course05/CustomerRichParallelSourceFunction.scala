package com.tianyafu.course05

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class CustomerRichParallelSourceFunction extends RichParallelSourceFunction[Long]{
  var counter = 1L
  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning ){
      ctx.collect(counter)
      counter +=1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {isRunning=false}
}
