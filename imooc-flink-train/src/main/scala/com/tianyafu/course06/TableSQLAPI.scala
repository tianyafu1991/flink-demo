package com.tianyafu.course06

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TableSQLAPI {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // ******************
    // FLINK BATCH QUERY
    // ******************
    val tableEnv = BatchTableEnvironment.create(env)

    val filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\sales.csv"
    // 文本 ==> DataSet
    val sales = env.readCsvFile[SaleLog](filePath,ignoreFirstLine=true)
    // DataSet ==> Table
    val salesTable = tableEnv.fromDataSet(sales)

    //Table ==> table
    tableEnv.registerTable("sales",salesTable)

    val resultTable = tableEnv.sqlQuery("select customerId,sum(amountPaId) from sales group by customerId")

    val result = tableEnv.toDataSet[Row](resultTable)

    result.print()



  }

}

case class SaleLog(transactionId:String,customerId:String,itemId:String,amountPaId:Double)
