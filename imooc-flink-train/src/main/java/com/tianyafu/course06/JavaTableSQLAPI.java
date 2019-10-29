package com.tianyafu.course06;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class JavaTableSQLAPI {

    public static void main(String[] args) throws  Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        String filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\sales.csv";
        //文本 ==> DataSet
        DataSource<Sales> dataSource = env.readCsvFile(filePath).ignoreFirstLine().pojoType(Sales.class, "transactionId", "customerId", "itemId", "amountPaId");

        //DataSet ==> Table
        Table saleTable = tableEnv.fromDataSet(dataSource);

        //Table ==> table
        tableEnv.registerTable("sales",saleTable);

        Table sqlResult = tableEnv.sqlQuery("select customerId,sum(amountPaId) from sales group by customerId");

        DataSet<Row> salesDataSet = tableEnv.toDataSet(sqlResult, Row.class);

        salesDataSet.print();


    }
}
