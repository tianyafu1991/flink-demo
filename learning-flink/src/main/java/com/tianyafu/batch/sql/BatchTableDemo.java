package com.tianyafu.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.expressions.Expression;

public class BatchTableDemo {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //创建table运行环境
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        //读取数据
        String path = "E:/WorkSpace/IDEAWorkspace/flinkdemo/learning-flink/src/main/resource/user.txt";

        DataSet<String> ds1 = env.readTextFile(path);

        DataSet<Tuple2<String,String>> ds2 = ds1.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0],split[1]);
            }
        });
        //将DataSet转换为table
        Table table = tEnv.fromDataSet(ds2,"uid,name");

        //查询数据
        Table name = table.select("name");

        //将表转换为DataSet
        DataSet<String> stringDataSet = tEnv.toDataSet(name, String.class);

        //打印
        stringDataSet.print();

    }
}
