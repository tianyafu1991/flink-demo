package com.tianyafu.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 流sql示例
 */
public class StreamSQLDemo {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建一个TableEnviroment
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(sEnv);

        //读取数据源
        String path = "E:/WorkSpace/IDEAWorkspace/flinkdemo/learning-flink/src/main/resource/user.txt";
        DataStream<String> ds1 = sEnv.readTextFile(path);

        //数据转换
        DataStream<Tuple2<String, String>> ds2 = ds1.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], split[1]);
            }
        });

        //将DataStream转换为table
        Table table = sTableEnv.fromDataStream(ds2,"uid,name");

        //注册为一个表
        sTableEnv.registerTable("user1",table);

        Table table2 = sTableEnv.sqlQuery("select * from user1").select("name");

        //将表转换成DataStream流
        DataStream<String> dataStream = sTableEnv.toAppendStream(table2, String.class);

        //打印输出
        dataStream.print();
        //执行
        sEnv.execute("StreamSQLDemo");

    }
}
