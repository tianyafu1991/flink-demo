package com.tianyafu.course04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class JavaCounterApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String>  list = new ArrayList<>();
        list.add("hadoop");
        list.add("spark");
        list.add("elasticsearch");
        list.add("flink");
        list.add("hbase");

        DataSource<String> dataSource = env.fromCollection(list);

        MapOperator<String, String> mapOperator = dataSource.map(new RichMapFunction<String, String>() {
            //step1. 获取计数器
            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                //注册计数器
                getRuntimeContext().addAccumulator("java-counter",counter);
            }

            @Override
            public String map(String input) throws Exception {
                counter.add(1);
                return input;
            }
        });
        String filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\sink-java";
        mapOperator.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(4);
        JobExecutionResult jobExecutionResult = env.execute();
        Long num = jobExecutionResult.getAccumulatorResult("java-counter");
        System.out.println("num:"+num);
    }
}
