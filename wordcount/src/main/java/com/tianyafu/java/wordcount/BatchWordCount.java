package com.tianyafu.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        //Obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Load/create the initial data
        String path  = "F:\\tianyafu\\tianyafu_github\\flink-demo\\wordcount\\src\\main\\resource\\hello.txt";
        DataSource<String> dataSource = env.readTextFile(path);

        //Specify transformations on this data
        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strs = value.toLowerCase().split(",");
                for (String str : strs) {
                    if(str.length()>0){
                        collector.collect(new Tuple2<String, Integer>(str,1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();

        //Specify where to put the results of your computations

        //Trigger the program execution
//        env.execute();



    }
}
