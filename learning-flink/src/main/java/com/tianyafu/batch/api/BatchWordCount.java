package com.tianyafu.batch.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.fromElements("flink flink flink", "spark spark spark");

        DataSet<Tuple2<String, Integer>> sum = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }).groupBy(0).sum(1);

        sum.print();


    }


}
