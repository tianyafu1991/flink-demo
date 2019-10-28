package com.tianyafu.course04;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;

import java.util.ArrayList;
import java.util.List;

public class JavaCounterApp {

    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String>  list = new ArrayList<>();
        list.add("hadoop");
        list.add("spark");
        list.add("elasticsearch");
        list.add("flink");
        list.add("hbase");

        DataSource<String> dataSource = env.fromCollection(list);

        MapOperator<String, String> mapOperator = dataSource.map(new MapFunction<String, String>() {


            @Override
            public String map(String s) throws Exception {
                return null;
            }
        });
    }
}
