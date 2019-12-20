package com.tianyafu.batch.api;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

public class MapPartitionDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<Long> dataSource = env.generateSequence(1, 20);

        MapPartitionOperator<Long, Long> mapPartitionOperator = dataSource.mapPartition(new MapPartitionFunction<Long, Long>() {
            @Override
            public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
                long count = 0;
                for (Long value : values) {
                    count++;
                }
                out.collect(count);
            }
        });

        mapPartitionOperator.print();

    }
}
