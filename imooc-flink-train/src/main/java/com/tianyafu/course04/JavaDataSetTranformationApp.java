package com.tianyafu.course04;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


public class JavaDataSetTranformationApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        filterFunction(env);
        mapPartitionFunction(env);

    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for(int i = 0;i<11;i++){
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list);
        MapOperator<Integer, String> mapOperator = dataSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer input) throws Exception {
                return "天涯"+input;
            }
        });

        mapOperator.print();

    }


    public static void filterFunction(ExecutionEnvironment env) throws  Exception{
        List<Integer> list = new ArrayList<>();
        for(int i = 0;i<11;i++){
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list);
        MapOperator<Integer, String> mapOperator = dataSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer input) throws Exception {
                return "天涯"+input;
            }
        });

        FilterOperator<String> filterOperator = mapOperator.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String input) throws Exception {
                Integer num = Integer.valueOf(input.substring(2));
                if (num % 2 == 0) {
                    System.out.println(input);
                    return true;
                }
                return false;
            }
        });

        filterOperator.print();

    }


    /**
     * 按数据分区进行map操作 常用于操作数据库 减少数据库连接的创建
     * @param env
     * @throws Exception
     */
    public static void mapPartitionFunction(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<>();
        for(int i = 0;i<11;i++){
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list).setParallelism(5);
        MapOperator<Integer, String> mapOperator = dataSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer input) throws Exception {
                return "天涯"+input;
            }
        });

        MapPartitionOperator<String, Integer> mapPartitionOperator = mapOperator.mapPartition(new MapPartitionFunction<String, Integer>() {
            @Override
            public void mapPartition(Iterable<String> input, Collector<Integer> collector) throws Exception {
                System.out.println("11111");
            }
        });

        mapPartitionOperator.print();

    }
}
