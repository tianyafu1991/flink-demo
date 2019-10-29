package com.tianyafu.course05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class JavaDataStreamingDataSourceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        fromSocket(env);

//        nonParallelSourceFunction(env);
//        parallelSourceFunction(env);
//        richParallelSourceFunction(env);
//        unionFunction(env);
//        splitSelect(env);
        customSinkToMysql(env);
        env.execute();
    }

    public static void customSinkToMysql(StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.101.217", 9999);
        SingleOutputStreamOperator<User> mapStream = dataStreamSource.map(new MapFunction<String, User>() {
            @Override
            public User map(String input) throws Exception {
                String[] splits = input.split(",");
                User user = new User();
                user.setId(Integer.parseInt(splits[0]));
                user.setName(splits[1]);
                user.setSex(splits[2]);
                user.setSchool(splits[3]);
                return user;
            }
        });

        mapStream.addSink(new CustomMysqlSink());

    }

    public static void splitSelect(StreamExecutionEnvironment env) {
        DataStreamSource<Long> dataStreamSource = env.addSource(new JavaCustomerNonParallelSourceFunction()).setParallelism(1);

        SplitStream<Long> splitStream = dataStreamSource.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> list = new ArrayList<>();
                if (value % 2 == 0) {
                    list.add("even");
                } else {
                    list.add("odd");
                }
                return list;
            }
        });
        splitStream.select("even").print();

    }

    public static void unionFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data1 = env.addSource(new JavaCustomerNonParallelSourceFunction()).setParallelism(1);
        DataStreamSource<Long> data2 = env.addSource(new JavaCustomerNonParallelSourceFunction()).setParallelism(1);

        DataStream<Long> unionStream = data1.union(data2);
        unionStream.print();

    }

    public static void richParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> dataStreamSource = env.addSource(new JavaCustomerRichParallelSourceFunction()).setParallelism(3);
        dataStreamSource.print();
    }

    public static void parallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> dataStreamSource = env.addSource(new JavaCustomerParallelSourceFunction()).setParallelism(2);
        dataStreamSource.print();
    }

    public static void nonParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> dataStreamSource = env.addSource(new JavaCustomerNonParallelSourceFunction()).setParallelism(1);
        dataStreamSource.print();
    }


    public static void fromSocket(StreamExecutionEnvironment env){
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.101.217", 9999);
        dataStreamSource.print().setParallelism(1);

    }
}
