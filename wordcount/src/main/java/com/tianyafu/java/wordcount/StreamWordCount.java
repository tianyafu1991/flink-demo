package com.tianyafu.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // source
        DataStreamSource<String> sourceStream = env.socketTextStream("192.168.101.217", 9999);

        //tranformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream = sourceStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strs = line.toLowerCase().split(" ");
                for (String str : strs) {
                    collector.collect(new Tuple2<String, Integer>(str, 1));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1);

        sumStream.print();

        env.execute();


    }
}
