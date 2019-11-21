package com.imooc.stream.task;

import com.imooc.stream.map.UsernetworkMap;
import com.imooc.stream.reduce.UsernetworkReduce;
import com.imooc.stream.sink.Usernetworksinkreduce;
import com.imooc.transfer.KafkaMessageSchema;
import com.imooc.transfer.KafkaMessageWatermarks;
import com.youfan.analy.Usernetwork;
import com.youfan.entity.KafkaMessage;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * Created by Administrator on 2018/10/27 0027.
 */
public class UserbrowseranalyProcessData {
    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir","E:\\soft\\hadoop-2.6.0-cdh5.5.1\\hadoop-2.6.0-cdh5.5.1");
//        args = new String[]{"--input-topic","test1","--bootstrap.servers","192.168.253.151:9092",
//                "--zookeeper.connect","192.168.253.151:2181","--group.id","myconsumer1","--winsdows.size","50"};

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        FlinkKafkaConsumer010  flinkKafkaConsumer = new FlinkKafkaConsumer010<KafkaMessage>(parameterTool.getRequired("input-topic"), new KafkaMessageSchema(), parameterTool.getProperties());
        DataStream<KafkaMessage> input = env.addSource(flinkKafkaConsumer.assignTimestampsAndWatermarks(new KafkaMessageWatermarks()));
        DataStream<Usernetwork> map = input.flatMap(new UsernetworkMap());
        DataStream<Usernetwork> reduce = map.keyBy("timestring").countWindow(Long.valueOf(parameterTool.getRequired("winsdows.size"))).reduce(new UsernetworkReduce());
//        reduce.print();
        reduce.addSink(new Usernetworksinkreduce()).name("usernetworkreduce");
        try {
            env.execute("pindaossfx");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
