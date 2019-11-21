package com.imooc.stream.task;

import com.imooc.stream.map.PindaopvuvFlatMap;
import com.imooc.stream.reduce.PindaopvuvReduce;
import com.imooc.stream.sink.PindaopvuvSinkFunction;
import com.imooc.transfer.KafkaMessageSchema;
import com.imooc.transfer.KafkaMessageWatermarks;
import com.youfan.analy.PindaoPvUv;
import com.youfan.entity.KafkaMessage;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * Created by Administrator on 2018/10/27 0027.
 */
public class SSXinXianDuProcessData {
    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir","E:\\soft\\hadoop-2.6.0-cdh5.5.1\\hadoop-2.6.0-cdh5.5.1");
        args = new String[]{"--input-topic","flink-kafka-msi","--output-topic","flink-kafka-output-msi","--bootstrap.servers","dsd:9092,dse:9092,dsf:9092",
                "--zookeeper.connect","dsd:2181,dse:2181,dsf:2181","--group.id","flinkDSSSanaly"};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        FlinkKafkaConsumer010  flinkKafkaConsumer = new FlinkKafkaConsumer010<KafkaMessage>(parameterTool.getRequired("input-topic"), new KafkaMessageSchema(), parameterTool.getProperties());
        DataStream<KafkaMessage> input = env.addSource(flinkKafkaConsumer.assignTimestampsAndWatermarks(new KafkaMessageWatermarks()));
        SingleOutputStreamOperator<PindaoPvUv> pindaoPvUvSingleOutputStreamOperator = input.flatMap(new PindaopvuvFlatMap());
        WindowedStream<PindaoPvUv, Tuple, GlobalWindow> groupbyfieldStream = pindaoPvUvSingleOutputStreamOperator.keyBy("groupbyfield").countWindow(5);
        SingleOutputStreamOperator<PindaoPvUv> reduce = groupbyfieldStream.reduce(new PindaopvuvReduce());
        reduce.addSink(new PindaopvuvSinkFunction());


        /*DataStream<PindaoPvUv> map = input.flatMap(new PindaopvuvFlatMap());
        DataStream<PindaoPvUv> reduce = map.keyBy("groupbyfield").countWindow(Long.valueOf(parameterTool.getRequired("winsdows.size"))).reduce(new PindaopvuvReduce());
//        reduce.print();
        reduce.addSink(new Pindaopvuvsinkreduce()).name("pdpvuvreduce");*/
        try {
            env.execute("pindaossfx");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
