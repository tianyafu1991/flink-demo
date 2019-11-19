package com.imooc.stream;

import com.imooc.stream.map.PingdaoKafkaMap;
import com.imooc.stream.reduce.PindaoReduce;
import com.imooc.transfer.KafkaMessageSchema;
import com.imooc.transfer.KafkaMessageWatermarks;
import com.youfan.analy.PindaoRD;
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
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Created by Administrator on 2018/10/27 0027.
 */
public class SSProcessData {
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
        SingleOutputStreamOperator<PindaoRD> map = input.map(new PingdaoKafkaMap());
        WindowedStream<PindaoRD, Tuple, GlobalWindow> pingdaoidWindowedStream = map.keyBy("pingdaoid").countWindow(10, 5);
        SingleOutputStreamOperator<PindaoRD> reduce = pingdaoidWindowedStream.reduce(new PindaoReduce());
//        reduce.print();
        FlinkJedisPoolConfig jedisConf = new FlinkJedisPoolConfig.Builder().setHost("192.168.101.212").setPort(6379).setDatabase(10).setPassword("NaRT9gnxMKZ6MqA2").build();
        reduce.addSink(new RedisSink<PindaoRD>(jedisConf, new RedisMapper<PindaoRD>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.LPUSH,"tianyafu");
            }

            @Override
            public String getKeyFromData(PindaoRD in) {
                return "pingdaord"+in.getPingdaoid();
            }

            @Override
            public String getValueFromData(PindaoRD in) {
                return in.getCount()+"";
            }
        }));

        /*DataStream<PidaoPvUv> map = input.flatMap(new PindaopvuvMap());
        DataStream<PidaoPvUv> reduce = map.keyBy("groupbyfield").countWindow(Long.valueOf(parameterTool.getRequired("winsdows.size"))).reduce(new PindaopvuvReduce());
//        reduce.print();
        reduce.addSink(new Pindaopvuvsinkreduce()).name("pdpvuvreduce");*/
        try {
            env.execute("pindaossfx");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
