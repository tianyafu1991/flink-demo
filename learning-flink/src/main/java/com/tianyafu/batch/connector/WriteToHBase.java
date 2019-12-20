package com.tianyafu.batch.connector;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class WriteToHBase {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSet<Tuple4<String,String,Integer,String>> users = env.fromElements(Tuple4.of("1010", "zhangsan", 30, "beijing"),
                Tuple4.of("1011", "lisi", 23, "guangzhou"),
                Tuple4.of("1012", "wangwu", 45, "hubei"),
                Tuple4.of("1013", "zhaoliu", 23, "beijing"),
                Tuple4.of("1014", "lilei", 56, "henan"),
                Tuple4.of("1015", "maxiaoshuai", 34, "xizang"),
                Tuple4.of("1016", "liudehua", 26, "fujian"),
                Tuple4.of("1017", "jiangxiaohan", 18, "hubei"),
                Tuple4.of("1018", "qianjin", 29, "shanxi"),
                Tuple4.of("1019", "zhujie", 37, "shangdong"),
                Tuple4.of("1020", "taobinzhe", 19, "guangxi"),
                Tuple4.of("1021", "muqixian", 20, "hainan")
        );
        //转换为hbase所需的数据格式
        DataSet<Tuple2<Text, Mutation>> result = convertResultToMutation(users);
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master,slave01,slave02");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set(TableOutputFormat.OUTPUT_TABLE,"learning_flink:users");
        conf.set("mapreduce.output.fileoutputformat.outputdir","/tmp");

        Job job = Job.getInstance(conf);
        result.output(new HadoopOutputFormat<Text,Mutation>(new TableOutputFormat<Text>(),job));

        env.execute("WriteToHBaseDemo");


    }

    //转换为hbase所需的数据格式
    public static DataSet<Tuple2<Text, Mutation>> convertResultToMutation(DataSet<Tuple4<String,String,Integer,String>> tuple4DataSet){

        return tuple4DataSet.map(new RichMapFunction<Tuple4<String, String, Integer, String>, Tuple2<Text, Mutation>>() {

            private transient Tuple2<Text,Mutation> resultTuple;

            private byte[] cf = "F".getBytes(ConfigConstants.DEFAULT_CHARSET);

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                resultTuple = new Tuple2<>();
            }

            @Override
            public Tuple2<Text, Mutation> map(Tuple4<String, String, Integer, String> user) throws Exception {
                resultTuple.f0 = new Text(user.f0);
                Put put = new Put(user.f0.getBytes(ConfigConstants.DEFAULT_CHARSET));
                if(null != user.f1){
                    put.addColumn(cf, Bytes.toBytes("name"),Bytes.toBytes(user.f1));
                }
                if(null!=user.f2){
                    put.addColumn(cf, Bytes.toBytes("age"),Bytes.toBytes(user.f2.toString()));
                }
                if(null!=user.f3){
                    put.addColumn(cf, Bytes.toBytes("address"),Bytes.toBytes(user.f3));
                }

                resultTuple.f1 = put;

                return resultTuple;
            }
        });
    }
}
