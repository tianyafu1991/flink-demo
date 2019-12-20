package com.tianyafu.batch.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 广播变量的使用
 */
public class BroadcastDemo {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        ArrayList<Tuple2<String ,String >> broadcastData = new ArrayList<>();

        broadcastData.add(new Tuple2<>("101","jack"));
        broadcastData.add(new Tuple2<>("102","tom"));
        broadcastData.add(new Tuple2<>("103","john"));
        DataSet<Tuple2<String, String>> dataSource = env.fromCollection(broadcastData);
        //数据集转换为map类型
        DataSet<HashMap<String, String>> broadcastDs = dataSource.map(new MapFunction<Tuple2<String, String>, HashMap<String, String>>() {
            @Override
            public HashMap<String, String> map(Tuple2<String, String> value) throws Exception {
                HashMap<String, String> map = new HashMap<>();
                map.put(value.f0, value.f1);
                return map;
            }
        });

        //准备处理数据
        ArrayList<Tuple2<String ,Integer >> operatorData = new ArrayList<>();

        operatorData.add(new Tuple2<>("101",1000000));
        operatorData.add(new Tuple2<>("102",200000));
        operatorData.add(new Tuple2<>("103",30000));

        //读取处理数据
        DataSource<Tuple2<String, Integer>> tuple2DataSource = env.fromCollection(operatorData);
        DataSet<String> result = tuple2DataSource.map(new RichMapFunction<Tuple2<String, Integer>, String>() {

            List<HashMap<String, String>> broadCastMap = new ArrayList<>();

            HashMap<String, String> allMap = new HashMap<String, String>();

            /**
             * open方法只会执行一次   所以适合获取广播变量  或者建立连接
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastName");
                for (HashMap<String, String> map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                String name = allMap.get(value.f0);
                return name +","+ value.f1;
            }
        }).withBroadcastSet(broadcastDs, "broadCastName");

        result.print();


    }
}
