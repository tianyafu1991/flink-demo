package com.tianyafu.batch.api;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 分布式缓存实例
 */
public class DistributeCacheDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        String filePath = "E:/WorkSpace/IDEAWorkspace/flinkdemo/learning-flink/src/main/resource/user.txt";

        env.registerCachedFile(filePath,"userCache",true);

        //准备处理数据
        ArrayList<Tuple2<String ,Integer >> operatorData = new ArrayList<>();

        operatorData.add(new Tuple2<>("101",1000000));
        operatorData.add(new Tuple2<>("102",200000));
        operatorData.add(new Tuple2<>("103",30000));
//读取处理数据
        DataSource<Tuple2<String, Integer>> tuple2DataSource = env.fromCollection(operatorData);

        DataSet<String> result = tuple2DataSource.map(new RichMapFunction<Tuple2<String, Integer>, String>() {

            HashMap<String, String> allMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File userCache = getRuntimeContext().getDistributedCache().getFile("userCache");
                List<String> lines = FileUtils.readLines(userCache);

                for (String line : lines) {
                    String[] split = line.split(",");
                    allMap.put(split[0], split[1]);
                }
            }

            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                String name = allMap.get(value.f0);

                return name + "," + value.f1;
            }
        });

        result.print();

    }
}
