package com.tianyafu.course04;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class JavaDistributedCacheApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\hello.txt";
        //step1注册分布式缓存文件
        env.registerCachedFile(filePath,"distributedCache-java");

        List<String> list =new ArrayList<>();

        list.add("hadoop");
        list.add("spark");
        list.add("hbase");
        list.add("elasticsearch");
        list.add("flink");

        DataSource<String> dataSource = env.fromCollection(list);

        MapOperator<String, String> mapOperator = dataSource.map(new RichMapFunction<String, String>() {


            @Override
            public void open(Configuration parameters) throws Exception {
                //step2获取到分布式缓存文件
                File file = getRuntimeContext().getDistributedCache().getFile("distributedCache-java");
                List<String> list1 = FileUtils.readLines(file);

                for (String str : list1) {
                    System.out.println(str);
                }

            }

            @Override
            public String map(String s) throws Exception {
                return s;
            }
        });

        mapOperator.print();

    }
}
