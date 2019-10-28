package com.tianyafu.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class JavaDataSetSinkApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Integer> lists = new ArrayList<>();

        for(int i = 0;i <11;i++){
            lists.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(lists).setParallelism(2);

        String filePath ="F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\sink";
        dataSource.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

    }
}
