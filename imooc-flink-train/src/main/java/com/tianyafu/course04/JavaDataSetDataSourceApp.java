package com.tianyafu.course04;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class JavaDataSetDataSourceApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env);

        fromTextFile(env);
    }

    public static void  fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for(int i =0; i <11; i++){
            list.add(i);
        }
        env.fromCollection(list).print();
    }


    public static void fromTextFile(ExecutionEnvironment env) throws Exception{
        String  filePath = "E:\\WorkSpace\\IDEAWorkspace\\flinkdemo\\imooc-flink-train\\src\\main\\resource\\hello.txt";

        env.readTextFile(filePath).print();
        System.out.println("华丽的分割性~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        filePath = "E:\\WorkSpace\\IDEAWorkspace\\flinkdemo\\imooc-flink-train\\src\\main\\resource";
        env.readTextFile(filePath).print();

    }
}
