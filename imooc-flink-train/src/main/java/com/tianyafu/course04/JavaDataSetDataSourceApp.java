package com.tianyafu.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

public class JavaDataSetDataSourceApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env);

//        fromTextFile(env);

//        fromCsvFile(env);
//        fromRecursiveFile(env);
        fromCompressionFile(env);
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

    /**
     * 读取csv文件
     * @param env
     * @throws Exception
     */
    public static void fromCsvFile(ExecutionEnvironment env) throws Exception {
        String filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\user.csv";
        env.readCsvFile(filePath).ignoreFirstLine().includeFields(false,true,true,true).pojoType(MyPerson.class, "name", "sex", "school").print();

    }

    /**
     * 递归读取文件
     * @param env
     * @throws Exception
     */
    public static void fromRecursiveFile(ExecutionEnvironment env) throws Exception {
        String filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource";

        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);

        env.readTextFile(filePath).withParameters(parameters).print();


    }

    /**
     * 读取压缩文件
     * @param env
     * @throws Exception
     */
    public static void fromCompressionFile(ExecutionEnvironment env) throws Exception {
        String filePath = "F:\\tianyafu\\tianyafu_github\\flink-demo\\imooc-flink-train\\src\\main\\resource\\userCsv.gz";

        env.readTextFile(filePath).print();
    }
}
