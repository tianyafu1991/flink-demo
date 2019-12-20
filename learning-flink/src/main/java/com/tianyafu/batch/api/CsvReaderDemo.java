package com.tianyafu.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;

public class CsvReaderDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String path  = "E:/WorkSpace/IDEAWorkspace/flinkdemo/learning-flink/src/main/resource/user.csv";

        DataSet<Tuple3<Integer,Integer,String>>csvDataSet = env.readCsvFile(path)
                .includeFields("11100")
                .ignoreFirstLine()
                .ignoreComments("##")
                .ignoreInvalidLines()
                .types(Integer.class,Integer.class,String.class);
        csvDataSet.print();

    }
}
