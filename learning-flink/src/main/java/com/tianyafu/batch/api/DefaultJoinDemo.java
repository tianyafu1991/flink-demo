package com.tianyafu.batch.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * 默认join操作
 */
public class DefaultJoinDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(1,"lily"));
        list1.add(new Tuple2<>(2,"lucy"));
        list1.add(new Tuple2<>(3,"tom"));
        list1.add(new Tuple2<>(4,"jack"));

        ArrayList<Tuple2<Integer,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(1,"北京"));
        list2.add(new Tuple2<>(2,"上海"));
        list2.add(new Tuple2<>(3,"杭州"));
        list2.add(new Tuple2<>(5,"广州"));

        DataSource<Tuple2<Integer, String>> ds1 = env.fromCollection(list1);
        DataSource<Tuple2<Integer, String>> ds2 = env.fromCollection(list2);

        JoinOperator.DefaultJoin<Tuple2<Integer, String>, Tuple2<Integer, String>> join = ds1.join(ds2).where(0).equalTo(0);

        join.print();



    }
}
