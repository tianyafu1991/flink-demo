package com.tianyafu.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class UnionDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(101,"lily"));
        list1.add(new Tuple2<>(102,"lucy"));
        list1.add(new Tuple2<>(103,"tom"));

        ArrayList<Tuple2<Integer,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(101,"孙悟空"));
        list2.add(new Tuple2<>(102,"唐三藏"));
        list2.add(new Tuple2<>(103,"猪八戒"));

        DataSource<Tuple2<Integer, String>> listDataSet1 = env.fromCollection(list1);
        DataSource<Tuple2<Integer, String>> listDataSet2 = env.fromCollection(list2);

        DataSet<Tuple2<Integer, String>> union = listDataSet1.union(listDataSet2);
        union.print();
    }
}
