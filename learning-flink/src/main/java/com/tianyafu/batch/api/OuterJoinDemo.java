package com.tianyafu.batch.api;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.join.JoinOperatorSetsBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class OuterJoinDemo {

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

        /**
         * 左外连接
         * 注意：second tuple中元素可能为空 因为关联不上

        ds1.leftOuterJoin(ds2).where(0).equalTo(0).with(new FlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public void join(Tuple2<Integer, String> first, Tuple2<Integer, String> second, Collector<Tuple3<Integer, String, String>> out) throws Exception {
                if (second == null) {
                    out.collect(new Tuple3<>(first.f0, first.f1, "null"));
                } else {
                    out.collect(new Tuple3<>(first.f0, first.f1, second.f1));
                }
            }
        }).print();
         */


        /**
         * 右外连接
         * 注意：first可能为null

        ds1.rightOuterJoin(ds2).where(0).equalTo(0).with(new FlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public void join(Tuple2<Integer, String> first, Tuple2<Integer, String> second, Collector<Tuple3<Integer, String, String>> out) throws Exception {
                if(first==null){
                    out.collect(new Tuple3<>(second.f0,"null",second.f1));
                }else {
                    out.collect(new Tuple3<>(second.f0,first.f1,second.f1));
                }
            }
        }).print();*/

        /**
         * 全外连接
         * 笛卡尔积
         */
        ds1.fullOuterJoin(ds2).where(0).equalTo(0).with(new FlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public void join(Tuple2<Integer, String> first, Tuple2<Integer, String> second, Collector<Tuple3<Integer, String, String>> out) throws Exception {
                if(first ==null){
                    out.collect(new Tuple3<>(second.f0,"null",second.f1));
                }else if(second ==null){
                    out.collect(new Tuple3<>(first.f0,first.f1,"null"));
                }else {
                    out.collect(new Tuple3<>(first.f0,first.f1,second.f1));
                }
            }
        }).print();
    }
}
