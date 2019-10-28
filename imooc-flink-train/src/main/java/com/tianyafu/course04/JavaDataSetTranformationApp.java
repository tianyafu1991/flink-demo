package com.tianyafu.course04;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


public class JavaDataSetTranformationApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        filterFunction(env);
//        mapPartitionFunction(env);
//        FirstFunction(env);
//        flatMapFunction(env);
//        distinctFunction(env);
//        joinFunction(env);
//        outerJoinFunction(env);
        crossFunction(env);
    }

    /**
     * 笛卡尔积
     * @param env
     * @throws Exception
     */
    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> list1 = new ArrayList<>();
        list1.add("曼联");
        list1.add("曼城");
        List<Integer> list2 = new ArrayList<>();
        list2.add(3);
        list2.add(1);
        list2.add(0);

        DataSource<String> data1 = env.fromCollection(list1);
        DataSource<Integer> data2 = env.fromCollection(list2);

        data1.cross(data2).print();
    }

    public static void outerJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer,String>> info1 = new ArrayList<>();
        info1.add(new Tuple2(1,"PK哥"));
        info1.add(new Tuple2(2,"南哥"));
        info1.add(new Tuple2(3,"香蕉哥"));
        info1.add(new Tuple2(4,"猪头呼"));

        List<Tuple2<Integer,String>> info2 = new ArrayList<>();
        info2.add(new Tuple2(1,"北京"));
        info2.add(new Tuple2(2,"上海"));
        info2.add(new Tuple2(3,"成都"));
        info2.add(new Tuple2(5,"杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        //左外连接
        /*data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (second==null) {
                    return new Tuple3<>(first.f0,first.f1,"-");
                }else {
                    return new Tuple3<>(first.f0,first.f1,second.f1);
                }
            }
        }).print();*/

        //右外连接
        /*data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first==null) {
                    return new Tuple3<>(second.f0,"-",second.f1);
                }else {
                    return new Tuple3<>(first.f0,first.f1,second.f1);
                }
            }
        }).print();*/

        //全外连接

        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first==null) {
                    return new Tuple3<>(second.f0,"-",second.f1);
                }else if (second==null){
                    return new Tuple3<>(first.f0,first.f1,"-");
                }else {
                    return new Tuple3<>(first.f0,first.f1,second.f1);
                }
            }
        }).print();
    }

    /**
     * 内连接
     * @param env
     * @throws Exception
     */
    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer,String>> info1 = new ArrayList<>();
        info1.add(new Tuple2(1,"PK哥"));
        info1.add(new Tuple2(2,"南哥"));
        info1.add(new Tuple2(3,"香蕉哥"));
        info1.add(new Tuple2(4,"猪头呼"));

        List<Tuple2<Integer,String>> info2 = new ArrayList<>();
        info2.add(new Tuple2(1,"北京"));
        info2.add(new Tuple2(2,"上海"));
        info2.add(new Tuple2(3,"成都"));
        info2.add(new Tuple2(5,"杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<>(first.f0,first.f1,second.f1);
            }
        }).print();
    }


    /**
     * distinct算子
     * @param env
     * @throws Exception
     */
    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> lists = new ArrayList<>();
        lists.add("hello spark");
        lists.add("flink spark");
        lists.add("elasticsearch spark");
        lists.add("hello flink");

        DataSource<String> dataSource = env.fromCollection(lists);

        dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] splits = input.split(" ");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).distinct().print();
    }

    public static void flatMapFunction(ExecutionEnvironment env)throws Exception{
        List<String> lists = new ArrayList<>();
        lists.add("hello spark");
        lists.add("flink spark");
        lists.add("elasticsearch spark");
        lists.add("hello flink");

        DataSource<String> dataSource = env.fromCollection(lists);

        AggregateOperator<Tuple2<String, Integer>> sum = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] splits = input.split(" ");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String input) throws Exception {
                return new Tuple2<>(input, 1);
            }
        }).groupBy(0).sum(1);

        sum.print();
    }


    /**
     * First方法
     * @param env
     * @throws Exception
     */
    public static void FirstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer,String>> data = new ArrayList<>();
        data.add(new Tuple2(1,"Flink"));
        data.add(new Tuple2(1,"hadoop"));
        data.add(new Tuple2(1,"spark"));
        data.add(new Tuple2(2,"java"));
        data.add(new Tuple2(2,"springboot"));
        data.add(new Tuple2(3,"linux"));
        data.add(new Tuple2(4,"vue"));

        DataSource<Tuple2<Integer, String>> dataSource = env.fromCollection(data);

        GroupReduceOperator<Tuple2<Integer, String>, Tuple2<Integer, String>> firstOperator = dataSource.first(2);
        firstOperator.print();

        GroupReduceOperator<Tuple2<Integer, String>, Tuple2<Integer, String>> firstGroupOperator = dataSource.groupBy(0).first(2);
        firstGroupOperator.print();

        GroupReduceOperator<Tuple2<Integer, String>, Tuple2<Integer, String>> firstGroupSortOperator = dataSource.groupBy(0).sortGroup(1, Order.DESCENDING).first(2);
        firstGroupSortOperator.print();

    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for(int i = 0;i<11;i++){
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list);
        MapOperator<Integer, String> mapOperator = dataSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer input) throws Exception {
                return "天涯"+input;
            }
        });

        mapOperator.print();

    }


    public static void filterFunction(ExecutionEnvironment env) throws  Exception{
        List<Integer> list = new ArrayList<>();
        for(int i = 0;i<11;i++){
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list);
        MapOperator<Integer, String> mapOperator = dataSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer input) throws Exception {
                return "天涯"+input;
            }
        });

        FilterOperator<String> filterOperator = mapOperator.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String input) throws Exception {
                Integer num = Integer.valueOf(input.substring(2));
                if (num % 2 == 0) {
                    System.out.println(input);
                    return true;
                }
                return false;
            }
        });

        filterOperator.print();

    }


    /**
     * 按数据分区进行map操作 常用于操作数据库 减少数据库连接的创建
     * @param env
     * @throws Exception
     */
    public static void mapPartitionFunction(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<>();
        for(int i = 0;i<11;i++){
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list).setParallelism(5);
        MapOperator<Integer, String> mapOperator = dataSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer input) throws Exception {
                return "天涯"+input;
            }
        });

        MapPartitionOperator<String, Integer> mapPartitionOperator = mapOperator.mapPartition(new MapPartitionFunction<String, Integer>() {
            @Override
            public void mapPartition(Iterable<String> input, Collector<Integer> collector) throws Exception {
                System.out.println("11111");
            }
        });

        mapPartitionOperator.print();

    }
}
