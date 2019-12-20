package com.tianyafu.batch.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 数据去重
 */
public class DistinctDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSource<Tuple3<Long, String, Integer>> tuple3DataSource = env.fromElements(Tuple3.of(1L, "zhangsan", 28)
                , Tuple3.of(3L, "lisi", 34),
                Tuple3.of(3L, "wangwu", 23),
                Tuple3.of(3L, "zhaoliu", 34),
                Tuple3.of(3L, "lili", 25));

        DistinctOperator<Tuple3<Long, String, Integer>> distinct = tuple3DataSource.distinct(0);
        distinct.print();
    }
}
