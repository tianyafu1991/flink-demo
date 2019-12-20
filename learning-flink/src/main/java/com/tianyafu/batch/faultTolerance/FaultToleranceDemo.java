package com.tianyafu.batch.faultTolerance;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;

import java.util.concurrent.TimeUnit;

public class FaultToleranceDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //设置重启策略
        //固定延时策略  重试3次  每次重试间隔10秒  都失败了则任务失败
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        //失败率策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(2,Time.of(1,TimeUnit.HOURS),Time.of(10,TimeUnit.SECONDS)));

        DataSet<String> dataSet = env.fromElements("1","2",",","4");

        dataSet.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        }).print();
    }
}
