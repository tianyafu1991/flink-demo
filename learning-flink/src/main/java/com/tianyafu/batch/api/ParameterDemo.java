package com.tianyafu.batch.api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class ParameterDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env= ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> data = env.fromElements(1, 2, 3, 4, 5);

        //处理数据
        /**
         * 构造方法传参
         */
//        data.filter(new MyFilter(2)).print();

        /**
         * parameter传参
         */

        /*Configuration config = new Configuration();
        config.setInteger("limit",3);
        data.filter(new RichFilterFunction<Integer>() {
            private Integer limit ;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                limit = parameters.getInteger("limit",0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value>limit;
            }
        }).withParameters(config).print();*/

        /**
         * 全局参数传参
         */

        Configuration config = new Configuration();
        config.setInteger("limit",3);
        env.getConfig().setGlobalJobParameters(config);

        data.filter(new RichFilterFunction<Integer>() {
            private int limit = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                Configuration globalJobParameters = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                limit = globalJobParameters.getInteger("limit", 0);

            }

            @Override
            public boolean filter(Integer value) throws Exception {

                return value>limit;

            }
        }).print();
    }

    /**
     * 构造方法传参
     */
    public static class MyFilter implements FilterFunction<Integer>{
        private Integer limit = 0;

        public MyFilter(Integer limit ){
            this.limit = limit;
        }

        @Override
        public boolean filter(Integer value) throws Exception {
            return value>limit;
        }
    }
}

