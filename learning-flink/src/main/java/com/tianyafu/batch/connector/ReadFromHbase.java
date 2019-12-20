package com.tianyafu.batch.connector;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 批量读取HBase
 */
public class ReadFromHbase {

    public static final byte[] FAMILY = "F".getBytes(ConfigConstants.DEFAULT_CHARSET);

    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<String, String, Integer, String>> users = env.createInput(new TableInputFormat<Tuple4<String, String, Integer, String>>() {
            private HTable createTable() {
                org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "master");
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                try {
                    return new HTable(conf, getTableName());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }

            /**
             *
             * @param conf
             */
            @Override
            public void configure(Configuration conf) {
                super.configure(conf);
                table = createTable();
                if (null != table) {
                    scan = getScanner();
                }
            }

            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addFamily(FAMILY);
                return scan;
            }

            @Override
            protected String getTableName() {
                return "learning_flink:users";
            }

            @Override
            protected Tuple4<String, String, Integer, String> mapResultToTuple(Result result) {
                System.out.println("你好"+result);
                return Tuple4.of(Bytes.toString(result.getRow()),
                        Bytes.toString(result.getValue(FAMILY, "name".getBytes(ConfigConstants.DEFAULT_CHARSET))),
                        Integer.parseInt(Bytes.toString(result.getValue(FAMILY, "age".getBytes(ConfigConstants.DEFAULT_CHARSET)))),
                        Bytes.toString(result.getValue(FAMILY, "address".getBytes(ConfigConstants.DEFAULT_CHARSET)))
                );
            }
        });

        users.print();
    }
}
