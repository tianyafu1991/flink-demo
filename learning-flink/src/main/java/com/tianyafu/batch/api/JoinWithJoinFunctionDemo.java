package com.tianyafu.batch.api;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * join并且处理join之后的数据
 */
public class JoinWithJoinFunctionDemo {

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

        JoinOperator.EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, UserInfo> userInfoDs = ds1.join(ds2).where(0).equalTo(0).with(new FlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, UserInfo>() {
            @Override
            public void join(Tuple2<Integer, String> first, Tuple2<Integer, String> second, Collector<UserInfo> out) throws Exception {
                out.collect(new UserInfo(first.f0, first.f1, second.f1));
            }
        });

        userInfoDs.print();



    }
}
class UserInfo{

    private Integer userId;

    private String userName;

    private String address;

    public UserInfo() {
    }

    public UserInfo(Integer userId, String userName, String address) {
        this.userId = userId;
        this.userName = userName;
        this.address = address;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}