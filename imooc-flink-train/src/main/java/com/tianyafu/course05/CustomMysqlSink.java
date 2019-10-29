package com.tianyafu.course05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CustomMysqlSink extends RichSinkFunction<User> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open");
        connection = getConnection();
        String sql  = "insert into user(id , name, sex,school) values(?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        System.out.println("close");
        if(preparedStatement!=null){
            preparedStatement.close();
        }
        if(connection!=null){
            connection.close();
        }

    }

    @Override
    public void invoke(User value, Context context) throws Exception {
        System.out.println("invoke");
        preparedStatement.setInt(1,value.getId());
        preparedStatement.setString(2,value.getName());
        preparedStatement.setString(3,value.getSex());
        preparedStatement.setString(4,value.getSchool());
        preparedStatement.execute();
    }

    public Connection getConnection() {
        String url = "jdbc:mysql://192.168.101.217:3306/test?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8";

        String driver = "com.mysql.jdbc.Driver";

        try {
            Class.forName(driver);
            Connection connection = DriverManager.getConnection(url, "dev", "lJZx2Ik5eqX3xBDp");
            return connection;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }
}
