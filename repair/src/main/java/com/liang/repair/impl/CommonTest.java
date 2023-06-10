package com.liang.repair.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;

@Slf4j
public class CommonTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setTestWhileIdle(false);
        druidDataSource.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
        druidDataSource.setUrl("jdbc:hsqldb:mem:db");
        druidDataSource.setUsername("user");
        druidDataSource.setPassword("password");



        Connection connection;
        connection = druidDataSource.getConnection();
        log.info("{}",connection);
        connection.prepareStatement("create table t1 (id varchar(255),name varchar(255))").execute();
        connection.prepareStatement("create table t2 (id varchar(255),name varchar(255))").execute();
        connection.prepareStatement("create table t3 (id varchar(255),name varchar(255))").execute();
        //connection.close();
        connection = druidDataSource.getConnection();
        log.info("{}",connection);
        ResultSet resultSet = connection.prepareStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC'").executeQuery();
        while (resultSet.next()){
            System.out.println(resultSet.getString(1));
        }

    }
}
