package com.liang.repair.impl;

import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

@Slf4j
public class CommonTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        log.info("1-------------------");
        Class.forName("org.hsqldb.jdbc.JDBCDriver");
        log.info("2-------------------");
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db", "user", "password");
        log.info("3-------------------");
        connection.prepareStatement("create table test(id int primary key,info varchar(65530))").execute();
        log.info("4-------------------");
        connection.prepareStatement("insert into test values (1,'aaa')").execute();
        connection.prepareStatement("insert into test values (2,'bbb')").execute();
        connection.prepareStatement("insert into test values (3,'ccc')").execute();
        connection.prepareStatement("insert into test values (4,'ddd')").execute();
        log.info("5-------------------");
        ResultSet resultSet = connection.prepareStatement("SELECT count(1) FROM test").executeQuery();
        log.info("6-------------------");
        while (resultSet.next()){
            System.out.println(resultSet.getString(1));
        }

    }
}
