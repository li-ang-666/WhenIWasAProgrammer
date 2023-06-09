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
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db", "user", "password");
            connection.prepareStatement("create table test(id int,info varchar(255))").execute();
            connection.prepareStatement("insert into test values(1,'aaa'),(2,'bbb')").execute();
            ResultSet resultSet = connection.prepareStatement("select * from test").executeQuery();
            while (resultSet.next()){
                String id = resultSet.getString(1);
                String info = resultSet.getString(2);
                log.info("{} -> {}",id,info);
            }
            ResultSet resultSet1 = connection.prepareStatement("select '002' > 1 from test").executeQuery();
            while (resultSet1.next()){
                String id = resultSet1.getString(1);
                log.info("{} -> {}",id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
