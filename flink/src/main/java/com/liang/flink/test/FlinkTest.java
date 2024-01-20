package com.liang.flink.test;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

@Slf4j
public class FlinkTest {
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
        Class.forName(HIVE_DRIVER);
        Connection connection = DriverManager.getConnection("jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2", "hive", "");
        ResultSet rs = connection.prepareStatement("show databases").executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}
