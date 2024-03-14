package com.liang.flink.test;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;

@Slf4j
public class FlinkTest {
    // hive jdbc
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String USER = "hive";
    private static final String PASSWORD = "";

    public static void main(String[] args) throws Exception {
        Class.forName(DRIVER);
        String sql = "!sh hdfs dfs -ls /";
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            connection.prepareStatement("set spark.yarn.queue=offline").executeUpdate();
        } catch (Exception ignore) {
        }
    }
}
