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
        String sql = "INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/export/'ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE\n" +
                "SELECT `org_name`\n" +
                "FROM flink.open_api_record\n" +
                "WHERE token='aaf1704a-78ee-4e1d-bb04-039a6c8c93ab' AND interface_id=1152 AND request_date='2024-03-12';";
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception ignore) {
        }
    }
}
