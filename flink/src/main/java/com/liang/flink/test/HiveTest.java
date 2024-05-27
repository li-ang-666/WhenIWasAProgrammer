package com.liang.flink.test;

import cn.hutool.core.io.IoUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;

@Slf4j
public class HiveTest {
    // hive jdbc
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String USER = "hive";
    private static final String PASSWORD = "";

    public static void main(String[] args) throws Exception {
        Class.forName(DRIVER);
//        String[] sqls = new String[]{
//                "set spark.yarn.queue=offline",
//                "set spark.yarn.priority=999",
//                "set spark.executor.memory=16g",
//                "set spark.executor.memoryOverhead=1g",
//                "set spark.driver.memory=2g",
//                "set spark.driver.memoryOverhead=1g",
//                "set mapred.max.split.size=9223372036854775807",
//                "set mapred.min.split.size.per.node=9223372036854775807",
//                "set mapred.min.split.size.per.rack=9223372036854775807",
//                "set mapred.reduce.tasks=1",
//        };
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
//            for (String sql : sqls) {
//                connection.prepareStatement(sql).executeUpdate();
//            }
            InputStream resourceAsStream = HiveTest.class.getClassLoader().getResourceAsStream("sql.txt");
            String sql = IoUtil.readUtf8(resourceAsStream);
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            log.error("error", e);
        }
    }
}
