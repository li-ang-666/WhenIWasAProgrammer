package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class CrowdUserBitmapJob {
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String USER = "hive";
    private static final String PASSWORD = "";
    private static final List<String> HIVE_CONFIG_SQLS = Arrays.asList(
            "set spark.yarn.priority=999",
            // executor
            "set spark.executor.cores=1",
            "set spark.executor.memory=8g",
            "set spark.executor.memoryOverhead=512m",
            // driver
            "set spark.driver.memory=2g",
            "set spark.driver.memoryOverhead=512m"
    );

    public static void main(String[] args) throws Exception {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            for (String hiveConfigSql : HIVE_CONFIG_SQLS) {
                connection.prepareStatement(hiveConfigSql).executeUpdate();
            }
            ResultSet resultSet = connection.prepareStatement("select 1").executeQuery();
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }
        }
        while (true) ;
    }
}
