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
            "set spark.hadoop.yarn.nm.liveness-monitor.expiry-interval-ms=5000",
            // executor
            "set spark.executor.cores=1",
            "set spark.executor.memory=8g",
            "set spark.executor.memoryOverhead=512m",
            // driver
            "set spark.driver.memory=2g",
            "set spark.driver.memoryOverhead=512m",
            // name
            "set spark.app.name=abc"
    );

    public static void main(String[] args) throws Exception {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        Class.forName(DRIVER);
        Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
        for (String hiveConfigSql : HIVE_CONFIG_SQLS) {
            connection.prepareStatement(hiveConfigSql).executeUpdate();
        }
        ResultSet resultSet = connection.prepareStatement("select count(1) from hudi_ods.company_bond_plates").executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
        while (true) ;
    }
}
