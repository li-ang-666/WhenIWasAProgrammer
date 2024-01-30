package com.liang.flink.job;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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
    private static final int THREAD_NUM = 40;
    private static final int CREATE_TIMESTAMP = -7;

    public static void main(String[] args) throws Exception {
        Class.forName(DRIVER);
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
        for (int i = 0; i < THREAD_NUM; i++) {
            final int crowdId = i;
            new Thread(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
                    for (String hiveConfigSql : HIVE_CONFIG_SQLS) {
                        connection.prepareStatement(hiveConfigSql).executeUpdate();
                    }
                    String sql = "INSERT INTO test.crowd_user_bitmap PARTITION(pt=20240129) " +
                            "SELECT %s crowd_id, %s create_timestamp, cast(count(t1.old_user_id) as string) user_id_bitmap " +
                            "FROM dim_offline.dim_user_comparison_df t1 where t1.pt = 20240129 and t1.old_user_id regexp '^\\\\d+$'";
                    connection.prepareStatement(String.format(sql, crowdId, CREATE_TIMESTAMP)).executeUpdate();
                    connection.close();
                    log.info("insert-{} done", crowdId);
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
    }
}
