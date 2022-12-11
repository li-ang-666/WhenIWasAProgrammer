package com.liang.common.database.cluster;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.database.pool.JdbcPoolFactory;
import com.liang.common.dto.config.MySQLConfig;
import com.liang.common.util.GlobalUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.liang.common.util.lambda.LambdaExceptionEraser.biConsumerEraser;

@Slf4j
public class JdbcPoolCluster {
    private JdbcPoolCluster() {
    }

    private static final ConcurrentHashMap<String, DataSource> dataSources = new ConcurrentHashMap<>();
    private static boolean init = false;

    @SneakyThrows
    public static Connection getConnectionByName(String name) {
        init();
        synchronized (JdbcPoolCluster.class) {
            return dataSources.get(name).getConnection();
        }
    }

    private static void init() {
        Map<String, MySQLConfig> mysqlConfigs = GlobalUtils.getConnectionConfig().getMysqlConfigs();
        init(mysqlConfigs);
    }

    private static void init(Map<String, MySQLConfig> mysqlConfigs) {
        if (init) return;
        synchronized (JdbcPoolCluster.class) {
            if (init) return;
            mysqlConfigs.forEach(biConsumerEraser((name, mysqlConfig) ->
                    dataSources.put(name, JdbcPoolFactory.createMySQLConnectionPool(mysqlConfig))
            ));
            init = true;
        }
    }

    public static void release() {
        if (!init) return;
        synchronized (JdbcPoolCluster.class) {
            if (!init) return;
            dataSources.forEach((k, v) -> ((DruidDataSource) v).close());
            init = false;
        }
    }
}