package com.liang.common.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.database.factory.JdbcPoolFactory;
import com.liang.common.dto.config.DBConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JdbcPoolUtils {
    private JdbcPoolUtils() {
    }

    private static final ConcurrentHashMap<String, DataSource> dataSources = new ConcurrentHashMap<>();

    @SneakyThrows
    public static synchronized Connection getConnectionByName(String name) {
        if (dataSources.get(name) == null) {
            DBConfig config = ConfigUtils.getConfig().getDBConfigs().get(name);
            DataSource dataSource = JdbcPoolFactory.createMySQLConnectionPool(config);
            dataSources.put(name, dataSource);
        }
        return dataSources.get(name).getConnection();
    }

    public static synchronized void release() {
        dataSources.forEach((name, dataSource) -> ((DruidDataSource) dataSource).close());
    }
}