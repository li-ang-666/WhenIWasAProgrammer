package com.liang.common.util;

import com.liang.common.database.factory.JdbcPoolFactory;
import com.liang.common.dto.config.DBConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JdbcPoolUtils {
    private JdbcPoolUtils() {
    }

    private static final Map<String, DataSource> dataSources = new HashMap<>();

    @SneakyThrows
    public static synchronized Connection getConnectionByName(String name) {
        if (dataSources.get(name) == null) {
            DBConfig config = ConfigUtils.getConfig().getDbConfigs().get(name);
            DataSource dataSource = JdbcPoolFactory.createPool(config);
            dataSources.put(name, dataSource);
        }
        return dataSources.get(name).getConnection();
    }
}