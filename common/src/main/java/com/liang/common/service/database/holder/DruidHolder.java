package com.liang.common.service.database.holder;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.dto.config.DBConfig;
import com.liang.common.service.database.factory.DruidFactory;
import com.liang.common.util.ConfigUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DruidHolder {
    private DruidHolder() {
    }

    private static final Map<String, DruidDataSource> dataSources = new ConcurrentHashMap<>();

    @SneakyThrows
    public static DruidPooledConnection getConnectionByName(String name) {
        if (dataSources.get(name) == null) {
            DBConfig config = ConfigUtils.getConfig().getDbConfigs().get(name);
            DruidDataSource druidDataSource = DruidFactory.createPool(config);
            DruidDataSource callback = dataSources.putIfAbsent(name, druidDataSource);
            //说明这次put已经有值了，druidDataSource无用了
            if (callback != null) {
                druidDataSource.close();
            }
        }
        return dataSources.get(name).getConnection();
    }
}