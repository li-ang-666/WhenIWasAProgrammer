package com.liang.common.service.database.factory;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.dto.config.DBConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.*;

@Slf4j
public class DruidFactory implements SinglePoolFactory<DBConfig, DruidDataSource> {
    private static final Map<String, String> URL_PROP_MAP = new HashMap<>();
    private static final String URL_PROP_STR;
    private static final List<String> OTHER_INIT_SQLS = new ArrayList<>();

    static {
        // map
        URL_PROP_MAP.put("serverTimezone", "GMT%2B8");
        URL_PROP_MAP.put("zeroDateTimeBehavior", "convertToNull");
        URL_PROP_MAP.put("yearIsDateType", "false");
        URL_PROP_MAP.put("tinyInt1isBit", "false");
        URL_PROP_MAP.put("useUnicode", "true");
        URL_PROP_MAP.put("characterEncoding", "UTF-8");
        URL_PROP_MAP.put("characterSetResults", "UTF-8");
        URL_PROP_MAP.put("useSSL", "false");
        URL_PROP_MAP.put("autoReconnect", "true");
        URL_PROP_MAP.put("maxAllowedPacket", String.valueOf(1024 * 1024 * 1024));
        URL_PROP_MAP.put("rewriteBatchedStatements", "true");
        // str
        URL_PROP_STR = URL_PROP_MAP.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        // list
        OTHER_INIT_SQLS.add("set wait_timeout = 3600 * 24 * 7");
        OTHER_INIT_SQLS.add("set interactive_timeout = 3600 * 24 * 7");
    }

    @Override
    public DruidDataSource createPool(String name) {
        return createPool(ConfigUtils.getConfig().getDbConfigs().get(name));
    }

    @Override
    public DruidDataSource createPool(DBConfig config) {
        try {
            DruidDataSource druidDataSource = new DruidDataSource();
            String url = String.format("jdbc:mysql://%s:%s/%s?%s",
                    config.getHost(), config.getPort(), config.getDatabase(), URL_PROP_STR);
            druidDataSource.setUrl(url);
            druidDataSource.setUsername(config.getUser());
            druidDataSource.setPassword(config.getPassword());
            // 配置参数
            druidDataSource.setInitialSize(1);
            druidDataSource.setMinIdle(1);
            druidDataSource.setMaxActive(128);
            druidDataSource.setMaxWait(MILLISECONDS.convert(7, DAYS));
            druidDataSource.setTestOnBorrow(false);
            druidDataSource.setTestOnReturn(false);
            druidDataSource.setTestWhileIdle(true);
            // 管理minIdle
            druidDataSource.setTimeBetweenEvictionRunsMillis(MILLISECONDS.convert(30, SECONDS));
            druidDataSource.setMinEvictableIdleTimeMillis(MILLISECONDS.convert(60, SECONDS));
            // minIdle以内的连接保持活跃
            druidDataSource.setKeepAlive(true);
            druidDataSource.setValidationQuery("SELECT 1");
            // 其它
            druidDataSource.setPoolPreparedStatements(true);
            druidDataSource.setMaxOpenPreparedStatements(100);
            druidDataSource.setUsePingMethod(false);
            druidDataSource.setAsyncInit(true);
            // 隔离级别
            druidDataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            // 超时
            druidDataSource.setConnectionInitSqls(OTHER_INIT_SQLS);
            druidDataSource.setConnectTimeout((int) MILLISECONDS.convert(7, DAYS));
            druidDataSource.setSocketTimeout((int) MILLISECONDS.convert(7, DAYS));
            druidDataSource.setQueryTimeout((int) SECONDS.convert(7, DAYS));
            druidDataSource.setTransactionQueryTimeout((int) SECONDS.convert(7, DAYS));
            log.info("DruidFactory createPool success, config: {}", JsonUtils.toString(config));
            return druidDataSource;
        } catch (Exception e) {
            String msg = "DruidFactory createPool error, config: " + JsonUtils.toString(config);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }
}
