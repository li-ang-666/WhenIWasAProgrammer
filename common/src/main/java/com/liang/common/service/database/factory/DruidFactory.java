package com.liang.common.service.database.factory;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.dto.config.DBConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DruidFactory implements SinglePoolFactory<DBConfig, DruidDataSource> {

    @Override
    public DruidDataSource createPool(String name) {
        return createPool(ConfigUtils.getConfig().getDbConfigs().get(name));
    }

    @Override
    public DruidDataSource createPool(DBConfig config) {
        try {
            String url = "jdbc:mysql://" +
                    config.getHost() + ":" + config.getPort() + "/" + config.getDatabase() +
                    // 时区
                    "?serverTimezone=GMT%2B8" +
                    // datetime 字段处理
                    "&zeroDateTimeBehavior=convertToNull" +
                    // year
                    "&yearIsDateType=false" +
                    // tinyint(1) 字段处理
                    "&tinyInt1isBit=false" +
                    // 编码
                    "&useUnicode=true" +
                    "&characterEncoding=UTF-8" +
                    "&characterSetResults=UTF-8" +
                    // useSSL
                    "&useSSL=false" +
                    // 重连
                    "&autoReconnect=true" +
                    // 性能优化
                    "&maxAllowedPacket=1073741824" + // 1G
                    "&rewriteBatchedStatements=true";
            DruidDataSource druidDataSource = new DruidDataSource();
            druidDataSource.setUrl(url);
            druidDataSource.setUsername(config.getUser());
            druidDataSource.setPassword(config.getPassword());
            // 配置参数
            druidDataSource.setInitialSize(1);
            druidDataSource.setMinIdle(1);
            druidDataSource.setMaxActive(128);
            druidDataSource.setMaxWait((int) TimeUnit.DAYS.toMillis(7));
            druidDataSource.setTestOnBorrow(false);
            druidDataSource.setTestOnReturn(false);
            druidDataSource.setTestWhileIdle(true);
            // 管理minIdle
            druidDataSource.setTimeBetweenEvictionRunsMillis(TimeUnit.SECONDS.toMillis(30));
            druidDataSource.setMinEvictableIdleTimeMillis(TimeUnit.SECONDS.toMillis(60));
            // minIdle以内的连接保持活跃
            druidDataSource.setKeepAlive(true);
            druidDataSource.setValidationQuery("select 1");
            // 其它
            druidDataSource.setPoolPreparedStatements(true);
            druidDataSource.setMaxOpenPreparedStatements(100);
            druidDataSource.setUsePingMethod(false);
            druidDataSource.setAsyncInit(true);
            // 隔离级别
            druidDataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            // 超时
            druidDataSource.setConnectionInitSqls(Arrays.asList(
                    "set wait_timeout = 3600 * 24 * 7",
                    "set interactive_timeout = 3600 * 24 * 7"
            ));
            druidDataSource.setConnectTimeout((int) TimeUnit.DAYS.toMillis(7));
            druidDataSource.setSocketTimeout((int) TimeUnit.DAYS.toMillis(7));
            druidDataSource.setQueryTimeout((int) TimeUnit.DAYS.toSeconds(7));
            druidDataSource.setTransactionQueryTimeout((int) TimeUnit.DAYS.toSeconds(7));
            log.info("DruidFactory createPool success, config: {}", JsonUtils.toString(config));
            return druidDataSource;
        } catch (Exception e) {
            String msg = "DruidFactory createPool error, config: " + JsonUtils.toString(config);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }
}
