package com.liang.common.service.database.factory;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.dto.config.DBConfig;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DruidFactory implements IFactory<DruidDataSource> {

    @Override
    public DruidDataSource createPool(String name) {
        try {
            DruidDataSource druidDataSource = createNormal(name);
            configDruid(druidDataSource);
            log.info("druid 加载: {}", name);
            return druidDataSource;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DruidDataSource createNormal(String name) {
        DBConfig dbConfig = ConfigUtils.getConfig().getDbConfigs().get(name);
        String url = "jdbc:mysql://" +
                dbConfig.getHost() + ":" + dbConfig.getPort() + "/" + dbConfig.getDatabase() +
                // 时区
                "?serverTimezone=GMT%2B8" +
                // datetime 字段处理
                "&zeroDateTimeBehavior=convertToNull" +
                // tinyint(1) 字段处理
                "&tinyInt1isBit=false" +
                // 编码
                "&useUnicode=true" +
                "&characterEncoding=UTF-8" +
                "&characterSetResults=UTF-8" +
                // useSSL
                "&useSSL=false" +
                // 性能优化
                "&maxAllowedPacket=1073741824" + // 1G
                "&rewriteBatchedStatements=true";
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(dbConfig.getUser());
        druidDataSource.setPassword(dbConfig.getPassword());
        return druidDataSource;
    }

    private void configDruid(DruidDataSource druidDataSource) {
        druidDataSource.setInitialSize(1);
        druidDataSource.setMinIdle(1);
        druidDataSource.setMaxActive(16);
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
        druidDataSource.setConnectTimeout((int) TimeUnit.DAYS.toMillis(7));
        druidDataSource.setSocketTimeout((int) TimeUnit.DAYS.toMillis(7));
        druidDataSource.setQueryTimeout((int) TimeUnit.DAYS.toSeconds(7));
        druidDataSource.setTransactionQueryTimeout((int) TimeUnit.DAYS.toSeconds(7));
    }
}
