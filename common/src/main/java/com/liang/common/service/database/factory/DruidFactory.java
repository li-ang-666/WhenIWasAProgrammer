package com.liang.common.service.database.factory;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.dto.config.DBConfig;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

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
                "&useCursorFetch=true" +
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
        druidDataSource.setMaxActive(10);
        druidDataSource.setMaxWait(1000 * 60 * 5);
        druidDataSource.setTestOnBorrow(false);
        druidDataSource.setTestOnReturn(false);
        druidDataSource.setTestWhileIdle(true);
        // 管理minIdle
        druidDataSource.setTimeBetweenEvictionRunsMillis(1000 * 30);
        druidDataSource.setMinEvictableIdleTimeMillis(1000 * 60);
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
        // 设置sql超时, 避免断电未提交的事务导致其它sql lock wait timeout
        List<String> initSqls = Arrays.asList(
                "set wait_timeout = 1800", "set interactive_timeout = 1800");
        druidDataSource.setConnectionInitSqls(initSqls);
        druidDataSource.setConnectTimeout(1000 * 1800);
        druidDataSource.setSocketTimeout(1000 * 1800);
        druidDataSource.setQueryTimeout(1800);
        druidDataSource.setTransactionQueryTimeout(1800);
    }
}
