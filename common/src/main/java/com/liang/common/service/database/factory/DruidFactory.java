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
    public static final String MEMORY_DRUID = "mem";

    @Override
    public DruidDataSource createPool(String name) {
        try {
            boolean isMem = MEMORY_DRUID.equals(name);
            DruidDataSource druidDataSource = isMem ? createMem() : createNormal(name);
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
                "&maxAllowedPacket=67108864" + // 64MB
                "&rewriteBatchedStatements=true";
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(dbConfig.getUser());
        druidDataSource.setPassword(dbConfig.getPassword());
        return druidDataSource;
    }

    private DruidDataSource createMem() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setDriverClassName("org.h2.Driver");
        druidDataSource.setUrl("jdbc:h2:mem:db" +
                ";MODE=MySQL" +
                ";DATABASE_TO_LOWER=TRUE" +
                ";CASE_INSENSITIVE_IDENTIFIERS=TRUE" +
                ";IGNORECASE=TRUE" +
                ";AUTO_RECONNECT=TRUE" +
                ";DB_CLOSE_ON_EXIT=FALSE" +
                ";DB_CLOSE_DELAY=-1"
        );
        return druidDataSource;
    }

    private void configDruid(DruidDataSource druidDataSource) {
        druidDataSource.setInitialSize(1);
        druidDataSource.setMinIdle(1);
        druidDataSource.setMaxActive(10);
        druidDataSource.setMaxWait(1000 * 60 * 2);
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
        druidDataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_NONE);
        // 设置sql超时, 避免断电未提交的事务导致其它sql lock wait timeout
        List<String> initSqls = Arrays.asList(
                "set wait_timeout = 120", "set interactive_timeout = 120");
        druidDataSource.setConnectionInitSqls(initSqls);
        druidDataSource.setConnectTimeout(1000 * 60 * 2);
        druidDataSource.setSocketTimeout(1000 * 60 * 2);
        druidDataSource.setQueryTimeout(120);
        druidDataSource.setTransactionQueryTimeout(120);
    }
}
