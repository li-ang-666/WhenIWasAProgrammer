package com.liang.common.service.database.factory;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.dto.config.DBConfig;
import com.liang.common.util.ConfigUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DruidFactory {
    private DruidFactory() {
    }

    @SneakyThrows
    public static DruidDataSource create(String name) {
        DBConfig dbConfig = ConfigUtils.getConfig().getDbConfigs().get(name);
        String url = "jdbc:mysql://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/" + dbConfig.getDatabase() +
                "?serverTimezone=GMT%2B8" +
                "&zeroDateTimeBehavior=convertToNull" +
                "&useUnicode=true" +
                "&characterEncoding=UTF-8" +
                "&characterSetResults=UTF-8" +
                "&useSSL=false" +
                "&autoReconnect=true" +
                "&failOverReadOnly=false" +
                "&initialTimeout=2" +
                "&socketTimeout=30000" +
                "&connectTimeout=30000" +
                "&rewriteBatchedStatements=true";
        DruidDataSource druidDataSource = new DruidDataSource();

        druidDataSource.setUrl(url);
        druidDataSource.setUsername(dbConfig.getUser());
        druidDataSource.setPassword(dbConfig.getPassword());
        druidDataSource.setInitialSize(1);
        druidDataSource.setMinIdle(1);
        druidDataSource.setMaxActive(10);
        druidDataSource.setMaxWait(5000);
        druidDataSource.setUseUnfairLock(true);
        druidDataSource.setTestOnBorrow(false);
        druidDataSource.setTestOnReturn(false);
        druidDataSource.setTestWhileIdle(true);
        //超时的视为idle连接,如果数量大于minIdle,下次清除线程会将其清除
        druidDataSource.setMinEvictableIdleTimeMillis(1000 * 60 * 5);
        //超时的视为idle连接,不管minIdle,下次清除线程都会将其清除
        druidDataSource.setMaxEvictableIdleTimeMillis(1000 * 60 * 10);
        //清除线程运行间隔
        druidDataSource.setTimeBetweenEvictionRunsMillis(1000 * 60);
        druidDataSource.setKeepAlive(true);
        druidDataSource.setKeepAliveBetweenTimeMillis(1000 * 30);
        druidDataSource.setValidationQuery("select 1 from DUAL");
        druidDataSource.setValidationQueryTimeout(5);
        druidDataSource.setAsyncInit(true);
        druidDataSource.setPoolPreparedStatements(true);
        druidDataSource.setMaxOpenPreparedStatements(100);

        log.info("jdbc连接池加载, url: {}", url.split("\\?")[0]);
        return druidDataSource;
    }
}
