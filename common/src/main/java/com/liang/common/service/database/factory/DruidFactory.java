package com.liang.common.service.database.factory;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.dto.config.DBConfig;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
public class DruidFactory implements IFactory<DruidDataSource> {

    @Override
    public DruidDataSource createPool(String name) {
        DruidDataSource druidDataSource = "mem".equals(name) ? createMem() : createNormal(name);
        configDruid(druidDataSource);
        if ("mem".equals(name)) {
            druidDataSource.setValidationQuery("select 1 from INFORMATION_SCHEMA.SYSTEM_USERS");
        }
        log.info("druid 加载: {}", name);
        return druidDataSource;
    }

    private DruidDataSource createNormal(String name) {
        DBConfig dbConfig = ConfigUtils.getConfig().getDbConfigs().get(name);
        String url = "jdbc:mysql://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/" + dbConfig.getDatabase() +
                //时区
                "?serverTimezone=GMT%2B8" +
                //时间字段处理
                "&zeroDateTimeBehavior=convertToNull" +
                //编码
                "&useUnicode=true" +
                "&characterEncoding=UTF-8" +
                "&characterSetResults=UTF-8" +
                //useSSL
                "&useSSL=false" +
                //连接策略
                "&autoReconnect=true" +
                "&maxReconnects=3" +
                "&failOverReadOnly=false" +
                "&initialTimeout=3" +
                "&socketTimeout=30000" +
                "&connectTimeout=30000" +
                //性能优化
                "&allowMultiQueries=true" +
                "&rewriteBatchedStatements=true";
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(dbConfig.getUser());
        druidDataSource.setPassword(dbConfig.getPassword());
        return druidDataSource;
    }

    private DruidDataSource createMem() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setDriverClassName("org.hsqldb.jdbcDriver");
        druidDataSource.setUrl("jdbc:hsqldb:mem:db");
        druidDataSource.setUsername("李昂");
        druidDataSource.setPassword("牛逼");
        return druidDataSource;
    }

    private void configDruid(DruidDataSource druidDataSource) {
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
        //定期对idle线程进行探活
        druidDataSource.setKeepAlive(true);
        druidDataSource.setKeepAliveBetweenTimeMillis(1000 * 30);
        druidDataSource.setValidationQuery("select 1");
        druidDataSource.setValidationQueryTimeout(5);
        druidDataSource.setAsyncInit(true);
        druidDataSource.setPoolPreparedStatements(true);
        druidDataSource.setMaxOpenPreparedStatements(100);
        druidDataSource.setConnectProperties(new Properties() {{
            put("druid.mysql.usePingMethod", "false");
        }});
    }
}
