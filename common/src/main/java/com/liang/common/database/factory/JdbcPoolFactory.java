package com.liang.common.database.factory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.liang.common.dto.config.DBConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.Properties;

@Slf4j
public class JdbcPoolFactory {
    private JdbcPoolFactory() {
    }

    @SneakyThrows
    public static DataSource createMySQLConnectionPool(DBConfig mysqlConfig) {
        String url = "jdbc:mysql://" + mysqlConfig.getHost() + ":" + mysqlConfig.getPort() + "/" + mysqlConfig.getDatabase() +
                "?useUnicode=true" +
                "&characterEncoding=utf-8" +
                "&zeroDateTimeBehavior=CONVERT_TO_NULL" +
                "&useSSL=false" +
                "&serverTimezone=GMT%2B8";
        Properties props = new Properties();
        props.put("url", url);
        props.put("username", mysqlConfig.getUser());
        props.put("password", mysqlConfig.getPassword());

        /*---------config---------*/
        props.put("minIdle", "3");
        props.put("maxActive", "5");
        props.put("maxWait", String.valueOf(1000 * 10));
        //回收线程1分钟启动一次
        props.put("timeBetweenEvictionRunsMillis", String.valueOf(1000 * 60));
        //回收idle时长5分钟以上的
        props.put("minEvictableIdleTimeMillis", String.valueOf(1000 * 60 * 5));
        //从池子里拿连接的时候测试一下是不是有效连接
        props.put("testOnBorrow", "true");
        //回收线程启动的时候测试一下是不是有效连接
        props.put("testWhileIdle", "true");
        //还给池子的时候测试一下是不是有效连接
        props.put("testOnReturn", "false");

        log.info("jdbc连接池懒加载, url: {}", url.split("\\?")[0]);
        DataSource dataSource = DruidDataSourceFactory.createDataSource(props);
        ((DruidDataSource) dataSource).setKeepAlive(true);
        ((DruidDataSource) dataSource).setValidationQuery("select 1");
        ((DruidDataSource) dataSource).setPoolPreparedStatements(false);
        return dataSource;
    }
}
