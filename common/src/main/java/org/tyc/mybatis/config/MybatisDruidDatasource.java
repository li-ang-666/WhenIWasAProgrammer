package org.tyc.mybatis.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;


public class MybatisDruidDatasource implements DataSourceFactory {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    private Properties properties;

    @Override
    public DataSource getDataSource() {
        //创建druid数据源,这是druid jar包提供的一个类
        DruidDataSource ds = new DruidDataSource();
        //从配置好的properties加载配置
        ds.setUsername(this.properties.getProperty("username"));//用户名
        ds.setPassword(this.properties.getProperty("password"));//密码
        ds.setUrl(this.properties.getProperty("url") + "?serverTimezone=GMT%2B8&rewriteBatchedStatements=true&autoReconnect=true&zeroDateTimeBehavior=convertToNull&autoReconnect=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true&socketTimeout=60000&connectTimeout=60000&useSSL=false");
        ds.setDriverClassName(this.properties.getProperty("driver"));
        ds.setInitialSize(3);//初始连接数
        ds.setMaxActive(6);//最大活动连接数
        ds.setMaxWait(6000);//最大等待时间

        //初始化连接
        try {
            ds.init();
            logger.info("init success instance:{},name:{},schema:{}", this.properties.getProperty("url"), this.properties.getProperty("url_name"), this.properties.getProperty("schema"));
        } catch (SQLException e) {
            logger.error(LogUtil.toStackTrace(e));
        }
        return ds;
    }

    @Override
    public void setProperties(Properties properties) {
        // xml文档会将properties注入进来
        this.properties = properties;
    }

}