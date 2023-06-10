package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;

public class MemJdbcTemplate extends JdbcTemplate {
    private final static DruidDataSource druidDataSource;

    static {
        druidDataSource = new DruidDataSource();
        druidDataSource.setTestWhileIdle(false);
        druidDataSource.setDriverClassName("org.hsqldb.jdbcDriver");
        druidDataSource.setUrl("jdbc:hsqldb:mem:db");
        druidDataSource.setUsername("李昂");
        druidDataSource.setPassword("牛逼");
    }

    public MemJdbcTemplate(String name) {
        super(druidDataSource, new TemplateLogger(MemJdbcTemplate.class.getSimpleName(), name));
    }
}
