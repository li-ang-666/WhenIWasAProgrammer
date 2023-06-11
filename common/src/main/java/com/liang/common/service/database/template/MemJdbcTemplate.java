package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.service.database.factory.DruidFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemJdbcTemplate extends JdbcTemplate {
    private static final DruidDataSource druidDataSource = new DruidFactory().createPool("mem");

    public MemJdbcTemplate(String name) {
        super(druidDataSource, new TemplateLogger(MemJdbcTemplate.class.getSimpleName(), name));
    }

    public static void main(String[] args) {
        System.out.println(111111111);
    }
}
