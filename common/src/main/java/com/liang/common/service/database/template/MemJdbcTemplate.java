package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.service.database.factory.DruidFactory;
import com.liang.common.service.Logging;
import lombok.extern.slf4j.Slf4j;

import static com.liang.common.service.database.factory.DruidFactory.MEMORY_DRUID;

@Slf4j
public class MemJdbcTemplate extends JdbcTemplate {
    private static final DruidDataSource druidDataSource = new DruidFactory().createPool(MEMORY_DRUID);

    public MemJdbcTemplate(String name) {
        super(druidDataSource, new Logging(MemJdbcTemplate.class.getSimpleName(), name));
    }
}
