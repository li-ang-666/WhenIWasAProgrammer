package com.liang.common.service.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.service.database.factory.DruidFactory;
import com.liang.common.service.database.template.inner.TemplateLogger;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.liang.common.service.database.factory.DruidFactory.MEMORY_DRUID;

@Slf4j
public class MemJdbcTemplate extends JdbcTemplate {
    private static final DruidDataSource druidDataSource = new DruidFactory().createPool(MEMORY_DRUID);

    public MemJdbcTemplate(String name) {
        super(druidDataSource, new TemplateLogger(MemJdbcTemplate.class.getSimpleName(), name));
    }

    @Override
    public void update(List<String> sqls) {
        if (sqls == null || sqls.isEmpty()) {
            return;
        }
        updateImmediately(sqls);
    }
}
