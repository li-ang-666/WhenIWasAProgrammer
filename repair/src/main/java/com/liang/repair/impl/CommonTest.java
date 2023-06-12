package com.liang.repair.impl;

import com.liang.common.service.database.template.MemJdbcTemplate;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class CommonTest implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        MemJdbcTemplate jdbcTemplate = new MemJdbcTemplate("aaaaa");
        jdbcTemplate.update("create table test(`id` varchar(255), `visit_date` varchar(255), `visit_page_name` varchar(255), `visit_secondary_dim_name` varchar(255), `visit_count` varchar(255), `create_time` varchar(255), `update_time` varchar(255))");
        jdbcTemplate.update("insert into test(`id`, `visit_date`, `visit_page_name`, `visit_secondary_dim_name`, `visit_count`, `create_time`, `update_time`) values('47427300', \"2023-06-05\", \"公司详情页\", \"最终受益人\", \"24686\", \"2023-06-05 00:00:30\", \"2023-06-05 11:34:00\")");
    }
}
