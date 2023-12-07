package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Test extends ConfigHolder {
    public static void main(String[] args) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("427.test");
        jdbcTemplate.update("create table if not exists ttt(id int)");
        jdbcTemplate.update("select 1", "select 2");
    }
}
