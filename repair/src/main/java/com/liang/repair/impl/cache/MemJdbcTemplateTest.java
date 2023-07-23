package com.liang.repair.impl.cache;

import com.liang.common.service.database.template.MemJdbcTemplate;
import com.liang.repair.service.ConfigHolder;

public class MemJdbcTemplateTest extends ConfigHolder {
    public static void main(String[] args) {
        MemJdbcTemplate jdbcTemplate = new MemJdbcTemplate("aaa");
        String s = jdbcTemplate.queryForObject("select now()", rs -> rs.getString(1));
        log.info("s: {}", s);
    }
}
