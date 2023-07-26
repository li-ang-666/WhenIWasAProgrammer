package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;

import java.util.concurrent.TimeUnit;

public class DruidTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        new Thread(() -> {
            JdbcTemplate jdbcTemplate1 = new JdbcTemplate("test");
            jdbcTemplate1.queryForObject("select sleep(100)", rs -> rs.getString(1));
        }).start();
        TimeUnit.MINUTES.sleep(2);
    }
}
