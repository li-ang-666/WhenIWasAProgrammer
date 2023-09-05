package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;

public class Evaluate extends ConfigHolder {
    public static void main(String[] args) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("150.data_index");
        //jdbcTemplate.queryForList("select * from ")
    }
}
