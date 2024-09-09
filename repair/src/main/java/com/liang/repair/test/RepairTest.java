package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("435.company_base");
        jdbcTemplate.streamQuery("select * from company_index where create_time >= '2024-09-09 00:00:00'", rs -> {
            System.out.println(rs.getString("id"));
        });
    }
}
