package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) {
        try {
            JdbcTemplate jdbcTemplate = new JdbcTemplate("435.company_base");
            jdbcTemplate.streamQuery(true, "1", rs -> {
            });
        } catch (Exception ignore) {
        }

    }
}
