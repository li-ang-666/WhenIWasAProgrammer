package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("435.company_base");
        String sql = "select 1 from company_index where 1=1 order by id";
        jdbcTemplate.streamQuery(sql, rs -> {
        });
        System.out.println(111);
    }
}
