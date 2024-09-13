package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("435.company_base");
        String sql = "select company_id from company_index limit 1";
        jdbcTemplate.queryForObject(sql, rs -> {
            System.out.println(rs.getMetaData());
            System.out.println(rs.getMetaData().getCatalogName(1));
            System.out.println(rs.getMetaData().getTableName(1));
            return "";
        });
    }
}
