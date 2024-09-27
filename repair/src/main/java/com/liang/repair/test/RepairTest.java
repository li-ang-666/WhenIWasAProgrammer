package com.liang.repair.test;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) {
        long time1 = System.currentTimeMillis();
        JdbcTemplate jdbcTemplate = new JdbcTemplate("104.data_bid");
        long position = 0;
        long count = 0;
        while (count < 10000000L) {
            System.out.println("count: " + count + ", position: " + position);
            String innerSql = new SQL().SELECT("id")
                    .FROM("company_bid")
                    .WHERE("id > " + position)
                    .ORDER_BY("id")
                    .LIMIT(10000)
                    .toString();
            String sql = new SQL().SELECT("max(id)")
                    .FROM("(" + innerSql + ") t")
                    .toString();

            long id = jdbcTemplate.queryForObject(sql, rs -> rs.getLong(1));
            position = id;
            count += 10000;
        }
        long time2 = System.currentTimeMillis();
        System.out.println((time2 - time1) / 1000);
    }
}
