package com.liang.flink.test;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) throws Exception {
        ConfigUtils.setConfig(ConfigUtils.createConfig(""));
        JdbcTemplate jdbcTemplate = new JdbcTemplate("427.test");
        String sql = new SQL()
                .SELECT("patent_application_year", "patent_publish_year", "patent_type", "patent_status_detail")
                .FROM("company_patent_basic_info_index_split")
                .WHERE("company_id = " + SqlUtils.formatValue(3131283508L))
                .toString();
        AtomicInteger integer = new AtomicInteger(0);
        jdbcTemplate.streamQuery(sql, rs -> {
            if (integer.getAndIncrement() % 1000 == 0) {
                System.out.println(integer);
            }
        });
    }
}
