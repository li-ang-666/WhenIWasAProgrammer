package com.liang.flink.project.annual.report.dao;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;

import static com.liang.common.util.SqlUtils.formatValue;

public class AnnualReportDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("116prism");

    public String delete(String tableName, Object id) {
        return new SQL().DELETE_FROM(tableName)
                .WHERE("id = " + formatValue(id))
                .toString();
    }
}
