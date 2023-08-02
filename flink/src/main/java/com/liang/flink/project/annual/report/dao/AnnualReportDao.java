package com.liang.flink.project.annual.report.dao;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import org.apache.flink.api.java.tuple.Tuple3;

import static com.liang.common.util.SqlUtils.formatValue;

public class AnnualReportDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("116prism");

    public String delete(String tableName, Object id) {
        return new SQL().DELETE_FROM(tableName)
                .WHERE("id = " + formatValue(id))
                .toString();
    }

    public Tuple3<String, String, String> getInfoAndNameByReportId(String reportId) {
        String sql = new SQL().SELECT("t2.graph_id", "t1.company_name", "t1.report_year")
                .FROM("annual_report t1")
                .JOIN("company_graph t2 on t1.company_id = t2.company_id")
                .WHERE("t2.deleted = 0")
                .WHERE("t1.id = " + formatValue(reportId))
                .toString();
        return jdbcTemplate.queryForObject(sql,
                rs -> Tuple3.of(rs.getString(1), rs.getString(2), rs.getString(3)));
    }
}
