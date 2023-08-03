package com.liang.flink.project.annual.report.dao;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;

import static com.liang.common.util.SqlUtils.formatValue;

public class AnnualReportDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("116prism");
    private final JdbcTemplate bdpEquity = new JdbcTemplate("bdpEquity");

    public Tuple3<String, String, String> getInfoAndNameByReportId(String reportId, Map<String, Object> columnMap) {
        String sql = new SQL().SELECT("t2.graph_id", "t1.company_name", "t1.report_year")
                .FROM("annual_report t1")
                .JOIN("company_graph t2 on t1.company_id = t2.company_id")
                .WHERE("t2.deleted = 0")
                .WHERE("t1.id = " + formatValue(reportId))
                .toString();
        Tuple3<String, String, String> tuple3 = jdbcTemplate.queryForObject(sql,
                rs -> Tuple3.of(rs.getString(1), rs.getString(2), rs.getString(3)));
        if (tuple3 == null) {
            columnMap.put("delete_status", 2);
            return Tuple3.of("-1", "缺少有效company_gid", null);
        }
        if (SqlUtils.isCompanyId(tuple3.f0)) {
            sql = new SQL().SELECT("entity_name_valid")
                    .FROM("tyc_entity_main_reference")
                    .WHERE("entity_type_id = 1")
                    .WHERE("tyc_unique_entity_id = " + formatValue(tuple3.f0))
                    .toString();
            String name = bdpEquity.queryForObject(sql, rs -> rs.getString(1));
            if (name != null) {
                tuple3.f1 = name;
            }
        }
        return tuple3;
    }
}
