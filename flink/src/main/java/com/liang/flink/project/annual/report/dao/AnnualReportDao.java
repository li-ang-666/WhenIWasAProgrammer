package com.liang.flink.project.annual.report.dao;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;

import static com.liang.common.util.SqlUtils.formatValue;

public class AnnualReportDao {
    private final JdbcTemplate prism116 = new JdbcTemplate("116prism");
    private final JdbcTemplate prism464 = new JdbcTemplate("464prism");

    public Tuple3<String, String, String> getCompanyInfoAndReportYearByReportId(String reportId, Map<String, Object> columnMap) {
        String sql = new SQL()
                .SELECT("company_id,report_year")
                .FROM("annual_report")
                .WHERE("id = " + formatValue(reportId))
                .toString();
        Tuple2<String, String> companyCidAndReportYear = prism116.queryForObject(sql, rs -> Tuple2.of(rs.getString(1), rs.getString(2)));
        // 查询report表后做一次判空
        if (companyCidAndReportYear == null) {
            columnMap.put("delete_status", 2);
            return Tuple3.of("-1", String.format("查询report表,id = %s异常", reportId), null);
        }
        sql = new SQL().SELECT("graph_id,name")
                .FROM("enterprise")
                .WHERE("deleted = 0")
                .WHERE("id = " + formatValue(companyCidAndReportYear.f0))
                .toString();
        Tuple2<String, String> companyGidAndName = prism464.queryForObject(sql, rs -> Tuple2.of(rs.getString(1), rs.getString(2)));
        if (companyGidAndName == null) {
            columnMap.put("delete_status", 2);
            return Tuple3.of("-1", String.format("查询enterprise表,id = %s异常", companyCidAndReportYear.f0), null);
        }
        return Tuple3.of(companyGidAndName.f0, companyGidAndName.f1, companyCidAndReportYear.f1);
    }
}
