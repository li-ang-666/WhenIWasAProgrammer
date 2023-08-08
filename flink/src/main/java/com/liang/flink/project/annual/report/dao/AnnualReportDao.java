package com.liang.flink.project.annual.report.dao;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.TycUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;

import static com.liang.common.util.SqlUtils.formatValue;

public class AnnualReportDao {
    private final JdbcTemplate prism116 = new JdbcTemplate("116prism");
    private final JdbcTemplate prism464 = new JdbcTemplate("464prism");

    public Tuple3<String, String, String> getCompanyInfoAndReportYearByReportId(String reportId, Map<String, Object> columnMap) {
        // 查询report表
        String sql = new SQL()
                .SELECT("company_id,report_year")
                .FROM("annual_report")
                .WHERE("id = " + formatValue(reportId))
                .toString();
        Tuple2<String, String> companyCidAndReportYear = prism116.queryForObject(sql, rs -> Tuple2.of(rs.getString(1), rs.getString(2)));
        if (companyCidAndReportYear == null || !TycUtils.isUnsignedId(companyCidAndReportYear.f0) || !String.valueOf(companyCidAndReportYear.f1).matches("\\d{4}")) {
            columnMap.put("delete_status", 2);
            return Tuple3.of("-1", String.format("%s:report_id", reportId), null);
        }
        // 查询enterprise表
        Tuple2<String, String> companyGidAndName = TycUtils.companyCid2GidAndName(companyCidAndReportYear.f0);
        if (!TycUtils.isUnsignedId(companyGidAndName.f0) || !TycUtils.isValidName(companyGidAndName.f1)) {
            columnMap.put("delete_status", 2);
            return Tuple3.of("-1", String.format("%s:company_cid ", companyCidAndReportYear.f0), companyCidAndReportYear.f1);
        }
        return Tuple3.of(companyGidAndName.f0, companyGidAndName.f1, companyCidAndReportYear.f1);
    }
}
