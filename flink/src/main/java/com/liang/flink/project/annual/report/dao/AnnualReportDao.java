package com.liang.flink.project.annual.report.dao;

import com.liang.common.dto.tyc.Company;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.TycUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;

import static com.liang.common.util.SqlUtils.formatValue;

public class AnnualReportDao {
    private final JdbcTemplate prism116 = new JdbcTemplate("116prism");
    private final JdbcTemplate prism464 = new JdbcTemplate("464prism");

    public Tuple2<Company, String> getCompanyAndYear(String reportId, Map<String, Object> columnMap) {
        if (!TycUtils.isUnsignedId(reportId)) {
            return Tuple2.of(new Company(), null);
        }
        // 查询report表
        String sql = new SQL()
                .SELECT("company_id", "report_year")
                .FROM("annual_report")
                .WHERE("id = " + formatValue(reportId))
                .toString();
        Tuple2<String, String> companyCidAndYear = prism116.queryForObject(sql,
                rs -> Tuple2.of(rs.getString(1), rs.getString(2)));
        if (companyCidAndYear == null) {
            columnMap.put("delete_status", 2);
            return Tuple2.of(new Company(), null);
        }
        String companyCid = companyCidAndYear.f0;
        String year = companyCidAndYear.f1;
        if (!TycUtils.isUnsignedId(companyCid) || !String.valueOf(year).matches("\\d{4}")) {
            columnMap.put("delete_status", 2);
            return Tuple2.of(new Company(), null);
        }
        // 查询enterprise表
        Company company = TycUtils.cid2Company(companyCid);
        return Tuple2.of(company, year);
    }
}
