package com.liang.flink.project.annual.report.dao;

import com.liang.common.dto.tyc.Company;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.TycUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static com.liang.common.util.SqlUtils.formatValue;

public class AnnualReportDao {
    private final JdbcTemplate prism116 = new JdbcTemplate("116prism");

    public Tuple2<Company, String> getCompanyAndYear(String reportId) {
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
        // 防止空指针
        if (companyCidAndYear == null) {
            return Tuple2.of(new Company(), null);
        }
        String companyCid = companyCidAndYear.f0;
        String year = companyCidAndYear.f1;
        // 查询enterprise表
        return Tuple2.of(TycUtils.cid2Company(companyCid), year);
    }

    public void updateSource(String companyCid) {
        String querySql = new SQL()
                .SELECT("id")
                .FROM("annual_report")
                .WHERE("company_id = " + formatValue(companyCid))
                .toString();
        List<String> reportIds = prism116.queryForList(querySql, rs -> rs.getString(1));
        List<String> sqls = new ArrayList<>();
        // 四张表
        if (!reportIds.isEmpty()) {
            String strReportIds = "(" + String.join(",", reportIds) + ")";
            String sql1 = new SQL()
                    .UPDATE("report_equity_change_info")
                    .SET("createTime = date_add(createTime, interval 1 second)")
                    .WHERE("annualreport_id in " + strReportIds)
                    .toString();
            String sql2 = new SQL()
                    .UPDATE("report_shareholder")
                    .SET("createTime = date_add(createTime, interval 1 second)")
                    .WHERE("annual_report_id in " + strReportIds)
                    .toString();
            String sql3 = new SQL()
                    .UPDATE("report_outbound_investment")
                    .SET("createTime = date_add(createTime, interval 1 second)")
                    .WHERE("annual_report_id in " + strReportIds)
                    .toString();
            String sql4 = new SQL()
                    .UPDATE("report_webinfo")
                    .SET("createTime = date_add(createTime, interval 1 second)")
                    .WHERE("annualreport_id in " + strReportIds)
                    .toString();
            sqls.add(sql1);
            sqls.add(sql2);
            sqls.add(sql3);
            sqls.add(sql4);
        }
        // 三张有股东或者对外投资的表
        String sql1 = new SQL()
                .UPDATE("report_equity_change_info")
                .SET("createTime = date_add(createTime, interval 1 second)")
                .WHERE("investor_id = 2")
                .WHERE("investor_id = " + formatValue(companyCid))
                .toString();
        String sql2 = new SQL()
                .UPDATE("report_shareholder")
                .SET("createTime = date_add(createTime, interval 1 second)")
                .WHERE("investor_id = 2")
                .WHERE("investor_id = " + formatValue(companyCid))
                .toString();
        String sql3 = new SQL()
                .UPDATE("report_outbound_investment")
                .SET("createTime = date_add(createTime, interval 1 second)")
                .WHERE("outcompany_id = " + formatValue(companyCid))
                .toString();
        sqls.add(sql1);
        sqls.add(sql2);
        sqls.add(sql3);
        prism116.update(sqls);
    }
}
