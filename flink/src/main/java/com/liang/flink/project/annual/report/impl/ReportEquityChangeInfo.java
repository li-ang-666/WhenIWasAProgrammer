package com.liang.flink.project.annual.report.impl;

import com.liang.common.service.SQL;
import com.liang.common.util.DateTimeUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportEquityChangeInfo extends AbstractDataUpdate<String> {
    private final static String TABLE_NAME = "entity_annual_report_shareholder_equity_change_details";
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        Map<String, Object> resultMap = new HashMap<>();
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annualreport_id"));
        String investorName = String.valueOf(columnMap.get("investor_name"));
        String ratioBefore = String.valueOf(columnMap.get("ratio_before"));
        String ratioAfter = String.valueOf(columnMap.get("ratio_after"));
        String changeTime = String.valueOf(columnMap.get("change_time"));

        Tuple3<String, String, String> info = dao.getCompanyInfoAndReportYearByReportId(reportId, resultMap);
        resultMap.put("id", id);
        //
        resultMap.put("tyc_unique_entity_id", info.f0);
        resultMap.put("entity_name_valid", info.f1);
        resultMap.put("entity_type_id", 1);
        //
        resultMap.put("annual_report_tyc_unique_entity_id_shareholder", -1);
        resultMap.put("annual_report_entity_name_valid_shareholder", investorName);
        resultMap.put("annual_report_entity_type_id_shareholder", -1);
        //
        resultMap.put("annual_report_equity_ratio_before_change", parse(ratioBefore));
        resultMap.put("annual_report_equity_ratio_after_change", parse(ratioAfter));
        resultMap.put("annual_report_equity_ratio_change_time", DateTimeUtils.isDateTime(changeTime) ? changeTime : null);
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
        String sql = new SQL()
                .REPLACE_INTO(TABLE_NAME)
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
        return Collections.singletonList(sql);
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        String sql = new SQL().DELETE_FROM(TABLE_NAME)
                .WHERE("id = " + SqlUtils.formatValue(singleCanalBinlog.getColumnMap().get("id")))
                .toString();
        return Collections.singletonList(sql);
    }

    private String parse(String number) {
        String replaced = number.replaceAll("%|\\s", "");
        try {
            return new BigDecimal(replaced)
                    .divide(new BigDecimal(100), RoundingMode.DOWN)
                    .setScale(12, RoundingMode.DOWN)
                    .toPlainString();
        } catch (Exception e) {
            return new BigDecimal("0")
                    .setScale(12, RoundingMode.DOWN)
                    .toPlainString();
        }
    }
}
