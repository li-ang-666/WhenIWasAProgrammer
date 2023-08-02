package com.liang.flink.project.annual.report.impl;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportEquityChangeInfo extends AbstractDataUpdate<String> {
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

        Tuple3<String, String, String> info = dao.getInfoAndNameByReportId(reportId);
        resultMap.put("id", id);
        resultMap.put("tyc_unique_entity_id", info.f0);
        resultMap.put("entity_name_valid", info.f1);
        resultMap.put("entity_type_id", 1);
        resultMap.put("annual_report_tyc_unique_entity_id_shareholder", 0);
        resultMap.put("annual_report_entity_name_valid_shareholder", investorName);
        resultMap.put("annual_report_entity_type_id_shareholder", 0);

        resultMap.put("annual_report_equity_ratio_before_change", ratioBefore.endsWith("%") ? ratioBefore : ratioBefore + "%");
        resultMap.put("annual_report_equity_ratio_after_change", ratioAfter.endsWith("%") ? ratioAfter : ratioAfter + "%");
        resultMap.put("annual_report_equity_ratio_change_time", changeTime.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$") ? changeTime : null);
        resultMap.put("is_display_annual_report_equity_ratio_change", 1);
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
        String sql = new SQL()
                .REPLACE_INTO("entity_annual_report_shareholder_equity_change_details")
                .INTO_COLUMNS(insert.f0)
                .INTO_COLUMNS(insert.f1)
                .toString();
        return Collections.singletonList(sql);
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        return Collections.singletonList(dao.delete("report_equity_change_info", columnMap.get("id")));
    }
}
