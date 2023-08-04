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

public class ReportWebinfo extends AbstractDataUpdate<String> {
    private final static String TABLE_NAME = "entity_annual_report_ebusiness_details";
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annualreport_id"));
        String webType = String.valueOf(columnMap.get("web_type"));
        String name = String.valueOf(columnMap.get("name"));
        String website = String.valueOf(columnMap.get("website"));

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("id", id);
        Tuple3<String, String, String> info = dao.getCompanyInfoAndReportYearByReportId(reportId, resultMap);
        //
        resultMap.put("tyc_unique_entity_id", info.f0);
        resultMap.put("entity_name_valid", info.f1);
        resultMap.put("entity_type_id", 1);
        //
        resultMap.put("annual_report_year", info.f2);
        switch (webType) {
            case "网站":
                resultMap.put("annual_report_ebusiness_type", 1);
                break;
            case "网店":
                resultMap.put("annual_report_ebusiness_type", 2);
                break;
            case "网页":
                resultMap.put("annual_report_ebusiness_type", 3);
                break;
            default:
                resultMap.put("annual_report_ebusiness_type", 4);
                break;
        }
        resultMap.put("annual_report_ebusiness_name", name);
        resultMap.put("annual_report_ebusiness_website", website);
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
        String sql = new SQL().REPLACE_INTO(TABLE_NAME)
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
}
