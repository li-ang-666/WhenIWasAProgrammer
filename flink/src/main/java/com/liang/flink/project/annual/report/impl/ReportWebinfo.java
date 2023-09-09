package com.liang.flink.project.annual.report.impl;

import com.liang.common.dto.tyc.Company;
import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.flink.api.java.tuple.Tuple2;

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
        // 公司
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annualreport_id"));
        // 网上运营
        String webType = String.valueOf(columnMap.get("web_type"));
        String name = String.valueOf(columnMap.get("name"));
        String website = String.valueOf(columnMap.get("website"));
        // 开始解析
        Map<String, Object> resultMap = new HashMap<>();
        Tuple2<Company, String> companyAndYear = dao.getCompanyAndYear(reportId);
        Company company = companyAndYear.f0;
        String year = companyAndYear.f1;
        // 公司
        resultMap.put("id", id);
        resultMap.put("annual_report_year", year);
        resultMap.put("tyc_unique_entity_id", company.getGid());
        resultMap.put("entity_name_valid", company.getName());
        resultMap.put("entity_type_id", 1);
        // 网上运营
        switch (webType) {
            case "网站":
                resultMap.put("annual_report_ebusiness_type", "网站");
                break;
            case "网店":
                resultMap.put("annual_report_ebusiness_type", "网店");
                break;
            case "网页":
                resultMap.put("annual_report_ebusiness_type", "网页");
                break;
            default:
                resultMap.put("annual_report_ebusiness_type", "其它");
                resultMap.put("delete_status", 1);
                break;
        }
        resultMap.put("annual_report_ebusiness_name", TycUtils.isValidName(name) ? name : "");
        resultMap.put("annual_report_ebusiness_website", TycUtils.isValidName(website) ? website : "");
        // 两个都是空, 代表脏数据
        if (!TycUtils.isValidName(name) && !TycUtils.isValidName(website)) {
            resultMap.put("delete_status", 1);
        }
        // 检测脏数据
        checkMap(resultMap);
        if ("1".equals(String.valueOf(resultMap.get("delete_status")))) {
            return deleteWithReturn(singleCanalBinlog);
        }
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

    private void checkMap(Map<String, Object> resultMap) {
        if (TycUtils.isTycUniqueEntityId(resultMap.get("tyc_unique_entity_id")) &&
                TycUtils.isValidName(resultMap.get("entity_name_valid")) &&
                TycUtils.isYear(resultMap.get("annual_report_year"))
        ) {
        } else {
            resultMap.put("delete_status", 1);
        }
    }
}
