package com.liang.flink.project.annual.report.impl;

import com.liang.common.dto.tyc.Company;
import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
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
        if (!TycUtils.isYear(year)) {
            year = null;
        }
        if (!TycUtils.isUnsignedId(company.getGid()) || !TycUtils.isValidName(company.getName())) {
            log.error("report_webinfo, id: {}, 路由不到正确实体", id);
            return deleteWithReturn(singleCanalBinlog);
        }
        // 公司
        resultMap.put("id", id);
        resultMap.put("annual_report_year", year);
        resultMap.put("tyc_unique_entity_id", company.getGid());
        resultMap.put("entity_name_valid", company.getName());
        resultMap.put("entity_type_id", 1);
        // 网上运营
        resultMap.put("annual_report_ebusiness_type", TycUtils.isValidName(webType) ? webType : null);
        resultMap.put("annual_report_ebusiness_name", TycUtils.isValidName(name) ? name : null);
        resultMap.put("annual_report_ebusiness_website", TycUtils.isValidName(website) ? website : null);
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
