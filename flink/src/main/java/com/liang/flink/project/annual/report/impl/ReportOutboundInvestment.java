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

public class ReportOutboundInvestment extends AbstractDataUpdate<String> {
    private final static String TABLE_NAME = "entity_annual_report_investment_details";
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        // 公司
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annual_report_id"));
        // 对外投资
        String outcompanyId = String.valueOf(columnMap.get("outcompany_id"));
        String outCompanyName = String.valueOf(columnMap.get("outcompany_name"));
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
        // 投资对象
        Company outCompany = TycUtils.cid2Company(outcompanyId);
        resultMap.put("annual_report_company_id_invested", outCompany.getGid());
        resultMap.put("annual_report_company_name_invested", outCompany.getName());
        resultMap.put("annual_report_company_name_invested_register_name", outCompanyName);
        // 检测脏数据
        checkMap(resultMap);
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
                TycUtils.isTycUniqueEntityId(resultMap.get("annual_report_company_id_invested")) &&
                TycUtils.isValidName(resultMap.get("annual_report_company_name_invested")) &&
                TycUtils.isValidName(resultMap.get("annual_report_company_name_invested_register_name")) &&
                String.valueOf(resultMap.get("annual_report_year")).matches("\\d{4}")
        ) {
        } else {
            resultMap.put("delete_status", 1);
        }
    }
}
