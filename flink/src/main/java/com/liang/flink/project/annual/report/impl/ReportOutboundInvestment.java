package com.liang.flink.project.annual.report.impl;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

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
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annual_report_id"));
        String outcompanyId = String.valueOf(columnMap.get("outcompany_id"));
        String outCompanyName = String.valueOf(columnMap.get("outcompany_name"));

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("id", id);
        Tuple3<String, String, String> info = dao.getCompanyAndYear(reportId, resultMap);
        // 公司
        resultMap.put("tyc_unique_entity_id", info.f0);
        resultMap.put("entity_name_valid", info.f1);
        resultMap.put("entity_type_id", 1);
        // 投资对象
        resultMap.put("annual_report_year", info.f2);
        resultMap.put("annual_report_company_id_invested", TycUtils.companyCid2GidAndName(outcompanyId).f0);
        resultMap.put("annual_report_company_name_invested", outCompanyName);
        String fixedInvestCompanyId = String.valueOf(resultMap.get("annual_report_company_id_invested"));
        String fixedInvestCompanyName = String.valueOf(resultMap.get("annual_report_company_name_invested"));
        if (!TycUtils.isTycUniqueEntityId(fixedInvestCompanyId) || !TycUtils.isValidName(fixedInvestCompanyName)) {
            resultMap.put("delete_status", 2);
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
}
