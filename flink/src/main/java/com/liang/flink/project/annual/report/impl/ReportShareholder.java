package com.liang.flink.project.annual.report.impl;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.*;

public class ReportShareholder extends AbstractDataUpdate<String> {
    private final static String TABLE_NAME = "entity_annual_report_shareholder_equity_details";
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        List<String> result = new ArrayList<>(deleteWithReturn(singleCanalBinlog));
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annual_report_id"));
        String shareholderName = String.valueOf(columnMap.get("investor_name"));
        String investorId = String.valueOf(columnMap.get("investor_id"));
        String investorType = String.valueOf(columnMap.get("investor_type"));
        String subscribeAmount = String.valueOf(columnMap.get("subscribe_amount"));
        String subscribeTime = String.valueOf(columnMap.get("subscribe_time"));
        String subscribeType = String.valueOf(columnMap.get("subscribe_type"));
        String paidAmount = String.valueOf(columnMap.get("paid_amount"));
        String paidTime = String.valueOf(columnMap.get("paid_time"));
        String paidType = String.valueOf(columnMap.get("paid_type"));

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("business_id", id);
        Tuple3<String, String, String> info = dao.getCompanyInfoAndReportYearByReportId(reportId, resultMap);
        // 公司
        resultMap.put("tyc_unique_entity_id", info.f0);
        resultMap.put("entity_name_valid", info.f1);
        resultMap.put("entity_type_id", 1);
        // 股东(report_shareholder的人、公司枚举是反着的)
        resultMap.put("annual_report_entity_name_valid_shareholder", shareholderName);
        switch (investorType) {
            case "1": // 人
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", TycUtils.getHumanHashId(info.f0, TycUtils.humanCid2Gid(investorId)));
                resultMap.put("annual_report_entity_type_id_shareholder", 2);
                break;
            case "2": // 公司
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", TycUtils.companyCid2GidAndName(investorId).f0);
                resultMap.put("annual_report_entity_type_id_shareholder", 1);
                break;
            case "3": // 其它
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", investorId);
                resultMap.put("annual_report_entity_type_id_shareholder", 3);
                break;
            default:
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", investorId);
                resultMap.put("annual_report_entity_type_id_shareholder", 0);
                break;
        }
        String fixedShareholderId = String.valueOf(resultMap.get("annual_report_tyc_unique_entity_id_shareholder"));
        String fixedShareholderName = String.valueOf(resultMap.get("annual_report_entity_name_valid_shareholder"));
        if (!TycUtils.isTycUniqueEntityId(fixedShareholderId) || !TycUtils.isTycUniqueEntityName(fixedShareholderName)) {
            resultMap.put("delete_status", 2);
        }
        resultMap.put("annual_report_year", info.f2);
        // 认缴
        HashMap<String, Object> resultMap1 = new HashMap<>(resultMap);
        resultMap1.put("annual_report_shareholder_capital_type", 1);
        resultMap1.put("annual_report_shareholder_capital_source", subscribeAmount);
        Tuple2<String, String> numberAndUnit1 = TycUtils.formatEquity(subscribeAmount);
        resultMap1.put("annual_report_shareholder_equity_amt", numberAndUnit1.f0);
        resultMap1.put("annual_report_shareholder_equity_currency", numberAndUnit1.f1);
        resultMap1.put("annual_report_shareholder_equity_valid_date", TycUtils.isDateTime(subscribeTime) ? subscribeTime : null);
        resultMap1.put("annual_report_shareholder_equity_submission_method", subscribeType);
        Tuple2<String, String> insert1 = SqlUtils.columnMap2Insert(resultMap1);
        String sql1 = new SQL().REPLACE_INTO(TABLE_NAME)
                .INTO_COLUMNS(insert1.f0)
                .INTO_VALUES(insert1.f1)
                .toString();
        result.add(sql1);
        // 实缴
        HashMap<String, Object> resultMap2 = new HashMap<>(resultMap);
        resultMap2.put("annual_report_shareholder_capital_type", 2);
        resultMap2.put("annual_report_shareholder_capital_source", paidAmount);
        Tuple2<String, String> numberAndUnit2 = TycUtils.formatEquity(paidAmount);
        resultMap2.put("annual_report_shareholder_equity_amt", numberAndUnit2.f0);
        resultMap2.put("annual_report_shareholder_equity_currency", numberAndUnit2.f1);
        resultMap2.put("annual_report_shareholder_equity_valid_date", TycUtils.isDateTime(paidTime) ? paidTime : null);
        resultMap2.put("annual_report_shareholder_equity_submission_method", paidType);
        Tuple2<String, String> insert2 = SqlUtils.columnMap2Insert(resultMap2);
        String sql2 = new SQL().REPLACE_INTO(TABLE_NAME)
                .INTO_COLUMNS(insert2.f0)
                .INTO_VALUES(insert2.f1)
                .toString();
        result.add(sql2);
        return result;
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        String sql = new SQL().DELETE_FROM(TABLE_NAME)
                .WHERE("business_id = " + SqlUtils.formatValue(singleCanalBinlog.getColumnMap().get("id")))
                .toString();
        return Collections.singletonList(sql);
    }
}
