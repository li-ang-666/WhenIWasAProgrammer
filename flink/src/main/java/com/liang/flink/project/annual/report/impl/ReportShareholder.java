package com.liang.flink.project.annual.report.impl;

import com.liang.common.dto.tyc.Company;
import com.liang.common.dto.tyc.Human;
import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

@Slf4j
public class ReportShareholder extends AbstractDataUpdate<String> {
    private final static String TABLE_NAME = "entity_annual_report_shareholder_equity_details";
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        List<String> result = new ArrayList<>(deleteWithReturn(singleCanalBinlog));
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        // 公司
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annual_report_id"));
        // 股东
        String investorId = String.valueOf(columnMap.get("investor_id"));
        String investorName = String.valueOf(columnMap.get("investor_name"));
        String investorType = String.valueOf(columnMap.get("investor_type"));
        // 其它
        String subscribeAmount = String.valueOf(columnMap.get("subscribe_amount"));
        String subscribeTime = String.valueOf(columnMap.get("subscribe_time"));
        String subscribeType = String.valueOf(columnMap.get("subscribe_type"));
        String paidAmount = String.valueOf(columnMap.get("paid_amount"));
        String paidTime = String.valueOf(columnMap.get("paid_time"));
        String paidType = String.valueOf(columnMap.get("paid_type"));
        // 开始解析
        Map<String, Object> resultMap = new HashMap<>();
        Tuple2<Company, String> companyAndYear = dao.getCompanyAndYear(reportId);
        Company company = companyAndYear.f0;
        String year = companyAndYear.f1;
        // 公司
        resultMap.put("data_source_trace_id", id);
        resultMap.put("annual_report_year", year);
        resultMap.put("tyc_unique_entity_id", company.getGid());
        resultMap.put("entity_name_valid", company.getName());
        resultMap.put("entity_type_id", 1);
        // 股东
        resultMap.put("annual_report_entity_name_register_shareholder", investorName);
        switch (investorType) {
            case "1": // 人
                Human human = TycUtils.cid2Human(investorId);
                String pid = TycUtils.gid2Pid(company.getGid(), human.getGid());
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", pid);
                resultMap.put("annual_report_entity_name_valid_shareholder", human.getName());
                resultMap.put("annual_report_entity_type_id_shareholder", 2);
                break;
            case "2": // 公司
                Company investor = TycUtils.cid2Company(investorId);
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", investor.getGid());
                resultMap.put("annual_report_entity_name_valid_shareholder", investor.getName());
                resultMap.put("annual_report_entity_type_id_shareholder", 1);
                break;
            default: // 非人非公司 or 其它
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", 0);
                resultMap.put("annual_report_entity_name_valid_shareholder", investorName);
                resultMap.put("annual_report_entity_type_id_shareholder", 3);
                break;
        }
        // 检测脏数据
        checkMap(resultMap);
        if ("1".equals(String.valueOf(resultMap.get("delete_status")))) {
            return deleteWithReturn(singleCanalBinlog);
        }
        // 认缴
        HashMap<String, Object> resultMap1 = new HashMap<>(resultMap);
        resultMap1.put("annual_report_shareholder_capital_type", "认缴");
        resultMap1.put("annual_report_shareholder_capital_source", TycUtils.isValidName(subscribeAmount) ? subscribeAmount : "");
        Tuple2<String, String> numberAndUnit1 = formatEquity(id, TycUtils.formatEquity(subscribeAmount));
        if (numberAndUnit1.f0.contains("-")) {
            resultMap1.put("annual_report_shareholder_equity_amt", null);
            resultMap1.put("annual_report_shareholder_equity_currency", "");
        } else {
            resultMap1.put("annual_report_shareholder_equity_amt", numberAndUnit1.f0);
            resultMap1.put("annual_report_shareholder_equity_currency", numberAndUnit1.f1);
        }
        resultMap1.put("annual_report_shareholder_equity_valid_date", TycUtils.isDateTime(subscribeTime) ? subscribeTime : null);
        resultMap1.put("annual_report_shareholder_equity_submission_method", TycUtils.isValidName(subscribeType) ? subscribeType : "");
        Tuple2<String, String> insert1 = SqlUtils.columnMap2Insert(resultMap1);
        String sql1 = new SQL().REPLACE_INTO(TABLE_NAME)
                .INTO_COLUMNS(insert1.f0)
                .INTO_VALUES(insert1.f1)
                .toString();
        result.add(sql1);
        // 实缴
        HashMap<String, Object> resultMap2 = new HashMap<>(resultMap);
        resultMap2.put("annual_report_shareholder_capital_type", "实缴");
        resultMap2.put("annual_report_shareholder_capital_source", TycUtils.isValidName(paidAmount) ? paidAmount : "");
        Tuple2<String, String> numberAndUnit2 = formatEquity(id, TycUtils.formatEquity(paidAmount));
        if (numberAndUnit2.f0.contains("-")) {
            resultMap2.put("annual_report_shareholder_equity_amt", null);
            resultMap2.put("annual_report_shareholder_equity_currency", "");
        } else {
            resultMap2.put("annual_report_shareholder_equity_amt", numberAndUnit2.f0);
            resultMap2.put("annual_report_shareholder_equity_currency", numberAndUnit2.f1);
        }
        resultMap2.put("annual_report_shareholder_equity_valid_date", TycUtils.isDateTime(paidTime) ? paidTime : null);
        resultMap2.put("annual_report_shareholder_equity_submission_method", TycUtils.isValidName(paidType) ? paidType : "");
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
                .WHERE("data_source_trace_id = " + SqlUtils.formatValue(singleCanalBinlog.getColumnMap().get("id")))
                .toString();
        return Collections.singletonList(sql);
    }

    private void checkMap(Map<String, Object> resultMap) {
        if (TycUtils.isTycUniqueEntityId(resultMap.get("tyc_unique_entity_id")) &&
                TycUtils.isValidName(resultMap.get("entity_name_valid")) &&
                TycUtils.isTycUniqueEntityId(resultMap.get("annual_report_tyc_unique_entity_id_shareholder")) &&
                TycUtils.isValidName(resultMap.get("annual_report_entity_name_valid_shareholder")) &&
                TycUtils.isValidName(resultMap.get("annual_report_entity_name_register_shareholder")) &&
                String.valueOf(resultMap.get("annual_report_year")).matches("\\d{4}")
        ) {
        } else {
            resultMap.put("delete_status", 1);
        }
    }

    private Tuple2<String, String> formatEquity(String dataSourceTraceId, Tuple2<String, String> tuple2) {
        String equity = tuple2.f0;
        String[] split = equity.split("\\.");
        if (split[0].length() > 38 - 12) {
            log.error("超出decimal(38,12)的投资额: {}, data_source_trace_id: {}", tuple2, dataSourceTraceId);
            return Tuple2.of("0", tuple2.f1);
        } else {
            return tuple2;
        }
    }
}
