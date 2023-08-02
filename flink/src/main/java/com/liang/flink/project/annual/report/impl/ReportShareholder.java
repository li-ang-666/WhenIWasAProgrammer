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
import java.util.*;

public class ReportShareholder extends AbstractDataUpdate<String> {
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        List<String> result = new ArrayList<>();
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annual_report_id"));
        String shareholderName = String.valueOf(columnMap.get("investor_name_clean"));
        String subscribeAmount = String.valueOf(columnMap.get("subscribe_amount"));
        String subscribeTime = String.valueOf(columnMap.get("subscribe_time"));
        String subscribeType = String.valueOf(columnMap.get("subscribe_type"));
        String paidAmount = String.valueOf(columnMap.get("paid_amount"));
        String paidTime = String.valueOf(columnMap.get("paid_time"));
        String paidType = String.valueOf(columnMap.get("paid_type"));

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("business_id", id);
        Tuple3<String, String, String> info = dao.getInfoAndNameByReportId(reportId);
        //
        resultMap.put("tyc_unique_entity_id", info.f0);
        resultMap.put("entity_name_valid", info.f1);
        resultMap.put("entity_type_id", 1);
        //
        resultMap.put("annual_report_tyc_unique_entity_id_shareholder", -1);
        resultMap.put("annual_report_entity_name_valid_shareholder", shareholderName);
        resultMap.put("annual_report_entity_type_id_shareholder", -1);
        //
        HashMap<String, Object> resultMap1 = new HashMap<>(resultMap);
        resultMap1.put("annual_report_shareholder_capital_type", 1);
        resultMap1.put("annual_report_shareholder_equity_amt", parse(subscribeAmount));
        resultMap1.put("annual_report_shareholder_equity_currency", "万元");
        resultMap1.put("annual_report_shareholder_equity_valid_date", DateTimeUtils.isDateTime(subscribeTime) ? subscribeTime : null);
        resultMap1.put("annual_report_shareholder_equity_submission_method", subscribeType);
        Tuple2<String, String> insert1 = SqlUtils.columnMap2Insert(resultMap1);
        String sql1 = new SQL().REPLACE_INTO("entity_annual_report_shareholder_equity_details")
                .INTO_COLUMNS(insert1.f0)
                .INTO_VALUES(insert1.f1)
                .toString();
        result.add(sql1);
        //
        HashMap<String, Object> resultMap2 = new HashMap<>(resultMap);
        resultMap2.put("annual_report_shareholder_capital_type", 2);
        resultMap2.put("annual_report_shareholder_equity_amt", parse(paidAmount));
        resultMap2.put("annual_report_shareholder_equity_currency", "万元");
        resultMap2.put("annual_report_shareholder_equity_valid_date", DateTimeUtils.isDateTime(paidTime) ? paidTime : null);
        resultMap2.put("annual_report_shareholder_equity_submission_method", paidType);
        Tuple2<String, String> insert2 = SqlUtils.columnMap2Insert(resultMap2);
        String sql2 = new SQL().REPLACE_INTO("entity_annual_report_shareholder_equity_details")
                .INTO_COLUMNS(insert2.f0)
                .INTO_VALUES(insert2.f1)
                .toString();
        result.add(sql2);
        return result;
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        String sql = new SQL().DELETE_FROM("entity_annual_report_shareholder_equity_details")
                .WHERE("business_id = " + SqlUtils.formatValue(singleCanalBinlog.getColumnMap().get("id")))
                .toString();
        return Collections.singletonList(sql);
    }

    private String parse(String money) {
        StringBuilder builder = new StringBuilder();
        for (char c : money.toCharArray()) {
            if (Character.isDigit(c) || c == '.') {
                builder.append(c);
            }
        }
        try {
            return new BigDecimal(builder.toString()).setScale(12, RoundingMode.DOWN).toPlainString();
        } catch (Exception e) {
            return new BigDecimal("0").setScale(12, RoundingMode.DOWN).toPlainString();
        }
    }
}
