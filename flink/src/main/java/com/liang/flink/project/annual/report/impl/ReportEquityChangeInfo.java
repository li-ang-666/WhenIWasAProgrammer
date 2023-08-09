package com.liang.flink.project.annual.report.impl;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ReportEquityChangeInfo extends AbstractDataUpdate<String> {
    private final static String TABLE_NAME = "entity_annual_report_shareholder_equity_change_details";
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        Map<String, Object> resultMap = new HashMap<>();
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annualreport_id"));
        String investorId = String.valueOf(columnMap.get("investor_id"));
        String investorName = String.valueOf(columnMap.get("investor_name"));
        String investorType = String.valueOf(columnMap.get("investor_type"));
        String ratioBefore = String.valueOf(columnMap.get("ratio_before"));
        String ratioAfter = String.valueOf(columnMap.get("ratio_after"));
        String changeTime = String.valueOf(columnMap.get("change_time"));

        Tuple3<String, String, String> info = dao.getCompanyAndYear(reportId, resultMap);
        resultMap.put("id", id);
        // 公司
        resultMap.put("tyc_unique_entity_id", info.f0);
        resultMap.put("entity_name_valid", info.f1);
        resultMap.put("entity_type_id", 1);
        // 股东
        resultMap.put("annual_report_entity_name_valid_shareholder", investorName);
        switch (investorType) {
            case "2": // 人
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", TycUtils.getHumanHashId(info.f0, TycUtils.humanCid2Gid(investorId)));
                resultMap.put("annual_report_entity_type_id_shareholder", 2);
                break;
            case "1": // 公司
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", TycUtils.companyCid2GidAndName(investorId).f0);
                resultMap.put("annual_report_entity_type_id_shareholder", 1);
                break;
            case "3": // 非人非公司
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", investorId);
                resultMap.put("annual_report_entity_type_id_shareholder", 3);
                break;
            default: // 其它
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
        // 股权变更
        resultMap.put("annual_report_equity_ratio_before_change", parse(id, ratioBefore));
        resultMap.put("annual_report_equity_ratio_after_change", parse(id, ratioAfter));
        resultMap.put("annual_report_equity_ratio_change_time", TycUtils.isDateTime(changeTime) ? changeTime : null);
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

    private String parse(String id, String percent) {
        StringBuilder builder = new StringBuilder();
        int length = percent.length();
        for (int i = 0; i < length; i++) {
            char c = percent.charAt(i);
            if (Character.isDigit(c) || '.' == c) {
                builder.append(c);
            }
        }
        if (builder.length() == 0) {
            builder.append("0");
        }
        String defaultResult = new BigDecimal(0)
                .setScale(12, RoundingMode.DOWN)
                .toPlainString();
        try {
            String plainString = new BigDecimal(builder.toString())
                    .divide(new BigDecimal(100), RoundingMode.DOWN)
                    .setScale(12, RoundingMode.DOWN)
                    .toPlainString();
            if (plainString.compareTo("1.0000000000000") > 0) {
                log.error("股权变更解析异常, id = {}, percent = {}", id, percent);
                return defaultResult;
            }
            return plainString;
        } catch (Exception e) {
            log.error("股权变更解析异常, id = {}, percent = {}", id, percent, e);
            return defaultResult;
        }
    }
}
