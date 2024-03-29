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
        // 公司
        String id = String.valueOf(columnMap.get("id"));
        String reportId = String.valueOf(columnMap.get("annualreport_id"));
        // 股东
        String investorId = String.valueOf(columnMap.get("investor_id"));
        String investorName = String.valueOf(columnMap.get("investor_name"));
        String investorType = String.valueOf(columnMap.get("investor_type"));
        // 其它
        String ratioBefore = String.valueOf(columnMap.get("ratio_before"));
        String ratioAfter = String.valueOf(columnMap.get("ratio_after"));
        String changeTime = String.valueOf(columnMap.get("change_time"));
        // 开始解析
        Map<String, Object> resultMap = new HashMap<>();
        Tuple2<Company, String> companyAndYear = dao.getCompanyAndYear(reportId);
        Company company = companyAndYear.f0;
        String year = companyAndYear.f1;
        if (!TycUtils.isYear(year)) {
            year = null;
        }
        if (!TycUtils.isUnsignedId(company.getGid()) || !TycUtils.isValidName(company.getName())) {
            log.error("report_equity_change_info, id: {}, 路由不到正确实体", id);
            return deleteWithReturn(singleCanalBinlog);
        }
        // 公司
        resultMap.put("id", id);
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
                String s = TycUtils.pid2Name(pid);
                if (!TycUtils.isTycUniqueEntityId(pid) || !TycUtils.isValidName(s)) {
                    resultMap.put("annual_report_tyc_unique_entity_id_shareholder", 0);
                    resultMap.put("annual_report_entity_name_valid_shareholder", investorName);
                    resultMap.put("annual_report_entity_type_id_shareholder", 3);
                } else {
                    resultMap.put("annual_report_tyc_unique_entity_id_shareholder", pid);
                    resultMap.put("annual_report_entity_name_valid_shareholder", s);
                    resultMap.put("annual_report_entity_type_id_shareholder", 2);
                }
                break;
            case "2": // 公司
                Company investor = TycUtils.cid2Company(investorId);
                if (!TycUtils.isUnsignedId(investor.getGid()) || !TycUtils.isValidName(investor.getName())) {
                    resultMap.put("annual_report_tyc_unique_entity_id_shareholder", 0);
                    resultMap.put("annual_report_entity_name_valid_shareholder", investorName);
                    resultMap.put("annual_report_entity_type_id_shareholder", 3);
                } else {
                    resultMap.put("annual_report_tyc_unique_entity_id_shareholder", investor.getGid());
                    resultMap.put("annual_report_entity_name_valid_shareholder", investor.getName());
                    resultMap.put("annual_report_entity_type_id_shareholder", 1);
                }
                break;
            default: // 非人非公司 or 其它
                resultMap.put("annual_report_tyc_unique_entity_id_shareholder", 0);
                resultMap.put("annual_report_entity_name_valid_shareholder", investorName);
                resultMap.put("annual_report_entity_type_id_shareholder", 3);
                break;
        }
        // 股权变更
        String bef = parse(id, ratioBefore);
        String aft = parse(id, ratioAfter);
        // 股权比例脏数据
        if (bef.contains("-")) {
            bef = null;
        }
        if (aft.contains("-")) {
            aft = null;
        }
        resultMap.put("annual_report_equity_ratio_before_change", bef);
        resultMap.put("annual_report_equity_ratio_after_change", aft);
        String checkedChangeTime = TycUtils.isDateTime(changeTime) ? changeTime : null;
        resultMap.put("annual_report_equity_ratio_change_time", checkedChangeTime);
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
            return "-1";
        }
        try {
            String plainString = new BigDecimal(builder.toString())
                    .divide(new BigDecimal(100), 12, RoundingMode.DOWN)
                    .setScale(12, RoundingMode.DOWN)
                    .toPlainString();
            //if (plainString.compareTo("1.0000000000000") > 0) {
            //    log.warn("股权变更解析异常, id = {}, percent = {}", id, percent);
            //    return "-1";
            //}
            return plainString;
        } catch (Exception e) {
            log.warn("股权变更解析异常, id = {}, percent = {}", id, percent, e);
            return "-1";
        }
    }
}
