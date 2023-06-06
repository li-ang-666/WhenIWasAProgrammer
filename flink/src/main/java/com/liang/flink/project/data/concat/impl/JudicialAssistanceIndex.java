package com.liang.flink.project.data.concat.impl;


import com.liang.common.dto.HbaseOneRow;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.data.concat.dao.JudicialAssistanceIndexDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JudicialAssistanceIndex extends AbstractDataUpdate<HbaseOneRow> {
    private final JudicialAssistanceIndexDao dao = new JudicialAssistanceIndexDao();

    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        List<HbaseOneRow> result = new ArrayList<>();
        currentAndHistory(singleCanalBinlog, false, result);
        currentAndHistory(singleCanalBinlog, true, result);
        return result;
    }

    private void currentAndHistory(SingleCanalBinlog singleCanalBinlog, boolean isHistory, List<HbaseOneRow> result) {
        String prefix = isHistory ? "history_" : "";
        List<HbaseOneRow> subResult = new ArrayList<>();
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();

        String companyId = String.valueOf(columnMap.get("company_id"));
        String enforcedTargetId = String.valueOf(columnMap.get("enforced_target_id"));
        String enforcedTargetType = String.valueOf(columnMap.get("enforced_target_type"));

        if (StringUtils.isNumeric(companyId) && !"0".equals(companyId)) {
            HashMap<String, Object> hbaseColumnMap = new HashMap<>();

            hbaseColumnMap.put(prefix + "equity_frozen_equity_total", dao.queryTotalFrozenEquity(companyId, isHistory));
            Tuple3<String, String, String> equityFrozenEnforcedTarget = dao.queryMostEquityFrozenEnforcedTarget(companyId, isHistory);

            hbaseColumnMap.put(prefix + "equity_frozen_most_enforced_target_type", equityFrozenEnforcedTarget.f0);
            hbaseColumnMap.put(prefix + "equity_frozen_most_enforced_target_id", equityFrozenEnforcedTarget.f1);
            hbaseColumnMap.put(prefix + "equity_frozen_most_enforced_target_name", equityFrozenEnforcedTarget.f2);
            if (isHistory)
                subResult.add(new HbaseOneRow("dataConcatHistoricalInfoSchema", companyId).putAll(hbaseColumnMap));
            else
                subResult.add(new HbaseOneRow("dataConcatJudicialRiskSchema", companyId).putAll(hbaseColumnMap));
        }

        //作为被执行人(公司)
        if (StringUtils.isNumeric(enforcedTargetId) && !"0".equals(enforcedTargetId)
                && "1".equals(enforcedTargetType)) {
            HashMap<String, Object> hbaseColumnMap = new HashMap<>();

            hbaseColumnMap.put(prefix + "equity_frozen_equity_total", dao.queryTotalFrozenEquity(enforcedTargetId, isHistory));
            Tuple3<String, String, String> equityFrozenCompany = dao.queryMostEquityFrozenCompany(enforcedTargetId, isHistory);

            hbaseColumnMap.put(prefix + "equity_frozen_most_enforced_company_type", equityFrozenCompany.f0);
            hbaseColumnMap.put(prefix + "equity_frozen_most_enforced_company_id", equityFrozenCompany.f1);
            hbaseColumnMap.put(prefix + "equity_frozen_most_enforced_company_name", equityFrozenCompany.f2);
            if (isHistory)
                subResult.add(new HbaseOneRow("dataConcatHistoricalInfoSchema", enforcedTargetId).putAll(hbaseColumnMap));
            else
                subResult.add(new HbaseOneRow("dataConcatJudicialRiskSchema", enforcedTargetId).putAll(hbaseColumnMap));
        }
        result.addAll(subResult);
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
