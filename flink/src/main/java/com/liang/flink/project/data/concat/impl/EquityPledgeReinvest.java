package com.liang.flink.project.data.concat.impl;


import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.data.concat.dao.EquityPledgeReinvestDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EquityPledgeReinvest extends AbstractDataUpdate<HbaseOneRow> {
    EquityPledgeReinvestDao dao = new EquityPledgeReinvestDao();

    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        List<HbaseOneRow> result = new ArrayList<>();
        currentAndHistory(singleCanalBinlog, false, result);
        currentAndHistory(singleCanalBinlog, true, result);
        return result;
    }

    private void currentAndHistory(SingleCanalBinlog singleCanalBinlog, boolean isHistory, List<HbaseOneRow> result) {
        String prefix = isHistory ? "history_" : "";
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String companyId = String.valueOf(columnMap.get("company_id"));
        String pledgorEntityId = String.valueOf(columnMap.get("pledgor_entity_id"));
        String pledgorType = String.valueOf(columnMap.get("pledgor_type"));

        //作为出质人(公司)
        if (StringUtils.isNumeric(pledgorEntityId) && !"0".equals(pledgorEntityId)
                && "1".equals(pledgorType)) {
            HashMap<String, Object> hbaseColumnMap = new HashMap<>();
            hbaseColumnMap.put(prefix + "equity_pledge_reinvest_total_amt", dao.queryTotalEquity(pledgorEntityId, isHistory));
            Tuple3<String, String, String> maxTargetCompany = dao.queryMaxTargetCompany(pledgorEntityId, isHistory);
            hbaseColumnMap.put(prefix + "equity_pledge_reinvest_most_company_type", maxTargetCompany.f0);
            hbaseColumnMap.put(prefix + "equity_pledge_reinvest_most_company_id", maxTargetCompany.f1);
            hbaseColumnMap.put(prefix + "equity_pledge_reinvest_most_company_name", maxTargetCompany.f2);
            if (isHistory) {
                HbaseSchema hbaseSchema = new HbaseSchema("prism_c", "historical_info_splice", "ds", true);
                HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, pledgorEntityId, hbaseColumnMap);
                result.add(hbaseOneRow);
            } else {
                HbaseSchema hbaseSchema = new HbaseSchema("prism_c", "operating_risk_splice", "ds", true);
                HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, pledgorEntityId, hbaseColumnMap);
                result.add(hbaseOneRow);
            }
        }

        if (StringUtils.isNumeric(companyId) && !"0".equals(companyId)) {
            HashMap<String, Object> hbaseColumnMap = new HashMap<>();
            hbaseColumnMap.put(prefix + "equity_pledge_reinvest_total_amt", dao.queryTotalEquity(companyId, isHistory));
            Tuple3<String, String, String> maxPledgor = dao.queryMaxPledgor(companyId, isHistory);
            hbaseColumnMap.put(prefix + "equity_pledge_reinvest_most_pledgor_type", maxPledgor.f0);
            hbaseColumnMap.put(prefix + "equity_pledge_reinvest_most_pledgor_id", maxPledgor.f1);
            hbaseColumnMap.put(prefix + "equity_pledge_reinvest_most_pledgor_name", maxPledgor.f2);
            if (isHistory) {
                HbaseSchema hbaseSchema = new HbaseSchema("prism_c", "historical_info_splice", "ds", true);
                HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, companyId, hbaseColumnMap);
                result.add(hbaseOneRow);
            } else {
                HbaseSchema hbaseSchema = new HbaseSchema("prism_c", "operating_risk_splice", "ds", true);
                HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, companyId, hbaseColumnMap);
                result.add(hbaseOneRow);
            }
        }
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
