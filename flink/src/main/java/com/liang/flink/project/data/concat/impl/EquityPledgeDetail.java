package com.liang.flink.project.data.concat.impl;


import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.data.concat.dao.EquityPledgeDetailDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EquityPledgeDetail extends AbstractDataUpdate<HbaseOneRow> {
    EquityPledgeDetailDao dao = new EquityPledgeDetailDao();

    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        List<HbaseOneRow> result = new ArrayList<>();
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String companyId = String.valueOf(columnMap.get("company_id"));
        String pledgorId = String.valueOf(columnMap.get("pledgor_id"));

        if (StringUtils.isNumeric(companyId) && !"0".equals(companyId)) {
            HashMap<String, Object> hbaseColumnMap = new HashMap<>();
            hbaseColumnMap.put("equity_pledgee_total_amt", dao.queryTotalEquity(companyId));
            Tuple3<String, String, String> maxPledgor = dao.queryMaxPledgor(companyId);
            hbaseColumnMap.put("equity_pledgee_most_pledgor_type", maxPledgor.f0);
            hbaseColumnMap.put("equity_pledgee_most_pledgor_id", maxPledgor.f1);
            hbaseColumnMap.put("equity_pledgee_most_pledgor_name", maxPledgor.f2);

            HbaseSchema hbaseSchema = new HbaseSchema("prism_c", "operating_risk_splice", "ds", true);
            HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, companyId, hbaseColumnMap);
            result.add(hbaseOneRow);
        }

        if (StringUtils.isNumeric(pledgorId) && !"0".equals(pledgorId)) {
            HashMap<String, Object> hbaseColumnMap = new HashMap<>();
            Tuple3<String, String, String> maxTargetCompany = dao.queryMaxTargetCompany(pledgorId);
            hbaseColumnMap.put("equity_pledgee_most_company_type", maxTargetCompany.f0);
            hbaseColumnMap.put("equity_pledgee_most_company_id", maxTargetCompany.f1);
            hbaseColumnMap.put("equity_pledgee_most_company_name", maxTargetCompany.f2);

            HbaseSchema hbaseSchema = new HbaseSchema("prism_c", "operating_risk_splice", "ds", true);
            HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, pledgorId, hbaseColumnMap);
            result.add(hbaseOneRow);
        }
        return result;
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
