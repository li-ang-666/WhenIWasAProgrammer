package com.liang.flink.project.data.concat.impl;


import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.data.concat.dao.RestrictedOutboundIndexDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RestrictedOutboundIndex extends AbstractDataUpdate<HbaseOneRow> {
    private final RestrictedOutboundIndexDao dao = new RestrictedOutboundIndexDao();

    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        ArrayList<HbaseOneRow> result = new ArrayList<>();
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        HashMap<String, Object> hbaseColumnMap = new HashMap<>();
        String companyId = String.valueOf(columnMap.get("company_id"));
        if (StringUtils.isNumeric(companyId) && !"0".equals(companyId)) {
            String mostApplicant = dao.queryMostApplicant(companyId);
            if (StringUtils.isNotBlank(mostApplicant)) {
                if (mostApplicant.matches("(.*?):(\\d+):company")) {
                    hbaseColumnMap.put("restricted_outbound_most_applicant_type", "1");
                    hbaseColumnMap.put("restricted_outbound_most_applicant_id", mostApplicant.replaceAll("(.*?):(\\d+):company", "$2"));
                    hbaseColumnMap.put("restricted_outbound_most_applicant_name", mostApplicant.replaceAll("(.*?):(\\d+):company", "$1"));
                } else {
                    hbaseColumnMap.put("restricted_outbound_most_applicant_type", null);
                    hbaseColumnMap.put("restricted_outbound_most_applicant_id", null);
                    hbaseColumnMap.put("restricted_outbound_most_applicant_name", mostApplicant);
                }
            } else {
                hbaseColumnMap.put("restricted_outbound_most_applicant_type", null);
                hbaseColumnMap.put("restricted_outbound_most_applicant_id", null);
                hbaseColumnMap.put("restricted_outbound_most_applicant_name", null);
            }

            String restricted = dao.queryMostRestricted(companyId);
            if (StringUtils.isNotBlank(restricted)) {
                hbaseColumnMap.put("restricted_outbound_most_restricted_type", null);
                hbaseColumnMap.put("restricted_outbound_most_restricted_id", null);
                hbaseColumnMap.put("restricted_outbound_most_restricted_name", restricted);
            } else {
                hbaseColumnMap.put("restricted_outbound_most_restricted_type", null);
                hbaseColumnMap.put("restricted_outbound_most_restricted_id", null);
                hbaseColumnMap.put("restricted_outbound_most_restricted_name", null);
            }
            HbaseSchema hbaseSchema = new HbaseSchema("prism_c", "judicial_risk_splice", "ds", true);
            HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, companyId, hbaseColumnMap);
            result.add(hbaseOneRow);
        }
        return result;
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
