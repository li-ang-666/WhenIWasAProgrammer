package com.liang.flink.project.data.concat.impl;



import com.liang.common.dto.HbaseOneRow;
import com.liang.flink.project.data.concat.dao.RestrictConsumptionSplitIndexDao;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.AbstractDataUpdate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RestrictConsumptionSplitIndex extends AbstractDataUpdate {
    private final RestrictConsumptionSplitIndexDao dao = new RestrictConsumptionSplitIndexDao();

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
        String restrictedId = String.valueOf(columnMap.get("company_info_company_id"));
        String relatedRestrictedId = String.valueOf(columnMap.get("restrict_consumption_company_id"));

        if (StringUtils.isNumeric(companyId) && !"0".equals(companyId)) {
            HashMap<String, Object> hbaseColumnMap = new HashMap<>();
            String mostApplicant = dao.queryMostApplicant(companyId, isHistory);
            if (StringUtils.isNotBlank(mostApplicant)) {
                if (mostApplicant.matches("(.*?):(\\d+)")) {
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_applicant_type", "1");
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_applicant_id", mostApplicant.replaceAll("(.*?):(\\d+)", "$2"));
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_applicant_name", mostApplicant.replaceAll("(.*?):(\\d+)", "$1"));
                } else {
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_applicant_type", null);
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_applicant_id", null);
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_applicant_name", mostApplicant);
                }
            } else {
                hbaseColumnMap.put(prefix + "restrict_consumption_most_applicant_type", null);
                hbaseColumnMap.put(prefix + "restrict_consumption_most_applicant_id", null);
                hbaseColumnMap.put(prefix + "restrict_consumption_most_applicant_name", null);
            }
            subResult.add(new HbaseOneRow("dataConcat", companyId).putAll(hbaseColumnMap));
        }

        if (StringUtils.isNumeric(restrictedId) && !"0".equals(restrictedId)) {
            HashMap<String, Object> hbaseColumnMap = new HashMap<>();
            String mostRelatedRestricted = dao.queryMostRelatedRestricted(restrictedId, isHistory);
            if (StringUtils.isNotBlank(mostRelatedRestricted)) {
                if (mostRelatedRestricted.matches("(.*?):(\\d+):(company)")) {
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_type", "1");
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_id", mostRelatedRestricted.replaceAll("(.*?):(\\d+):(company)", "$2"));
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_name", mostRelatedRestricted.replaceAll("(.*?):(\\d+):(company)", "$1"));
                } else if (mostRelatedRestricted.matches("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)")) {
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_type", "2");
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_id", mostRelatedRestricted.replaceAll("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)", "$3"));
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_name", mostRelatedRestricted.replaceAll("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)", "$1"));
                } else {
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_type", null);
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_id", null);
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_name", mostRelatedRestricted);
                }
            } else {
                hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_type", null);
                hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_id", null);
                hbaseColumnMap.put(prefix + "restrict_consumption_most_related_restricted_name", null);
            }
            subResult.add(new HbaseOneRow("dataConcat", restrictedId).putAll(hbaseColumnMap));
        }

        if (StringUtils.isNumeric(relatedRestrictedId) && !"0".equals(relatedRestrictedId)) {
            HashMap<String, Object> hbaseColumnMap = new HashMap<>();
            String mostRestricted = dao.queryMostRestricted(relatedRestrictedId, isHistory);
            if (StringUtils.isNotBlank(mostRestricted)) {
                if (mostRestricted.matches("(.*?):(\\d+):(company)")) {
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_type", "1");
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_id", mostRestricted.replaceAll("(.*?):(\\d+):(company)", "$2"));
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_name", mostRestricted.replaceAll("(.*?):(\\d+):(company)", "$1"));
                } else if (mostRestricted.matches("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)")) {
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_type", "2");
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_id", mostRestricted.replaceAll("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)", "$3"));
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_name", mostRestricted.replaceAll("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)", "$1"));
                } else {
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_type", null);
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_id", null);
                    hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_name", mostRestricted);
                }
            } else {
                hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_type", null);
                hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_id", null);
                hbaseColumnMap.put(prefix + "restrict_consumption_most_restricted_name", null);
            }
            subResult.add(new HbaseOneRow("dataConcat", relatedRestrictedId).putAll(hbaseColumnMap));
        }
        result.addAll(subResult);
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
