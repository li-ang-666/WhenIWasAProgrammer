package com.liang.flink.project.evaluation.institution.candidate.impl;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.evaluation.institution.candidate.EvaluationInstitutionCandidateService;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class EntityEnforcementObjectEvaluateInstitutionCandidateDetailsMiddle extends AbstractDataUpdate<String> {

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        List<String> sqls = new ArrayList<>();
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String caseNumber = String.valueOf(columnMap.get("enforcement_case_number"));
        String deleteSql = new SQL().DELETE_FROM(EvaluationInstitutionCandidateService.TABLE.replaceAll("_middle", ""))
                .WHERE("enforcement_case_number = " + SqlUtils.formatValue(caseNumber))
                .toString();
        String insertSql = SqlHolder.getSql(caseNumber);
        sqls.add(deleteSql);
        sqls.add(insertSql);
        return sqls;
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
