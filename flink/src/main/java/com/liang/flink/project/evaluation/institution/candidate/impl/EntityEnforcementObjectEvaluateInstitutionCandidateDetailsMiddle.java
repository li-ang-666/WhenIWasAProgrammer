package com.liang.flink.project.evaluation.institution.candidate.impl;

import com.liang.flink.dto.SingleCanalBinlog;
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
        sqls.add(SqlHolder.getDeleteSql(caseNumber));
        sqls.add(SqlHolder.getInsertSql(caseNumber));
        return sqls;
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
