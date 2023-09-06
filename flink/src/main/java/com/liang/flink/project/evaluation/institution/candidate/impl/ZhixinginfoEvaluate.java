package com.liang.flink.project.evaluation.institution.candidate.impl;

import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.evaluation.institution.candidate.EvaluationInstitutionCandidateService;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.List;
import java.util.Map;

public class ZhixinginfoEvaluate extends AbstractDataUpdate<String> {
    private final EvaluationInstitutionCandidateService service = new EvaluationInstitutionCandidateService();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String id = String.valueOf(columnMap.get("id"));
        return service.invoke(id);
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
