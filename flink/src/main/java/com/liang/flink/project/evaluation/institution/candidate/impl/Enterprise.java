package com.liang.flink.project.evaluation.institution.candidate.impl;

import com.liang.common.service.SQL;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.evaluation.institution.candidate.EvaluationInstitutionCandidateDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Enterprise extends AbstractDataUpdate<String> {
    private final EvaluationInstitutionCandidateDao dao = new EvaluationInstitutionCandidateDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String graphId = String.valueOf(columnMap.get("graph_id"));
        List<String> businessIds = dao.getBusinessIds(graphId);
        List<String> sqls = new ArrayList<>();
        if (!businessIds.isEmpty()) {
            String sql = new SQL().UPDATE("zhixinginfo_evaluate_index")
                    .SET("update_time = date_add(update_time, interval 1 second)")
                    .WHERE("main_id in (" + String.join(",", businessIds) + ")")
                    .toString();
            sqls.add(sql);
        }
        return sqls;
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
