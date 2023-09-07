package com.liang.flink.project.evaluation.institution.candidate;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvaluationInstitutionCandidateDao {
    private final JdbcTemplate dataJudicialRisk108 = new JdbcTemplate("108.data_judicial_risk");
    private final JdbcTemplate dataIndex150 = new JdbcTemplate("150.data_index");
    private final JdbcTemplate humanBase040 = new JdbcTemplate("040.human_base");
    private final JdbcTemplate prism464 = new JdbcTemplate("464.prism");
    private final JdbcTemplate sink = new JdbcTemplate("427.test");

    public Map<String, Object> getEvaluate(String evaluateId) {
        if (!TycUtils.isUnsignedId(evaluateId)) {
            return new HashMap<>();
        }
        String sql = new SQL()
                .SELECT("*")
                .FROM("zhixinginfo_evaluate")
                .WHERE("deleted = 0")
                .WHERE("id = " + SqlUtils.formatValue(evaluateId))
                .toString();
        List<Map<String, Object>> columnMaps = dataJudicialRisk108.queryForColumnMaps(sql);
        return columnMaps.isEmpty() ? new HashMap<>() : columnMaps.get(0);
    }

    public Map<String, Object> getEvaluateIndex(String evaluateId) {
        if (!TycUtils.isUnsignedId(evaluateId)) {
            return new HashMap<>();
        }
        String sql = new SQL()
                .SELECT("*")
                .FROM("zhixinginfo_evaluate_index")
                .WHERE("deleted = 0")
                .WHERE("main_id = " + SqlUtils.formatValue(evaluateId))
                .toString();
        List<Map<String, Object>> columnMaps = dataIndex150.queryForColumnMaps(sql);
        return columnMaps.isEmpty() ? new HashMap<>() : columnMaps.get(0);
    }

    public List<Map<String, Object>> getCompanyLawHumanRelations(String sourceId) {
        if (!TycUtils.isUnsignedId(sourceId)) {
            return new ArrayList<>();
        }
        String sql = new SQL()
                .SELECT("*")
                .FROM("company_law_human_realtion")
                .WHERE("source_id = " + SqlUtils.formatValue(sourceId))
                .WHERE("source_table = 'zhixinginfo_evaluate'")
                .WHERE("data_flag = 1")
                .toString();
        return humanBase040.queryForColumnMaps(sql);
    }

    public String companyGid2Name(String companyGid) {
        if (!TycUtils.isUnsignedId(companyGid)) {
            return "";
        }
        String sql = new SQL().SELECT("name")
                .FROM("enterprise")
                .WHERE("deleted = 0")
                .WHERE("graph_id = " + SqlUtils.formatValue(companyGid))
                .toString();
        String res = prism464.queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "";
    }

    public List<String> getBusinessIds(String companyGid) {
        String sql = new SQL().SELECT("data_source_trace_id")
                .FROM(EvaluationInstitutionCandidateService.TABLE)
                .WHERE("tyc_unique_entity_id_subject_to_enforcement = " + SqlUtils.formatValue(companyGid))
                .OR()
                .WHERE("tyc_unique_entity_id_candidate_evaluation_institution = " + SqlUtils.formatValue(companyGid))
                .toString();
        return sink.queryForList(sql, rs -> rs.getString(1));
    }
}
