package com.liang.flink.project.evaluation.institution.candidate;

import com.liang.common.dto.tyc.Company;
import com.liang.common.service.SQL;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class EvaluationInstitutionCandidateService {
    public final static String TABLE = "entity_enforcement_object_evaluate_institution_candidate_details";
    private final EvaluationInstitutionCandidateDao dao = new EvaluationInstitutionCandidateDao();
    private final CaseCodeClean caseCodeClean = new CaseCodeClean();
    private final CaseCodeType caseCodeType = new CaseCodeType();

    public List<String> invoke(String evaluateId) {
        // 如果不是合法id, 跳出
        ArrayList<String> sqls = new ArrayList<>();
        if (!TycUtils.isUnsignedId(evaluateId)) {
            return sqls;
        }
        // 合法id首先生成删除sql
        String deleteSql = new SQL().DELETE_FROM(TABLE)
                .WHERE("data_source_trace_id = " + SqlUtils.formatValue(evaluateId))
                .toString();
        sqls.add(deleteSql);
        Map<String, Object> evaluate = dao.getEvaluate(evaluateId);
        // 合法id但是查不出数据,跳出
        if (evaluate.isEmpty()) {
            return sqls;
        }
        // evaluate表字段陈列
        String type = String.valueOf(evaluate.get("type"));
        String zhixingid = String.valueOf(evaluate.get("zhixingid"));
        // 准备所有涉及到的被执行实体
        List<Entity> entities = new ArrayList<>();
        if ("1".equals(type)) {
            // 1 代表 公司
            Company company = TycUtils.cid2Company(zhixingid);
            entities.add(new Entity(String.valueOf(company.getGid()), company.getName(), "1"));
        } else {
            // 其余一概走人的逻辑
            List<Map<String, Object>> companyLawHumanRelations = dao.getCompanyLawHumanRelations(evaluateId);
            for (Map<String, Object> companyLawHumanRelation : companyLawHumanRelations) {
                entities.add(new Entity(String.valueOf(companyLawHumanRelation.get("human_id")), String.valueOf(companyLawHumanRelation.get("human_name")), "2"));
            }
        }
        // entities清洗
        entities = entities.stream().filter(e -> TycUtils.isTycUniqueEntityId(e.getTycUniqueEntityId()) && TycUtils.isValidName(e.getEntityName())).collect(Collectors.toList());
        // 没有涉及到的被执行实体,跳出
        if (entities.isEmpty()) {
            return sqls;
        }
        // 准备所有涉及到的机构
        Map<String, Object> evaluateIndex = dao.getEvaluateIndex(evaluateId);
        String json = String.valueOf(evaluateIndex.get("evaluationAgency"));
        List<Tuple2<String, String>> agencies = JsonUtils.parseJsonArr(json).stream()
                .map(e -> {
                    String gid = String.valueOf(((Map<String, Object>) e).get("gid"));
                    String name = dao.companyGid2Name(gid);
                    return Tuple2.of(gid, name);
                })
                .filter(e -> TycUtils.isUnsignedId(e.f0) && TycUtils.isValidName(e.f1))
                .collect(Collectors.toList());
        // 如果没有合法机构,跳出
        if (agencies.isEmpty()) {
            return sqls;
        }
        HashMap<String, Object> resultMap = new HashMap<>();
        // id
        resultMap.put("data_source_trace_id", evaluateId);
        // 执行案号(清洗)
        String caseNumber = String.valueOf(evaluate.get("caseNumber"));
        String caseNumberClean = caseCodeClean.evaluate(caseNumber);
        resultMap.put("enforcement_case_number", formatString(caseNumberClean));
        // 执行案号(原始)
        resultMap.put("enforcement_case_number_original", formatString(caseNumber));
        // 执行案型
        resultMap.put("enforcement_case_type", formatString(caseCodeType.evaluate(caseNumberClean)));
        // 委托法院
        resultMap.put("enforcement_object_evaluation_court_name", formatString(evaluate.get("execCourtName")));
        // 财产类型
        resultMap.put("enforcement_object_asset_type", formatString(evaluate.get("subjectType")));
        // 财产名称
        resultMap.put("enforcement_object_name", formatString(String.valueOf(evaluate.get("subjectname")).replaceAll("\\s", "")));
        // 摇号日期
        resultMap.put("lottery_date_to_candidate_evaluation_institution", TycUtils.isDateTime(evaluate.get("insertTime")) ? evaluate.get("insertTime") : null);
        // 是否最终选定的机构
        resultMap.put("is_eventual_evaluation_institution", 0);
        for (Entity entity : entities) {
            for (Tuple2<String, String> agency : agencies) {
                // 被执行实体
                resultMap.put("tyc_unique_entity_id_subject_to_enforcement", entity.getTycUniqueEntityId());
                resultMap.put("entity_name_valid_subject_to_enforcement", entity.getEntityName());
                resultMap.put("entity_type_id_subject_to_enforcement", entity.getEntityType());
                // 候选实体
                resultMap.put("tyc_unique_entity_id_candidate_evaluation_institution", agency.f0);
                resultMap.put("entity_name_valid_selected_evaluation_institution", agency.f1.replaceAll("\\s", ""));
                resultMap.put("entity_type_id_candidate_evaluation_institution", 1);
                // sql
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
                String sql = new SQL().INSERT_INTO(TABLE).INTO_COLUMNS(insert.f0).INTO_VALUES(insert.f1).toString();
                sqls.add(sql);
            }
        }
        return sqls;
    }

    private String formatString(Object obj) {
        return TycUtils.isValidName(obj) ? String.valueOf(obj) : "";
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private final static class Entity {
        private String tycUniqueEntityId;
        private String EntityName;
        private String EntityType;
    }
}
