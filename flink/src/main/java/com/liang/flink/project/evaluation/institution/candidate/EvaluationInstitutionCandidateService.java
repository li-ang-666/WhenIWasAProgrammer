package com.liang.flink.project.evaluation.institution.candidate;

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
        // 查询evaluate
        Map<String, Object> evaluate = dao.getEvaluate(evaluateId);
        // evaluate为空, 跳出
        if (evaluate.isEmpty()) {
            return sqls;
        }
        // 查询index
        Map<String, Object> evaluateIndex = dao.getEvaluateIndex(evaluateId);
        // evaluate_index为空, 跳出
        if (evaluateIndex.isEmpty()) {
            return sqls;
        }
        // 提取被执行实体
        List<Entity> entities = new ArrayList<>();
        String gid = String.valueOf(evaluateIndex.get("gid"));
        // 如果index.gid不为0, 则代表是公司
        if (TycUtils.isUnsignedId(gid)) {
            // 查询最新名字
            String newCompanyName = dao.companyGid2Name(gid);
            // 名字不合法, 跳出
            if (!TycUtils.isValidName(newCompanyName)) {
                return sqls;
            }
            // 国家机关, 跳出
            if (dao.isStateOrgans(gid)) {
                return sqls;
            }
            entities.add(new Entity(gid, newCompanyName, "1"));
        }
        // 否则, 走自然人的逻辑
        else {
            String ename = String.valueOf(evaluate.get("ename"));
            // 名字不合法, 跳出
            if (!TycUtils.isValidName(ename)) {
                return sqls;
            }
            // 查询名字对应的human_pid
            List<Map<String, Object>> companyLawHumanRelations = dao.getCompanyLawHumanRelations(evaluateId, ename);
            for (Map<String, Object> companyLawHumanRelation : companyLawHumanRelations) {
                entities.add(new Entity(String.valueOf(companyLawHumanRelation.get("human_id")), ename, "2"));
            }
            // 如果查不到pid, 插入一条非人非公司
            if (entities.isEmpty()) {
                entities.add(new Entity("", ename, "3"));
            }
        }
        // 提取候选机构
        String json = String.valueOf(evaluateIndex.get("evaluationAgency"));
        List<Tuple2<String, String>> agencies = JsonUtils.parseJsonArr(json).stream()
                .map(e -> {
                    String eGid = String.valueOf(((Map<String, Object>) e).get("gid"));
                    String name = dao.companyGid2Name(eGid);
                    return Tuple2.of(eGid, name);
                })
                // 候选机构只保留合法id、合法名称、非国家机构的公司
                .filter(e -> TycUtils.isUnsignedId(e.f0) && TycUtils.isValidName(e.f1) && !dao.isStateOrgans(e.f0))
                .collect(Collectors.toList());
        // 没有合法候选机构, 跳出
        if (agencies.isEmpty()) {
            return sqls;
        }
        HashMap<String, Object> resultMap = new HashMap<>();
        // id
        resultMap.put("data_source_trace_id", evaluateId);
        // 执行案号 & 执行类型
        String caseNumber = String.valueOf(evaluate.get("caseNumber"));
        String caseNumberClean = caseCodeClean.evaluate(caseNumber);
        String caseType = caseCodeType.evaluate(caseNumberClean);
        if (caseNumber.matches(".*?([法减假刑]).*") || caseNumberClean.matches(".*?([法减假刑]).*") || caseType.matches(".*?((刑事)|(国家赔偿)).*")) {
            return sqls;
        }
        // 执行案号(清洗)
        resultMap.put("enforcement_case_number", formatString(caseNumberClean));
        // 执行案号(原始)
        resultMap.put("enforcement_case_number_original", formatString(caseNumber));
        // 执行案型(清洗)
        resultMap.put("enforcement_case_type", formatString(caseType));
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
        // 写入
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
