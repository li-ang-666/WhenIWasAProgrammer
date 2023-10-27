package com.liang.flink.project.multi.node;


import com.liang.common.service.SQL;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.job.MultiNodeJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@SuppressWarnings("unchecked")
public class MultiNodeService {
    private final static String CONTROL_SINK = "control_graph_analysis_through_multi_control_path";
    private final static String BENEFIT_SINK = "beneficiary_graph_analysis_through_multi_beneficiary_path";

    private final MultiNodeDao dao = new MultiNodeDao();

    public List<String> invoke(MultiNodeJob.Input input) {
        String module = input.getModule();
        String tycUniqueEntityId = input.getId();
        String entityNameValid = input.getName();
        // 公司名称维表
        if ("name".equals(module) && TycUtils.isValidName(entityNameValid)) {
            String updateSql1 = new SQL().UPDATE(CONTROL_SINK)
                    .SET("entity_name_valid = " + SqlUtils.formatValue(entityNameValid))
                    .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                    .toString();
            String updateSql2 = new SQL().UPDATE(BENEFIT_SINK)
                    .SET("entity_name_valid = " + SqlUtils.formatValue(entityNameValid))
                    .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                    .toString();
            return Arrays.asList(updateSql1, updateSql2);
        }
        // 实控 & 受益 共同代码
        List<String> sqls = new ArrayList<>();
        String deleteSQL = new SQL().DELETE_FROM("control".equals(module) ? CONTROL_SINK : BENEFIT_SINK)
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                .toString();
        sqls.add(deleteSQL);
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("tyc_unique_entity_id", tycUniqueEntityId);
        String name = TycUtils.isUnsignedId(tycUniqueEntityId) ? dao.getName(tycUniqueEntityId) : dao.getHumanName(tycUniqueEntityId);
        if (!TycUtils.isValidName(name)) {
            return sqls;
        }
        columnMap.put("entity_name_valid", name);
        columnMap.put("entity_type_id", TycUtils.isUnsignedId(tycUniqueEntityId) ? "1" : "2");
        // 实控相关
        if ("control".equals(module)) {
            List<String> types = new ArrayList<>();
            if (TycUtils.isUnsignedId(tycUniqueEntityId)) {
                types.add("实际控制人");
                types.add("实际控制权");
            } else {
                types.add("实际控制权");
            }
            for (String type : types) {
                Tuple3<Integer, Integer, Integer> tp3 = "实际控制人".matches(type)
                        ? parseJsonList(dao.getControlJsonByCompanyId(tycUniqueEntityId), true)
                        : parseJsonList(dao.getControlJsonByShareholderId(tycUniqueEntityId), false);
                columnMap.put("control_graph_data_application_type", type);
                columnMap.put("total_entity_cnt_through_multi_control_path", tp3.f0);
                columnMap.put("min_graph_tier_through_multi_control_path", tp3.f1);
                columnMap.put("max_graph_tier_through_multi_control_path", tp3.f2);
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                String insertSql = new SQL().INSERT_INTO(CONTROL_SINK)
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                sqls.add(insertSql);
            }
        }
        // 受益相关
        else {
            String type = TycUtils.isUnsignedId(tycUniqueEntityId) ? "受益所有人" : "受益所有权";
            Tuple3<Integer, Integer, Integer> tp3 = "受益所有人".matches(type)
                    ? parseJsonList(dao.getBenefitJsonByCompanyId(tycUniqueEntityId), true)
                    : parseJsonList(dao.getBenefitJsonByShareholderId(tycUniqueEntityId), false);
            columnMap.put("beneficiary_graph_data_application_type", type);
            columnMap.put("total_entity_cnt_through_multi_beneficiary_path", tp3.f0);
            columnMap.put("min_graph_tier_through_multi_beneficiary_path", tp3.f1);
            columnMap.put("max_graph_tier_through_multi_beneficiary_path", tp3.f2);
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            String insertSql = new SQL().INSERT_INTO(BENEFIT_SINK)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sqls.add(insertSql);
        }
        return sqls;
    }

    private Tuple3<Integer, Integer, Integer> parseJsonList(List<String> jsonList, boolean needReverse) {
        Map<String, Set<Tuple2<String, Integer>>> id2Levels = new HashMap<>();
        for (String json : jsonList) {
            List<Object> chains = JsonUtils.parseJsonArr(json);
            for (Object chain : chains) {
                List<Map<String, Object>> nodes = new ArrayList<>(((List<Map<String, Object>>) chain));
                if (needReverse) {
                    Collections.reverse(nodes);
                }
                String linkAll = "";
                // 掐掉第0层
                for (int i = 2; i < nodes.size(); i += 2) {
                    Map<String, Object> link = nodes.get(i - 1);
                    linkAll += link;
                    Map<String, Object> node = nodes.get(i);
                    String id = String.valueOf(node.get("id"));
                    id2Levels.putIfAbsent(id, new HashSet<>());
                    id2Levels.get(id).add(Tuple2.of(linkAll, i / 2));
                }
                log.debug("nodes: {}", nodes);
            }
        }
        log.debug("id2Levels: {}", id2Levels);
        AtomicInteger num = new AtomicInteger(0);
        AtomicInteger min = new AtomicInteger(Integer.MAX_VALUE);
        AtomicInteger max = new AtomicInteger(Integer.MIN_VALUE);
        id2Levels.forEach((k, v) -> {
            if (v.size() > 1) {
                num.getAndIncrement();
                v.forEach(e -> {
                    min.set(Math.min(min.get(), e.f1));
                    max.set(Math.max(max.get(), e.f1));
                });
            }
        });
        Tuple3<Integer, Integer, Integer> res = num.get() != 0
                ? Tuple3.of(num.get(), min.get(), max.get())
                : Tuple3.of(0, 0, 0);
        log.debug("res: {}", res);
        return res;
    }
}
