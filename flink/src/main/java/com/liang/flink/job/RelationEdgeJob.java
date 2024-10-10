package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
beeline

!sh hdfs dfs -rm -r -f -skipTrash obs://hadoop-obs/flink/relation/edge/*

drop table if exists test.relation_edge;
create external table if not exists test.relation_edge(
  `row` string
)stored as textfile location 'obs://hadoop-obs/flink/relation/edge';

*/

// spark-sql

// select count(1) from test.relation_edge;

// insert overwrite table test.relation_edge select /*+ REPARTITION(7) */ * from test.relation_edge;
@Slf4j
@LocalConfigFile("relation-edge.yml")
public class RelationEdgeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .flatMap(new RelationEdgeMapper(config))
                .name("RelationEdgeMapper")
                .uid("RelationEdgeMapper")
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .keyBy(e -> e)
                .addSink(new RelationEdgeSink(config))
                .name("RelationEdgeSink")
                .uid("RelationEdgeSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RelationEdgeJob");
    }

    private enum Relation {
        LEGAL, HIS_LEGAL, AC, HIS_INVEST, INVEST, BRANCH
    }

    @RequiredArgsConstructor
    private static final class RelationEdgeMapper extends RichFlatMapFunction<SingleCanalBinlog, String> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            switch (singleCanalBinlog.getTable()) {
                case "company_index":
                case "company_legal_person":
                case "company_equity_relation_details":
                case "company_branch":
                    out.collect((String) singleCanalBinlog.getColumnMap().get("company_id"));
                    break;
                case "entity_controller_details_new":
                    out.collect((String) singleCanalBinlog.getColumnMap().get("company_id_controlled"));
                    break;
                case "entity_investment_history_fusion_details":
                    out.collect((String) singleCanalBinlog.getColumnMap().get("company_id_invested"));
                    break;
                case "entity_legal_rep_list_total":
                    out.collect((String) singleCanalBinlog.getColumnMap().get("tyc_unique_entity_id"));
                    break;
                case "company_human_relation":
                    out.collect((String) singleCanalBinlog.getColumnMap().get("company_graph_id"));
                    break;
            }
        }
    }

    @RequiredArgsConstructor
    private static final class RelationEdgeSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Config config;
        private final Map<String, String> dictionary = new HashMap<>();
        private final Roaring64Bitmap bitmap = new Roaring64Bitmap();
        private JdbcTemplate prismBoss157;
        private JdbcTemplate companyBase435;
        private JdbcTemplate bdpEquity463;
        private JdbcTemplate bdpPersonnel466;
        private JdbcTemplate graphData430;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            dictionary.put("1", "法定代表人");
            dictionary.put("2", "负责人");
            dictionary.put("3", "经营者");
            dictionary.put("4", "投资人");
            dictionary.put("5", "执行事务合伙人");
            dictionary.put("6", "法定代表人|负责人");
            prismBoss157 = new JdbcTemplate("157.prism_boss");
            companyBase435 = new JdbcTemplate("435.company_base");
            bdpEquity463 = new JdbcTemplate("463.bdp_equity");
            bdpPersonnel466 = new JdbcTemplate("466.bdp_personnel");
            graphData430 = new JdbcTemplate("430.graph_data");
            sink = new JdbcTemplate("427.test");
            sink.enableCache();
        }

        @Override
        public void invoke(String companyId, Context context) {
            synchronized (this) {
                if (TycUtils.isUnsignedId(companyId)) {
                    bitmap.add(Long.parseLong(companyId));
                    if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.Repair) {
                        flush();
                    }
                }
            }
        }

        private void flush() {
            synchronized (this) {
                bitmap.forEach(companyId -> {
                    String deleteSql = new SQL().DELETE_FROM("relation_edge")
                            .WHERE("target_id = " + SqlUtils.formatValue(companyId))
                            .toString();
                    sink.update(deleteSql);
                    ArrayList<Row> results = new ArrayList<>();
                    parseLegalPerson(String.valueOf(companyId), results);
                    parseController(String.valueOf(companyId), results);
                    parseShareholder(String.valueOf(companyId), results);
                    parseBranch(String.valueOf(companyId), results);
                    parseHisShareholder(String.valueOf(companyId), results);
                    parseHisLegalPerson(String.valueOf(companyId), results);
                    List<Map<String, Object>> columnMaps = results.stream()
                            .filter(Row::isValid)
                            .map(Row::toColumnMap)
                            .collect(Collectors.toList());
                    Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMaps);
                    String insertSql = new SQL().INSERT_INTO("relation_edge")
                            .INTO_COLUMNS(insert.f0)
                            .INTO_VALUES(insert.f1)
                            .toString();
                    sink.update(insertSql);
                });
                bitmap.clear();
                sink.flush();
            }
        }

        // 法人 -> 公司
        private void parseLegalPerson(String companyId, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("company_legal_person")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            List<Map<String, Object>> columnMaps = companyBase435.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String type = (String) columnMap.get("legal_rep_type");
                if ("3".equals(type)) {
                    continue;
                }
                String sourceId = "1".equals(type) ? (String) columnMap.get("legal_rep_name_id") : (String) columnMap.get("legal_rep_human_id");
                String sourceName = (String) columnMap.get("legal_rep_name");
                String other = (String) columnMap.get("legal_rep_display_name");
                results.add(new Row(sourceId, sourceName, companyId, Relation.LEGAL, other));
            }
        }


        // 实控人 -> 公司
        private void parseController(String companyId, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("entity_controller_details_new")
                    .WHERE("company_id_controlled = " + SqlUtils.formatValue(companyId))
                    .WHERE("is_controller_tyc_unique_entity_id = 1")
                    .toString();
            List<Map<String, Object>> columnMaps = bdpEquity463.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String sourceId = (String) columnMap.get("tyc_unique_entity_id");
                String sourceName = (String) columnMap.get("entity_name_valid");
                String other = "";
                results.add(new Row(sourceId, sourceName, companyId, Relation.AC, other));
            }
        }

        // 股东 -> 公司
        private void parseShareholder(String companyId, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("company_equity_relation_details")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            List<Map<String, Object>> columnMaps = graphData430.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String type = (String) columnMap.get("shareholder_type");
                if ("3".equals(type)) {
                    continue;
                }
                String sourceId = (String) columnMap.get("shareholder_id");
                String name = (String) columnMap.get("shareholder_name");
                String other = (String) columnMap.get("equity_ratio");
                results.add(new Row(sourceId, name, companyId, Relation.INVEST, other));
            }
        }

        // 分公司 -> 总公司
        private void parseBranch(String companyId, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("company_branch")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .WHERE("is_deleted = 0")
                    .toString();
            List<Map<String, Object>> columnMaps = companyBase435.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String sourceId = (String) columnMap.get("branch_company_id");
                String name = "";
                String other = "";
                results.add(new Row(sourceId, name, companyId, Relation.BRANCH, other));
            }
        }

        // 历史股东 -> 公司
        private void parseHisShareholder(String companyId, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("entity_investment_history_fusion_details")
                    .WHERE("company_id_invested = " + SqlUtils.formatValue(companyId))
                    .WHERE("delete_status = 0")
                    .toString();
            List<Map<String, Object>> columnMaps = bdpEquity463.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String type = (String) columnMap.get("entity_type_id");
                if ("3".equals(type)) {
                    continue;
                }
                String sourceNameId = (String) columnMap.get("entity_name_id");
                String sourceId = "1".equals(type) ? sourceNameId : queryPid(companyId, sourceNameId);
                String name = (String) columnMap.get("entity_name_valid");
                String other = StrUtil.nullToDefault((String) columnMap.get("investment_ratio"), "");
                results.add(new Row(sourceId, name, companyId, Relation.HIS_INVEST, other));
            }
        }

        // 历史法人 -> 公司
        private void parseHisLegalPerson(String companyId, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("entity_legal_rep_list_total")
                    .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(companyId))
                    .WHERE("is_history_legal_rep = 1")
                    .WHERE("delete_status = 0")
                    .toString();
            List<Map<String, Object>> columnMaps = bdpPersonnel466.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String type = (String) columnMap.get("entity_type_id_legal_rep");
                if ("3".equals(type)) {
                    continue;
                }
                String sourceId = (String) columnMap.get("tyc_unique_entity_id_legal_rep");
                String name = (String) columnMap.get("entity_name_valid_legal_rep");
                String other = dictionary.get((String) columnMap.get("legal_rep_type_display_name"));
                results.add(new Row(sourceId, name, companyId, Relation.HIS_LEGAL, other));
            }
        }

        private String queryPid(String companyGid, String humanGid) {
            String sql = new SQL().SELECT("human_pid")
                    .FROM("company_human_relation")
                    .WHERE("company_graph_id = " + SqlUtils.formatValue(companyGid))
                    .WHERE("human_graph_id = " + SqlUtils.formatValue(humanGid))
                    .WHERE("deleted = 0")
                    .toString();
            return prismBoss157.queryForObject(sql, rs -> rs.getString(1));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush();
        }

        @Override
        public void finish() {
            flush();
        }

        @Override
        public void close() {
            flush();
        }
    }

    @Data
    @AllArgsConstructor
    @Accessors(chain = true)
    private static final class Row implements Serializable {
        private String sourceId;
        private String sourceName;
        private String targetId;
        private Relation relation;
        private String other;

        public boolean isValid() {
            return TycUtils.isTycUniqueEntityId(sourceId) && TycUtils.isUnsignedId(targetId);
        }

        public Map<String, Object> toColumnMap() {
            return new HashMap<String, Object>() {{
                put("source_id", sourceId);
                put("source_name", sourceName);
                put("target_id", targetId);
                put("relation", relation.toString());
                put("other", other);
            }};
        }
    }
}
