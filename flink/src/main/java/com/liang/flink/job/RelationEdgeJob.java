package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.storage.ObsWriter;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
hdfs dfs -rm -r -f -skipTrash obs://hadoop-obs/flink/relation/edge/


drop table if exists test.relation_edge;
create external table if not exists test.relation_edge(
  `row` string
)stored as textfile location 'obs://hadoop-obs/flink/relation/edge';


select count(1) from test.relation_edge;
*/
// insert overwrite table test.relation_edge select /*+ REPARTITION(12) */ * from test.relation_edge;
@Slf4j
@LocalConfigFile("relation-edge.yml")
public class RelationEdgeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> e.getColumnMap().get("id"))
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
    private static final class RelationEdgeMapper extends RichFlatMapFunction<SingleCanalBinlog, Row> {
        private final Config config;
        private final Map<String, String> dictionary = new HashMap<>();
        private JdbcTemplate prismBoss157;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            prismBoss157 = new JdbcTemplate("157.prism_boss");
            dictionary.put("1", "法定代表人");
            dictionary.put("2", "负责人");
            dictionary.put("3", "经营者");
            dictionary.put("4", "投资人");
            dictionary.put("5", "执行事务合伙人");
            dictionary.put("6", "法定代表人|负责人");
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<Row> out) {
            String table = singleCanalBinlog.getTable();
            switch (table) {
                case "company_legal_person":
                    // 法人
                    parseLegalPerson(singleCanalBinlog, out);
                    break;
                case "entity_controller_details_new":
                    // 实控人
                    parseController(singleCanalBinlog, out);
                    break;
                case "company_equity_relation_details":
                    // 股东
                    parseShareholder(singleCanalBinlog, out);
                    break;
                case "company_branch":
                    // 分支机构
                    parseBranch(singleCanalBinlog, out);
                    break;
                case "entity_investment_history_fusion_details":
                    // 历史股东
                    parseHisShareholder(singleCanalBinlog, out);
                    break;
                case "entity_legal_rep_list_total":
                    // 历史法人
                    parseHisLegalPerson(singleCanalBinlog, out);
                    break;
                default:
                    log.error("wrong table: {}", table);
            }
        }

        // 法人 -> 公司
        private void parseLegalPerson(SingleCanalBinlog singleCanalBinlog, Collector<Row> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String pid = (String) columnMap.get("legal_rep_human_id");
                String gid = (String) columnMap.get("legal_rep_name_id");
                String id = TycUtils.isTycUniqueEntityId(pid) ? pid : gid;
                String companyId = (String) columnMap.get("company_id");
                String identity = (String) columnMap.get("legal_rep_display_name");
                return new Row(id, companyId, Relation.LEGAL, identity, null);
            };
            if (!beforeColumnMap.isEmpty())
                out.collect(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty())
                out.collect(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 实控人 -> 公司
        private void parseController(SingleCanalBinlog singleCanalBinlog, Collector<Row> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String shareholderId = (String) columnMap.get("tyc_unique_entity_id");
                String companyId = (String) columnMap.get("company_id_controlled");
                return new Row(shareholderId, companyId, Relation.AC, "", null);
            };
            if (!beforeColumnMap.isEmpty())
                out.collect(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty())
                out.collect(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 股东 -> 公司
        private void parseShareholder(SingleCanalBinlog singleCanalBinlog, Collector<Row> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String shareholderId = (String) columnMap.get("shareholder_id");
                String companyId = (String) columnMap.get("company_id");
                String equityRatio = (String) columnMap.get("equity_ratio");
                return new Row(shareholderId, companyId, Relation.INVEST, equityRatio, null);
            };
            if (!beforeColumnMap.isEmpty())
                out.collect(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty())
                out.collect(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 分公司 -> 总公司
        private void parseBranch(SingleCanalBinlog singleCanalBinlog, Collector<Row> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String branchCompanyId = (String) columnMap.get("branch_company_id");
                String companyId = (String) columnMap.get("company_id");
                return new Row(branchCompanyId, companyId, Relation.BRANCH, "", null);
            };
            if (!beforeColumnMap.isEmpty())
                out.collect(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty() && "0".equals(afterColumnMap.get("is_deleted")))
                out.collect(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 历史股东 -> 公司
        private void parseHisShareholder(SingleCanalBinlog singleCanalBinlog, Collector<Row> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String shareholderGid = (String) columnMap.get("entity_name_id");
                String shareholderType = (String) columnMap.get("entity_type_id");
                String companyId = (String) columnMap.get("company_id_invested");
                String shareholderId = "2".equals(shareholderType) ? queryPid(companyId, shareholderGid) : shareholderGid;
                String investmentRatio = StrUtil.nullToDefault((String) columnMap.get("investment_ratio"), "");
                return new Row(shareholderId, companyId, Relation.HIS_INVEST, investmentRatio, null);
            };
            if (!beforeColumnMap.isEmpty())
                out.collect(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty() && "0".equals(afterColumnMap.get("delete_status")))
                out.collect(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 历史法人 -> 公司
        private void parseHisLegalPerson(SingleCanalBinlog singleCanalBinlog, Collector<Row> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String shareholderId = (String) columnMap.get("tyc_unique_entity_id_legal_rep");
                String companyId = (String) columnMap.get("tyc_unique_entity_id");
                String displayId = (String) columnMap.get("legal_rep_type_display_name");
                String displayName = dictionary.getOrDefault(displayId, "法定代表人|负责人");
                return new Row(shareholderId, companyId, Relation.HIS_LEGAL, displayName, null);
            };
            if (!beforeColumnMap.isEmpty())
                out.collect(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty() && "0".equals(afterColumnMap.get("delete_status")) && "1".equals(afterColumnMap.get("is_history_legal_rep")))
                out.collect(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
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
    }

    @RequiredArgsConstructor
    private static final class RelationEdgeSink extends RichSinkFunction<Row> implements CheckpointedFunction {
        private final Config config;
        private ObsWriter obsWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            obsWriter = new ObsWriter("obs://hadoop-obs/flink/relation/edge/", ObsWriter.FileFormat.CSV);
            obsWriter.enableCache();
        }

        @Override
        public void invoke(Row row, Context context) {
            if (row.isValid()) {
                String str = Stream.of(row.getId(), row.getCompanyId(), row.getRelation(), row.getOther())
                        .map(value -> String.valueOf(value).replaceAll("[\"',\\s]", ""))
                        .collect(Collectors.joining(","));
                obsWriter.update(str);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            obsWriter.flush();
        }

        @Override
        public void finish() {
            obsWriter.flush();
        }


        @Override
        public void close() {
            obsWriter.flush();
        }
    }

    @Data
    @AllArgsConstructor
    @Accessors(chain = true)
    private static final class Row {
        private String id;
        private String companyId;
        private Relation relation;
        private String other;
        private CanalEntry.EventType opt;

        public boolean isValid() {
            return TycUtils.isTycUniqueEntityId(id) && TycUtils.isUnsignedId(companyId);
        }
    }
}
