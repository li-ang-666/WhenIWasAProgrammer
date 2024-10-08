package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.ConfigUtils;
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

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        LEGAL, AC, INVEST, BRANCH
    }

    @RequiredArgsConstructor
    private static final class RelationEdgeMapper extends RichFlatMapFunction<SingleCanalBinlog, Row> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<Row> out) {
            String table = singleCanalBinlog.getTable();
            switch (table) {
                case "company_legal_person":
                    parseLegalPerson(singleCanalBinlog, out);
                    break;
                case "entity_controller_details_new":
                    parseController(singleCanalBinlog, out);
                    break;
                case "company_equity_relation_details":
                    parseShareholder(singleCanalBinlog, out);
                    break;
                case "company_branch":
                    parseBranch(singleCanalBinlog, out);
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
        public void close() {
            obsWriter.flush();
        }

        @Override
        public void finish() {
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
