package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@LocalConfigFile("relation.yml")
public class RelationJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> e.getColumnMap().get("id"))
                .flatMap(new RelationMapper(config))
                .returns(TypeInformation.of(new TypeHint<List<String>>() {
                }))
                .name("RelationMapper")
                .uid("RelationMapper")
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .keyBy(e -> e.get(0))
                .addSink(new RelationSink(config))
                .name("RelationSink")
                .uid("RelationSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RelationJob");
    }

    @RequiredArgsConstructor
    private static final class RelationMapper extends RichFlatMapFunction<SingleCanalBinlog, List<String>> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<List<String>> out) {
//            String table = singleCanalBinlog.getTable();
//            switch (table) {
//                case "company_legal_person":
//                    parseLegalPerson(singleCanalBinlog, out);
//                    break;
//                case "entity_controller_details_new":
//                    parseController(singleCanalBinlog, out);
//                    break;
//                case "company_equity_relation_details":
//                    parseShareholder(singleCanalBinlog, out);
//                    break;
//                case "company_branch":
//                    parseBranch(singleCanalBinlog, out);
//                    break;
//                default:
//                    log.error("unknown table: {}", table);
//                    throw new RuntimeException("wrong table: " + table);
//            }
        }

        private void parseLegalPerson(SingleCanalBinlog singleCanalBinlog, Collector<List<String>> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            if (!beforeColumnMap.isEmpty()) {
                String pid = (String) beforeColumnMap.get("legal_rep_human_id");
                String gid = (String) beforeColumnMap.get("legal_rep_name_id");
                String id;
                if (TycUtils.isTycUniqueEntityId(pid)) {
                    id = pid;
                } else if (TycUtils.isUnsignedId(gid)) {
                    id = gid;
                } else {
                    id = null;
                }
                String companyId = (String) beforeColumnMap.get("company_id");
                String identity = (String) beforeColumnMap.get("legal_rep_display_name");
                if (TycUtils.isTycUniqueEntityId(id) && TycUtils.isUnsignedId(companyId)) {
                    out.collect(Arrays.asList(id, companyId, "LEGAL", identity, "DELETE"));
                }
            }
            if (!afterColumnMap.isEmpty()) {
                String pid = (String) afterColumnMap.get("legal_rep_human_id");
                String gid = (String) afterColumnMap.get("legal_rep_name_id");
                String id;
                if (TycUtils.isTycUniqueEntityId(pid)) {
                    id = pid;
                } else if (TycUtils.isUnsignedId(gid)) {
                    id = gid;
                } else {
                    id = null;
                }
                String companyId = (String) afterColumnMap.get("company_id");
                String identity = (String) afterColumnMap.get("legal_rep_display_name");
                if (TycUtils.isTycUniqueEntityId(id) && TycUtils.isUnsignedId(companyId)) {
                    out.collect(Arrays.asList(id, companyId, "LEGAL", identity, "INSERT"));
                }
            }
        }

        private void parseController(SingleCanalBinlog singleCanalBinlog, Collector<List<String>> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            if (!beforeColumnMap.isEmpty()) {
                String shareholderId = (String) beforeColumnMap.get("tyc_unique_entity_id");
                String companyId = (String) beforeColumnMap.get("company_id_controlled");
                out.collect(Arrays.asList(shareholderId, companyId, "CONTROL", "", "DELETE"));
            }
            if (!afterColumnMap.isEmpty()) {
                String shareholderId = (String) afterColumnMap.get("tyc_unique_entity_id");
                String companyId = (String) afterColumnMap.get("company_id_controlled");
                out.collect(Arrays.asList(shareholderId, companyId, "CONTROL", "", "INSERT"));
            }
        }

        private void parseShareholder(SingleCanalBinlog singleCanalBinlog, Collector<List<String>> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            if (!beforeColumnMap.isEmpty()) {
                String shareholderId = (String) beforeColumnMap.get("shareholder_id");
                String companyId = (String) beforeColumnMap.get("company_id");
                out.collect(Arrays.asList(shareholderId, companyId, "INVEST", "", "DELETE"));
            }
            if (!afterColumnMap.isEmpty()) {
                String shareholderId = (String) afterColumnMap.get("shareholder_id");
                String companyId = (String) afterColumnMap.get("company_id");
                out.collect(Arrays.asList(shareholderId, companyId, "INVEST", "", "INSERT"));
            }
        }

        private void parseBranch(SingleCanalBinlog singleCanalBinlog, Collector<List<String>> out) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            if (!beforeColumnMap.isEmpty()) {
                String branchCompanyId = (String) beforeColumnMap.get("branch_company_id");
                String companyId = (String) beforeColumnMap.get("company_id");
                if (TycUtils.isUnsignedId(branchCompanyId) && TycUtils.isUnsignedId(companyId)) {
                    out.collect(Arrays.asList(branchCompanyId, companyId, "BRANCH", "", "DELETE"));
                }
            }
            if (!afterColumnMap.isEmpty()) {
                String branchCompanyId = (String) afterColumnMap.get("branch_company_id");
                String companyId = (String) afterColumnMap.get("company_id");
                String isDeleted = (String) afterColumnMap.get("is_deleted");
                if (TycUtils.isUnsignedId(branchCompanyId) && TycUtils.isUnsignedId(companyId)) {
                    if ("0".equals(isDeleted)) {
                        out.collect(Arrays.asList(branchCompanyId, companyId, "BRANCH", "", "INSERT"));
                    } else {
                        out.collect(Arrays.asList(branchCompanyId, companyId, "BRANCH", "", "DELETE"));
                    }
                }
            }
        }
    }

    @RequiredArgsConstructor
    private static final class RelationSink extends RichSinkFunction<List<String>> implements CheckpointedFunction {
        private final Config config;
        private ObsWriter obsWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            obsWriter = new ObsWriter("obs://hadoop-obs/flink/relation/edge/", ObsWriter.FileFormat.TXT);
            obsWriter.enableCache();
        }

        @Override
        public void invoke(List<String> values, Context context) {
            String row = values.subList(0, 4).stream()
                    .map(value -> value.replaceAll("[\"',\\s]", ""))
                    .collect(Collectors.joining(","));
            obsWriter.update(row);
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
}
