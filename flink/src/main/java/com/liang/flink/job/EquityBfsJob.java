package com.liang.flink.job;

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
import com.liang.flink.service.equity.bfs.EquityBfsService;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.*;

@LocalConfigFile("equity-bfs.yml")
public class EquityBfsJob {
    private static final String SINK_SOURCE = "457.prism_shareholder_path";
    private static final String SINK_TABLE = "prism_shareholder_path.ratio_path_company_new";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                // 根据股东id, 查询所有被投资公司
                .rebalance()
                .flatMap(new EquityBfsFlatMapper(config))
                .setParallelism(16)
                .name("EquityBfsFlatMapper")
                .uid("EquityBfsFlatMapper")
                // 向上穿透
                .keyBy(companyId -> companyId)
                .flatMap(new EquityBfsCalculator(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityBfsCalculator")
                .uid("EquityBfsCalculator")
                // 写入mysql
                .keyBy(sqls -> sqls.get(0))
                .addSink(new EquityBfsSink(config))
                .setParallelism(16)
                .name("EquityBfsSink")
                .uid("EquityBfsSink");
        env.execute("EquityBfsJob");
    }

    /**
     * 查询所有可能需要重新穿透的公司
     */
    @RequiredArgsConstructor
    private static final class EquityBfsFlatMapper extends RichFlatMapFunction<SingleCanalBinlog, String> {
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate(SINK_SOURCE);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            String database = singleCanalBinlog.getDatabase();
            String table = singleCanalBinlog.getTable();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            Set<String> entityIds = new LinkedHashSet<>();
            // 公司维表 ee59d.proto.company_base.company_index
            if (table.contains("company_index")) {
                entityIds.add(String.valueOf(columnMap.get("company_id")));
            }
            // 注吊销标签表 a3540.proto.company_base.bdp_company_profile_tag_details_total
            else if (table.contains("bdp_company_profile_tag_details_total")) {
                entityIds.add(String.valueOf(columnMap.get("company_id")));
            }
            // 公司类型细分表 c8ce5.proto.company_base.tyc_entity_general_property_reference
            else if (table.contains("tyc_entity_general_property_reference")) {
                entityIds.add(String.valueOf(columnMap.get("tyc_unique_entity_id")));
            }
            // 股东 1ae09.proto.graph_data.company_equity_relation_details
            else if (table.contains("company_equity_relation_details")) {
                entityIds.add(String.valueOf(columnMap.get("company_id_invested")));
                entityIds.add(String.valueOf(columnMap.get("tyc_unique_entity_id_investor")));
            }
            // 主要人员 ee59d.proto.company_base.personnel
            else if (table.contains("personnel")) {
                entityIds.add(String.valueOf(columnMap.get("company_id")));
                entityIds.add(String.valueOf(columnMap.get("human_id")));
            }
            // 法人 ee59d.proto.company_base.company_legal_person
            else if (table.contains("company_legal_person")) {
                entityIds.add(String.valueOf(columnMap.get("company_id")));
                entityIds.add(String.valueOf(columnMap.get("legal_rep_human_id")));
                entityIds.add(String.valueOf(columnMap.get("legal_rep_name_id")));
            }
            // 上市公告 7d89c.proto.data_listed_company.stock_actual_controller
            else if (table.contains("stock_actual_controller")) {
                entityIds.add(String.valueOf(columnMap.get("graph_id")));
                entityIds.add(String.valueOf(columnMap.get("controller_gid")));
                entityIds.add(String.valueOf(columnMap.get("controller_pid")));
            }
            // 分支机构 ee59d.proto.company_base.company_branch
            else if (table.contains("company_branch")) {
                entityIds.add(String.valueOf(columnMap.get("company_id")));
                entityIds.add(String.valueOf(columnMap.get("branch_company_id")));
            }
            // 老板维表 36c60.proto.human_base.human
            else if (database.contains("human_base") && table.contains("human")) {
                entityIds.add(String.valueOf(columnMap.get("human_id")));
            }
            // 老板公司关系表 9bc47.proto.prism_boss.company_human_relation
            else if (table.contains("company_human_relation")) {
                entityIds.add(String.valueOf(columnMap.get("company_graph_id")));
                entityIds.add(String.valueOf(columnMap.get("human_pid")));
            }
            // 上市板块 9bc47.proto.listed_base.company_bond_plates
            else if (table.contains("company_bond_plates")) {
                entityIds.add(String.valueOf(columnMap.get("company_id")));
            }
            for (String entityId : entityIds) {
                if (!TycUtils.isTycUniqueEntityId(entityId)) {
                    continue;
                }
                if (TycUtils.isUnsignedId(entityId)) {
                    out.collect(entityId);
                }
                // 全量repair的时候不走这里
                if (config.getFlinkConfig().getSourceType() != FlinkConfig.SourceType.Repair) {
                    String sql = new SQL()
                            .SELECT("distinct company_id")
                            .FROM(SINK_TABLE)
                            .WHERE("shareholder_id = " + SqlUtils.formatValue(entityId))
                            .toString();
                    sink.queryForList(sql, rs -> rs.getString(1))
                            .forEach(out::collect);
                }
            }
        }
    }

    /**
     * 股权穿透
     */
    @RequiredArgsConstructor
    private static final class EquityBfsCalculator extends RichFlatMapFunction<String, List<String>> implements CheckpointedFunction {
        private static final int CACHE_SIZE = 1024;
        private final Set<String> companyIdBuffer = new HashSet<>();
        private final Config config;
        private EquityBfsService service;
        private Collector<List<String>> out;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new EquityBfsService();
        }

        @Override
        public void flatMap(String companyId, Collector<List<String>> out) {
            synchronized (companyIdBuffer) {
                if (this.out == null) {
                    this.out = out;
                }
                if (!TycUtils.isUnsignedId(companyId)) {
                    return;
                }
                companyIdBuffer.add(companyId);
                if (companyIdBuffer.size() >= CACHE_SIZE) {
                    flush();
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush();
        }

        @Override
        public void close() {
            flush();
        }

        public void flush() {
            synchronized (companyIdBuffer) {
                for (String companyId : companyIdBuffer) {
                    List<Map<String, Object>> columnMaps = service.bfs(companyId);
                    String deleteSql = new SQL()
                            .DELETE_FROM(SINK_TABLE)
                            .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                            .toString();
                    if (columnMaps.isEmpty()) {
                        out.collect(Collections.singletonList(deleteSql));
                        continue;
                    }
                    Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMaps);
                    String insertSql = new SQL()
                            .INSERT_INTO(SINK_TABLE)
                            .INTO_COLUMNS(insert.f0)
                            .INTO_VALUES(insert.f1)
                            .toString();
                    out.collect(Arrays.asList(deleteSql, insertSql));
                }
                companyIdBuffer.clear();
            }
        }
    }

    /**
     * 写入Mysql
     */
    @RequiredArgsConstructor
    private static final class EquityBfsSink extends RichSinkFunction<List<String>> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate(SINK_SOURCE);
            sink.enableCache();
        }

        @Override
        public void invoke(List<String> sqls, Context context) {
            sink.update(sqls);
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

        public void flush() {
            sink.flush();
        }
    }
}
