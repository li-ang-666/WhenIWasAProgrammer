package com.liang.flink.job;

import com.liang.common.dto.Config;
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
    private static final String SINK_SOURCE = "427.test";
    private static final String SINK_TABLE = "prism_shareholder_path.ratio_path_company_new";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .rebalance()
                .flatMap(new EquityBfsFlatMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityBfsFlatMapper")
                .uid("EquityBfsFlatMapper")
                .keyBy(e -> e)
                .addSink(new EquityBfsSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityBfsSink")
                .uid("EquityBfsSink");
        env.execute("EquityBfsJob");
    }

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
            Set<String> shareholderIds = new LinkedHashSet<>();
            // 公司维表
            if (table.contains("company_index")) {
                shareholderIds.add(String.valueOf(columnMap.get("company_id")));
            }
            // 股东
            else if (table.contains("company_equity_relation_details")) {
                shareholderIds.add(String.valueOf(columnMap.get("company_id_invested")));
                shareholderIds.add(String.valueOf(columnMap.get("tyc_unique_entity_id_investor")));
            }
            // 主要人员
            else if (table.contains("personnel")) {
                shareholderIds.add(String.valueOf(columnMap.get("company_id")));
                shareholderIds.add(String.valueOf(columnMap.get("human_id")));
            }
            // 法人
            else if (table.contains("company_legal_person")) {
                shareholderIds.add(String.valueOf(columnMap.get("company_id")));
                shareholderIds.add(String.valueOf(columnMap.get("legal_rep_human_id")));
            }
            // 上市公告
            else if (table.contains("stock_actual_controller")) {
                shareholderIds.add(String.valueOf(columnMap.get("graph_id")));
                shareholderIds.add(String.valueOf(columnMap.get("controller_gid")));
                shareholderIds.add(String.valueOf(columnMap.get("controller_pid")));
            }
            // 分支机构
            else if (table.contains("company_branch")) {
                shareholderIds.add(String.valueOf(columnMap.get("company_id")));
                shareholderIds.add(String.valueOf(columnMap.get("branch_company_id")));
            }
            // 老板维表
            else if (database.contains("human_base") && table.contains("human")) {
                shareholderIds.add(String.valueOf(columnMap.get("human_id")));
            }
            for (String shareholderId : shareholderIds) {
                if (!TycUtils.isTycUniqueEntityId(shareholderId)) {
                    continue;
                }
                if (TycUtils.isUnsignedId(shareholderId)) {
                    out.collect(shareholderId);
                }
                String sql = new SQL()
                        .SELECT("distinct company_id")
                        .FROM(SINK_TABLE)
                        .WHERE("shareholder_id = " + SqlUtils.formatValue(shareholderId))
                        .toString();
                sink.queryForList(sql, rs -> rs.getString(1)).forEach(out::collect);
            }
        }
    }

    @RequiredArgsConstructor
    private static final class EquityBfsSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Set<String> companyIdBuffer = new HashSet<>();
        private final Config config;
        private EquityBfsService service;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new EquityBfsService();
            sink = new JdbcTemplate(SINK_SOURCE);
        }

        @Override
        public void invoke(String companyId, Context context) {
            if (!TycUtils.isUnsignedId(companyId)) {
                return;
            }
            synchronized (companyIdBuffer) {
                companyIdBuffer.add(companyId);
                if (companyIdBuffer.size() >= 128) {
                    flush();
                }
            }
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
            synchronized (companyIdBuffer) {
                for (String companyId : companyIdBuffer) {
                    List<Map<String, Object>> columnMaps = service.bfs(companyId);
                    String deleteSql = new SQL()
                            .DELETE_FROM(SINK_TABLE)
                            .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                            .toString();
                    if (columnMaps.isEmpty()) {
                        sink.update(deleteSql);
                        continue;
                    }
                    Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMaps);
                    String insertSql = new SQL()
                            .INSERT_INTO(SINK_TABLE)
                            .INTO_COLUMNS(insert.f0)
                            .INTO_VALUES(insert.f1)
                            .toString();
                    sink.update(deleteSql, insertSql);
                }
                companyIdBuffer.clear();
            }
        }
    }
}
