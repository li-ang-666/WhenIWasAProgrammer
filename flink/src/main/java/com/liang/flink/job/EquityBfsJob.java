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
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.*;
import java.util.stream.Collectors;

@LocalConfigFile("equity-bfs.yml")
public class EquityBfsJob {
    private static final String SINK_SOURCE = "491.prism_shareholder_path";
    private static final String SINK_TABLE = "ratio_path_company_new";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                // 根据股东id, 查询所有可能需要重新穿透的公司
                .rebalance()
                .flatMap(new EquityBfsFlatMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel() / 8)
                .name("EquityBfsFlatMapper")
                .uid("EquityBfsFlatMapper")
                // 股权穿透
                .keyBy(companyId -> companyId)
                .flatMap(new EquityBfsCalculator(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityBfsCalculator")
                .uid("EquityBfsCalculator")
                // 写入mysql
                .keyBy(companyIdAndColumnMaps -> companyIdAndColumnMaps.f0)
                .addSink(new EquityBfsSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel() / 8)
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
            // 股东 1ae09.proto.graph_data.company_equity_relation_details
            else if (table.contains("company_equity_relation_details")) {
                entityIds.add(String.valueOf(columnMap.get("company_id_invested")));
                entityIds.add(String.valueOf(columnMap.get("tyc_unique_entity_id_investor")));
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
            for (String entityId : entityIds) {
                if (!TycUtils.isTycUniqueEntityId(entityId)) {
                    continue;
                }
                if (TycUtils.isUnsignedId(entityId)) {
                    out.collect(entityId);
                }
                // 全量repair的时候不走这里
                if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.Repair) {
                    continue;
                }
                List<String> sqls = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    String sql = String.format("select company_id from %s_%s where shareholder_id = %s", SINK_TABLE, i, SqlUtils.formatValue(entityId));
                    sqls.add(sql);
                }
                String sql = sqls.stream().collect(Collectors.joining(" union all ", "select company_id from (", ") t"));
                sink.queryForList(sql, rs -> rs.getString(1))
                        .forEach(out::collect);
            }
        }
    }

    /**
     * 股权穿透
     */
    @RequiredArgsConstructor
    private static final class EquityBfsCalculator extends RichFlatMapFunction<String, Tuple2<String, List<Map<String, Object>>>> implements CheckpointedFunction {
        private final Roaring64Bitmap bitmap = new Roaring64Bitmap();
        private final Config config;
        private EquityBfsService service;
        private Collector<Tuple2<String, List<Map<String, Object>>>> collector;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new EquityBfsService();
        }

        @Override
        public void flatMap(String companyId, Collector<Tuple2<String, List<Map<String, Object>>>> out) {
            synchronized (bitmap) {
                if (collector == null) {
                    collector = out;
                }
                if (TycUtils.isUnsignedId(companyId)) {
                    bitmap.add(Long.parseLong(companyId));
                }
                // 全量修复的时候, 来一条计算一条
                if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.Repair) {
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
            synchronized (bitmap) {
                bitmap.forEach(companyId ->
                        collector.collect(Tuple2.of(String.valueOf(companyId), service.bfs(companyId)))
                );
                bitmap.clear();
            }
        }
    }

    /**
     * 写入mysql
     */
    @RequiredArgsConstructor
    private static final class EquityBfsSink extends RichSinkFunction<Tuple2<String, List<Map<String, Object>>>> implements CheckpointedFunction {
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
        public void invoke(Tuple2<String, List<Map<String, Object>>> companyIdAndColumnMaps, Context context) {
            String companyId = companyIdAndColumnMaps.f0;
            List<Map<String, Object>> columnMaps = companyIdAndColumnMaps.f1;
            String sinkTable = String.format("%s_%s", SINK_TABLE, Long.parseLong(companyId) % 100);
            String deleteSql = new SQL()
                    .DELETE_FROM(sinkTable)
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            sink.update(deleteSql);
            for (Map<String, Object> columnMap : columnMaps) {
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                String insertSql = new SQL()
                        .INSERT_INTO(sinkTable)
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                sink.update(insertSql);
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
            sink.flush();
        }
    }
}

