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
import com.liang.flink.service.equity.controller.EquityControlService;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@LocalConfigFile("equity-control.yml")
public class EquityControlJob {
    private static final String SINK_SOURCE = "427.test";
    private static final String SINK_TABLE = "bdp_equity.entity_controller_details_new";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .rebalance()
                .map(new EquityControlMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityControlMapper")
                .uid("EquityControlMapper")
                .keyBy(e -> e)
                .addSink(new EquityControlSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityControlSink")
                .uid("EquityControlSink");
        env.execute("EquityControlJob");
    }

    @RequiredArgsConstructor
    private static final class EquityControlMapper extends RichMapFunction<SingleCanalBinlog, String> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public String map(SingleCanalBinlog singleCanalBinlog) {
            String table = singleCanalBinlog.getTable();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            if (table.contains("company_index")) {
                return String.valueOf(columnMap.get("company_id"));
            } else if (table.contains("ratio_path_company")) {
                return String.valueOf(columnMap.get("company_id"));
            }
            return "";
        }
    }

    @RequiredArgsConstructor
    private static final class EquityControlSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Set<String> companyIdBuffer = new HashSet<>();
        private final Config config;
        private EquityControlService service;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new EquityControlService();
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
                    List<Map<String, Object>> columnMaps = service.processControl(companyId);
                    String deleteSql = new SQL()
                            .DELETE_FROM(SINK_TABLE)
                            .WHERE("company_id_controlled = " + SqlUtils.formatValue(companyId))
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
