package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import com.liang.flink.service.equity.bfs.EquityBfsService;
import lombok.RequiredArgsConstructor;
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

@LocalConfigFile("equity-bfs.yml")
public class EquityBfsJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .keyBy(e -> String.valueOf(e.getColumnMap().get("company_id_invested")))
                .addSink(new EquityBfsSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityBfsSink")
                .uid("EquityBfsSink");
        env.execute("EquityBfsJob");
    }

    @RequiredArgsConstructor
    private static final class EquityBfsSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
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
            sink = new JdbcTemplate("427.test");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            if (!"2024".equals(String.valueOf(columnMap.get("reference_pt_year")))) {
                return;
            }
            synchronized (companyIdBuffer) {
                companyIdBuffer.add(String.valueOf(columnMap.get("company_id_invested")));
                if (companyIdBuffer.size() >= 1) {
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
                    if (columnMaps.isEmpty()) {
                        continue;
                    }
                    String deleteSql = new SQL()
                            .DELETE_FROM("prism_shareholder_path.ratio_path_company_new")
                            .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                            .toString();
                    Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMaps);
                    String insertSql = new SQL()
                            .INSERT_INTO("prism_shareholder_path.ratio_path_company_new")
                            .INTO_COLUMNS(insert.f0)
                            .INTO_VALUES(insert.f1)
                            .toString();
                    sink.update(deleteSql, insertSql);
                }
            }
        }
    }
}
