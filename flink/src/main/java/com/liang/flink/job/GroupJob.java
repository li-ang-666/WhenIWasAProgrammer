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
import com.liang.flink.service.group.GroupService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;
import java.util.Map;

@Slf4j
@LocalConfigFile("group.yml")
public class GroupJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .rebalance()
                .addSink(new GroupSink(config))
                .name("GroupSink")
                .uid("GroupSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("GroupJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class GroupSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private GroupService service;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new GroupService();
            sink = new JdbcTemplate("427.test");
            sink.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String companyId = String.valueOf(columnMap.get("company_id"));
            List<Map<String, Object>> columnMaps = service.tryCreateGroup(companyId);
            String deleteSql = new SQL().DELETE_FROM("tyc_group")
                    .WHERE("group_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            sink.update(deleteSql);
            for (Map<String, Object> sinkColumnMap : columnMaps) {
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(sinkColumnMap);
                String insertSql = new SQL().INSERT_INTO("tyc_group")
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                sink.update(insertSql);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            sink.flush();
        }

        @Override
        public void finish() {
            sink.flush();
        }

        @Override
        public void close() {
            sink.flush();
        }
    }
}
