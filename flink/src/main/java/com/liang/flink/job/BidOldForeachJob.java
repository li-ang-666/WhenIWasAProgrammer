package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@LocalConfigFile("bid-old-foreach.yml")
public class BidOldForeachJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new BidOldForeachSink(config))
                .name("BidOldForeachSink")
                .uid("BidOldForeachSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("BidOldForeachJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class BidOldForeachSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final static Set<Object> ids = new HashSet<>();
        private final Config config;
        JdbcTemplate jdbcTemplate;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("104.data_bid");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            synchronized (ids) {
                ids.add(singleCanalBinlog.getColumnMap().get("id"));
                if (ids.size() >= 1024) {
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

        private void flush() {
            synchronized (ids) {
                if (!ids.isEmpty()) {
                    String sql = new SQL().UPDATE("company_bid")
                            .SET("update_time = now()")
                            .WHERE("id in " + ids.stream().map(String::valueOf).collect(Collectors.joining(",", "(", ")")))
                            .toString();
                    jdbcTemplate.update(sql);
                    ids.clear();
                }
            }
        }
    }
}
