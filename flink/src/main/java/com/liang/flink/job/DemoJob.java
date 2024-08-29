package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.storage.FsParquetWriter;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
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

import java.util.Map;

@Slf4j
@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new DemoSink(config))
                .name("DemoSink")
                .uid("DemoSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("DemoJob");
    }

    @RequiredArgsConstructor
    private final static class DemoSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate jdbcTemplate;
        private ObsWriter obsWriter;
        private FsParquetWriter fsParquetWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("069.semantic_analysis");
            jdbcTemplate.enableCache();
            //obsWriter = new ObsWriter("obs://hadoop-obs/flink/test/", ObsWriter.FileFormat.TXT);
            //obsWriter.enableCache();
            //fsParquetWriter = new FsParquetWriter("obs://hadoop-obs/flink/pqt/");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String createTime = (String) columnMap.get("create_time");
            if (createTime.compareTo("2024-08-10 00:00:00") >= 0) {
                String id = (String) columnMap.get("id");
                String sql = new SQL().UPDATE("company_bid_info_v2")
                        .SET("update_time = now()")
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                jdbcTemplate.update(sql);
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
            jdbcTemplate.flush();
            //obsWriter.flush();
            //fsParquetWriter.flush();
        }
    }
}
