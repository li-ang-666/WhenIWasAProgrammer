package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.storage.obs.ObsWriter;
import com.liang.common.service.storage.parquet.FsParquetWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;
import java.util.stream.Collectors;

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
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("427.test");
            jdbcTemplate.enableCache();
            obsWriter = new ObsWriter("obs://hadoop-obs/flink/test/", ObsWriter.FileFormat.TXT);
            obsWriter.enableCache();
            Map<String, String> descInfo = new JdbcTemplate("116.prism").queryForList("desc equity_ratio",
                            rs -> Tuple2.of(rs.getString(1), rs.getString(2)))
                    .stream().collect(Collectors.toMap(e -> e.f0, e -> e.f1));
            fsParquetWriter = new FsParquetWriter("equity_ratio", descInfo);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            fsParquetWriter.write(singleCanalBinlog.getColumnMap());
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
            obsWriter.flush();
            fsParquetWriter.flush();
        }
    }
}
