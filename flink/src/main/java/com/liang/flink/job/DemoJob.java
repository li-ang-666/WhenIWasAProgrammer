package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.storage.obs.ObsWriter;
import com.liang.common.service.storage.parquet.TableParquetWriter;
import com.liang.common.service.storage.parquet.schema.ReadableSchema;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;

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
        private TableParquetWriter tableParquetWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("427.test");
            jdbcTemplate.enableCache();
            obsWriter = new ObsWriter("obs://hadoop-obs/flink/test/", ObsWriter.FileFormat.TXT);
            obsWriter.enableCache();
            RepairTask repairTask = config.getRepairTasks().get(0);
            List<ReadableSchema> schemas = new JdbcTemplate(repairTask.getSourceName()).queryForList("desc " + repairTask.getTableName(),
                    rs -> ReadableSchema.of(rs.getString(1), rs.getString(2)));
            tableParquetWriter = new TableParquetWriter(repairTask.getTableName(), schemas);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            tableParquetWriter.write(singleCanalBinlog.getColumnMap());
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
            tableParquetWriter.flush();
        }
    }
}
