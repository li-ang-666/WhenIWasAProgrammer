package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
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
@LocalConfigFile("export.yml")
public class ExportJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new ExportSink(config))
                .name("ExportSink")
                .uid("ExportSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("ExportJob");
    }

    @RequiredArgsConstructor
    private final static class ExportSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private TableParquetWriter tableParquetWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            RepairTask repairTask = config.getRepairTasks().get(0);
            List<ReadableSchema> schemas = new JdbcTemplate(repairTask.getSourceName()).queryForList("DESC " + repairTask.getTableName(),
                    rs -> ReadableSchema.of(rs.getString(1), rs.getString(2)));
            tableParquetWriter = new TableParquetWriter(repairTask.getTableName(), schemas);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            tableParquetWriter.write(singleCanalBinlog.getColumnMap());
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            tableParquetWriter.flush();
        }

        @Override
        public void finish() {
            tableParquetWriter.flush();
        }

        @Override
        public void close() {
            tableParquetWriter.flush();
        }
    }
}
