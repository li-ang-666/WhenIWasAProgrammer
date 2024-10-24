package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.storage.parquet.TableParquetWriter;
import com.liang.common.service.storage.parquet.schema.ReadableSchema;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@LocalConfigFile("export.yml")
public class ExportJob {
    // hive jdbc
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String USER = "hive";
    private static final String PASSWORD = "";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        // 建表
        String sinkTableName = (String) config.getOtherConfigs().get("sinkTableName");
        String schemaSource = (String) config.getOtherConfigs().get("schemaSource");
        String schemaTable = (String) config.getOtherConfigs().get("schemaTable");
        List<ReadableSchema> schemas = new JdbcTemplate(schemaSource).queryForList("DESC " + schemaTable,
                rs -> ReadableSchema.of(rs.getString(1), rs.getString(2)));


        StreamFactory.create(env)
                .rebalance()
                .addSink(new ExportSink(config, sinkTableName, schemas))
                .name("ExportSink")
                .uid("ExportSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("ExportJob");
    }

    private static void createTable(String sinkTableName, List<ReadableSchema> schemas) throws Exception {
        Class.forName(DRIVER);
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            String dropSql = "DROP TABLE IF EXISTS test." + sinkTableName;
            String createSql = StrUtil.replaceLast(new ArrayList<String>() {{
                add("CREATE TABLE IF NOT EXISTS test." + sinkTableName + " (");
                AtomicInteger maxLength = new AtomicInteger(Integer.MIN_VALUE);
                schemas.forEach(readableSchema -> {
                    String formattedColumnName = SqlUtils.formatField(readableSchema.getName());
                    String formattedColumnType = readableSchema.getSqlType();
                    maxLength.set(Math.max(maxLength.get(), formattedColumnName.length() + 7));
                    add(String.format("  %s%s%s,", formattedColumnName, StrUtil.repeat(" ", maxLength.get() - formattedColumnName.length()), formattedColumnType));
                });
                add(String.format(") STORED AS PARQUET LOCATION '%s';", TableParquetWriter.PARQUET_PATH_PREFIX + sinkTableName));
            }}.stream().collect(Collectors.joining("\n", "\n", "\n")), ",", "");
            try (Statement statement = connection.createStatement()) {
                statement.execute(dropSql);
                statement.execute(createSql);
            }
            log.info("{}", dropSql);
            log.info("{}", createSql);
        }
    }

    @RequiredArgsConstructor
    private final static class ExportSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private final String sinkTableName;
        private final List<ReadableSchema> schemas;
        private TableParquetWriter tableParquetWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            tableParquetWriter = new TableParquetWriter(sinkTableName, schemas);
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
