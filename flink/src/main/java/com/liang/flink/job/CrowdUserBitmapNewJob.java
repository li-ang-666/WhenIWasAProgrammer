package com.liang.flink.job;

import cn.hutool.core.io.IoUtil;
import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisWriter;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DorisBitmapUtils;
import com.liang.flink.basic.EnvironmentFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

@Slf4j
public class CrowdUserBitmapNewJob {
    private static final int CKP_INTERVAL = 1000 * 60;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        // 1分钟一次ckp
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(CKP_INTERVAL);
        checkpointConfig.setMinPauseBetweenCheckpoints(CKP_INTERVAL);
        Config config = ConfigUtils.getConfig();
        // chain
        env.addSource(new CrowdUserBitmapSource(config))
                .setParallelism(1)
                .name("CrowdUserBitmapSource")
                .uid("CrowdUserBitmapSource")
                .rebalance()
                .addSink(new CrowdUserBitmapSink(config))
                .setParallelism(1)
                .name("CrowdUserBitmapSink")
                .uid("CrowdUserBitmapSink");
        env.execute("doris.crowd_user_bitmap");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class CrowdUserBitmapSource extends RichSourceFunction<BitmapTask> {
        private static final String DORIS_QUERY_TEMPLATE = "select * from crowd.crowd_user_bitmap_tasks where task_start_time is null";
        private static final String DORIS_START_TEMPLATE = "update crowd.crowd_user_bitmap_tasks set task_start_time = now() where crowd_id = '%s' and create_timestamp = '%s' and pt = '%s'";
        private final AtomicBoolean cancel = new AtomicBoolean(false);
        private final Config config;
        private JdbcTemplate dorisJdbcTemplate;


        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dorisJdbcTemplate = new JdbcTemplate("doris");
        }

        @Override
        public void run(SourceContext<BitmapTask> ctx) {
            while (!cancel.get()) {
                // 5秒巡检一次
                LockSupport.parkUntil(System.currentTimeMillis() + 1000 * 5);
                List<Map<String, Object>> columnMaps = dorisJdbcTemplate.queryForColumnMaps(DORIS_QUERY_TEMPLATE);
                for (Map<String, Object> columnMap : columnMaps) {
                    String crowdId = String.valueOf(columnMap.get("crowd_id"));
                    String createTimestamp = String.valueOf(columnMap.get("create_timestamp"));
                    String pt = String.valueOf(columnMap.get("pt"));
                    ctx.collect(new BitmapTask(crowdId, createTimestamp, pt));
                    dorisJdbcTemplate.update(String.format(DORIS_START_TEMPLATE, crowdId, createTimestamp, pt));
                }
            }
        }

        @Override
        public void cancel() {
            cancel.set(true);
        }
    }


    @Slf4j
    @RequiredArgsConstructor
    private final static class CrowdUserBitmapSink extends RichSinkFunction<BitmapTask> implements CheckpointedFunction {
        // hive
        private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
        private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        private static final String USER = "hive";
        private static final String PASSWORD = "";
        private static final List<String> HIVE_CONFIGS = Arrays.asList(
                "set spark.yarn.priority=999",
                // executor
                "set spark.executor.cores=1",
                "set spark.executor.memory=8g",
                "set spark.executor.memoryOverhead=512m",
                // driver
                "set spark.driver.memory=2g",
                "set spark.driver.memoryOverhead=512m"
        );
        private static final String HIVE_QUERY_TEMPLATE = "select crowd_id, create_timestamp, user_id_bitmap from project.crowd_user_bitmap where crowd_id = '%s' and create_timestamp = '%s' and pt = '%s'";
        // doris
        private static final DorisSchema DORIS_SCHEMA = DorisSchema.builder()
                .database("crowd").tableName("crowd_user_bitmap")
                .derivedColumns(Collections.singletonList("user_id_bitmap = if(user_id < 1, bitmap_empty(), to_bitmap(user_id))"))
                .build();
        // sink function
        private final List<BitmapTask> bitmapTasks = new ArrayList<>(128);
        private final Config config;
        private DorisWriter dorisWriter;
        private JdbcTemplate dorisJdbcTemplate;


        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dorisWriter = new DorisWriter("dorisSink", 256 * 1024 * 1024);
            dorisJdbcTemplate = new JdbcTemplate("doris");
            try {
                Class.forName(DRIVER);
            } catch (Exception ignore) {
            }
        }

        @Override
        public void invoke(BitmapTask bitmapTask, Context context) {
            synchronized (bitmapTasks) {
                bitmapTasks.add(bitmapTask);
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
            synchronized (bitmapTasks) {
                try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
                    // config
                    for (String hiveConfigSql : HIVE_CONFIGS) {
                        connection.prepareStatement(hiveConfigSql).executeUpdate();
                    }
                    // query
                    String hiveQuerySql = bitmapTasks.stream()
                            .map(bitmapTask -> String.format(HIVE_QUERY_TEMPLATE, bitmapTask.getCrowdId(), bitmapTask.getCreateTimestamp(), bitmapTask.getPt()))
                            .collect(Collectors.joining("\nunion all\n"));
                    log.info("query hive: {}", hiveQuerySql);
                    ResultSet resultSet = connection.prepareStatement(hiveQuerySql).executeQuery();
                    // send
                    while (resultSet.next()) {
                        String crowdId = resultSet.getString(1);
                        String createTimestamp = resultSet.getString(2);
                        Roaring64NavigableMap userIdBitmap = DorisBitmapUtils.parseBinary(IoUtil.readBytes(resultSet.getBinaryStream(3)));
                        log.info("get hive bitmap: crowd_id = {}, create_timestamp = {}, bitmap_count = {}", crowdId, createTimestamp, userIdBitmap.getLongCardinality());
                        if (userIdBitmap.isEmpty()) {
                            DorisOneRow dorisOneRow = new DorisOneRow(DORIS_SCHEMA)
                                    .put("crowd_id", crowdId)
                                    .put("create_timestamp", createTimestamp)
                                    .put("user_id", 0);
                            dorisWriter.write(dorisOneRow);
                            continue;
                        }
                        userIdBitmap.forEach(userId -> {
                            DorisOneRow dorisOneRow = new DorisOneRow(DORIS_SCHEMA)
                                    .put("crowd_id", crowdId)
                                    .put("create_timestamp", createTimestamp)
                                    .put("user_id", userId);
                            dorisWriter.write(dorisOneRow);
                        });
                    }
                    // flush, report finish, clear
                    dorisWriter.flush();
                    bitmapTasks.forEach(bitmapTask -> {

                    });
                    bitmapTasks.clear();
                } catch (Exception ignore) {
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    private final static class BitmapTask {
        private String crowdId;
        private String createTimestamp;
        private String pt;
    }
}
