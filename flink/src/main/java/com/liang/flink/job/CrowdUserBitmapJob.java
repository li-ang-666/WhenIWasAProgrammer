package com.liang.flink.job;

import cn.hutool.core.io.IoUtil;
import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisWriter;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DorisBitmapUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class CrowdUserBitmapJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
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
    private final static class CrowdUserBitmapSource extends RichSourceFunction<DorisOneRow> {
        private static final int MAX_TRY_TIMES = 3;
        private static final int SUCCESS_SCORE = 100;
        private static final String DORIS_CHECK_SQL = "select * from crowd.crowd_user_bitmap_tasks where task_finish_time is null";
        private static final String HIVE_CONFIG_SQL = "set spark.executor.memory=10g";
        private static final String HIVE_QUERY_SQL_TEMPLATE = "select user_id_bitmap from project.crowd_user_bitmap where crowd_id = '%s' and create_timestamp = '%s' and pt = '%s'";
        private static final String DORIS_START_SQL_TEMPLATE = "update crowd.crowd_user_bitmap_tasks set task_start_time = now() where crowd_id = '%s' and create_timestamp = '%s' and pt = '%s'";
        private static final String DORIS_FINISH_SQL_TEMPLATE = "update crowd.crowd_user_bitmap_tasks set task_finish_time = now(), error_message = %s where crowd_id = '%s' and create_timestamp = '%s' and pt = '%s'";
        private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
        private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        private static final String USER = "hive";
        private static final String PASSWORD = "";
        private static final DorisSchema DORIS_SCHEMA = DorisSchema.builder()
                .database("crowd").tableName("crowd_user_bitmap")
                .derivedColumns(Collections.singletonList("user_id_bitmap = if(user_id < 1, bitmap_empty(), to_bitmap(user_id))"))
                .build();
        private final AtomicBoolean cancel = new AtomicBoolean(false);
        private final Config config;
        private JdbcTemplate jdbcTemplate;
        private DorisWriter dorisWriter;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("doris");
            dorisWriter = new DorisWriter("dorisSink", 256 * 1024 * 1024);
            Class.forName(DRIVER);
        }

        @Override
        public void run(SourceContext<DorisOneRow> ctx) {
            while (!cancel.get()) {
                List<Map<String, Object>> taskMaps = jdbcTemplate.queryForColumnMaps(DORIS_CHECK_SQL);
                if (taskMaps.isEmpty()) {
                    log.info("no pending tasks");
                    LockSupport.parkUntil(System.currentTimeMillis() + 1000 * 30);
                }
                // 串行遍历任务列表
                for (Map<String, Object> taskMap : taskMaps) {
                    String crowdId = String.valueOf(taskMap.get("crowd_id"));
                    String createTimestamp = String.valueOf(taskMap.get("create_timestamp"));
                    String pt = String.valueOf(taskMap.get("pt"));
                    // 上报 start 时间
                    jdbcTemplate.update(String.format(DORIS_START_SQL_TEMPLATE, crowdId, createTimestamp, pt));
                    log.info("start task, crowd_id = {}, create_timestamp = {}, pt = {}", crowdId, createTimestamp, pt);
                    // 失败重试 3 次
                    Exception exception = null;
                    int i = 0;
                    while (i < MAX_TRY_TIMES && !cancel.get()) {
                        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
                            connection.prepareStatement(HIVE_CONFIG_SQL).executeUpdate();
                            ResultSet resultSet = connection.prepareStatement(String.format(HIVE_QUERY_SQL_TEMPLATE, crowdId, createTimestamp, pt)).executeQuery();
                            while (resultSet.next() && !cancel.get()) {
                                InputStream bitmapInputStream = resultSet.getBinaryStream(1);
                                Roaring64NavigableMap bitmap = DorisBitmapUtils.parseBinary(IoUtil.readBytes(bitmapInputStream));
                                if (bitmap.isEmpty()) {
                                    DorisOneRow dorisOneRow = new DorisOneRow(DORIS_SCHEMA);
                                    dorisOneRow.put("crowd_id", crowdId);
                                    dorisOneRow.put("create_timestamp", createTimestamp);
                                    dorisOneRow.put("user_id", 0);
                                    dorisWriter.write(dorisOneRow);
                                    continue;
                                }
                                LongIterator longIterator = bitmap.getLongIterator();
                                while (longIterator.hasNext() && !cancel.get()) {
                                    DorisOneRow dorisOneRow = new DorisOneRow(DORIS_SCHEMA);
                                    dorisOneRow.put("crowd_id", crowdId);
                                    dorisOneRow.put("create_timestamp", createTimestamp);
                                    dorisOneRow.put("user_id", longIterator.next());
                                    dorisWriter.write(dorisOneRow);
                                }
                            }
                            dorisWriter.flush();
                            i = SUCCESS_SCORE;
                        } catch (Exception e) {
                            exception = e;
                            log.error("hive to doris error for {} times, crowd_id = {}, create_timestamp = {}, pt = {}", ++i, crowdId, createTimestamp, pt, e);
                            LockSupport.parkUntil(System.currentTimeMillis() + 1000 * 30);
                        }
                    }
                    // 上报 finish 时间
                    String errorMessage = SqlUtils.formatValue((i == SUCCESS_SCORE) ? "SUCCESS" : Objects.requireNonNull(exception).getMessage());
                    jdbcTemplate.update(String.format(DORIS_FINISH_SQL_TEMPLATE, errorMessage, crowdId, createTimestamp, pt));
                    log.info("finish task, crowd_id = {}, create_timestamp = {}, pt = {}, error_message = {}", crowdId, createTimestamp, pt, errorMessage);
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
    private final static class CrowdUserBitmapSink extends RichSinkFunction<DorisOneRow> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void invoke(DorisOneRow dorisOneRow, Context context) {
        }
    }
}
