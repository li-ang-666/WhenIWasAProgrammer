package com.liang.flink.job;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisWriter;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DorisBitmapUtils;
import com.liang.flink.basic.EnvironmentFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class CrowdUserBitmapJob {
    private static final String REDIS_KEY = "crowd";
    private static final String PHASE_1 = "source working";
    private static final String PHASE_2 = "sink working";
    private static final String PHASE_3 = "all finish";

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
        private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
        private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        private static final String USER = "hive";
        private static final String PASSWORD = "";
        private static final DorisSchema DORIS_SCHEMA = DorisSchema.builder()
                .database("test").tableName("bitmap_test")
                .derivedColumns(Collections.singletonList("user_id_bitmap = if(user_id < 1, bitmap_empty(), to_bitmap(user_id))"))
                .build();
        private final AtomicBoolean cancel = new AtomicBoolean(false);
        private final Config config;
        private RedisTemplate redisTemplate;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            redisTemplate = new RedisTemplate("metadata");
            Class.forName(DRIVER);
        }

        @Override
        public void run(SourceContext<DorisOneRow> ctx) {
            while (!cancel.get()) {
                while (!StrUtil.equalsAny(redisTemplate.get(REDIS_KEY), PHASE_2, PHASE_3)) {
                    log.info("find mission");
                    try {
                        queryAndSend(ctx);
                    } catch (Exception e) {
                        log.error("source error, sleep 1 minutes", e);
                        LockSupport.parkUntil(System.currentTimeMillis() + 1000 * 60);
                        // 退回一阶段
                        redisTemplate.set(REDIS_KEY, PHASE_1);
                    }
                }
                log.info("no mission, sleep 1 minutes");
                LockSupport.parkUntil(System.currentTimeMillis() + 1000 * 60);
            }
        }

        private void queryAndSend(SourceContext<DorisOneRow> ctx) throws Exception {
            redisTemplate.set(REDIS_KEY, PHASE_1);
            Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
            PreparedStatement preparedStatement = connection.prepareStatement("select id, bitmap from test.bitmap_test");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String id = resultSet.getString(1);
                InputStream bitmapInputStream = resultSet.getBinaryStream(2);
                Roaring64NavigableMap bitmap = DorisBitmapUtils.parseBinary(IoUtil.readBytes(bitmapInputStream));
                if (bitmap.isEmpty()) {
                    DorisOneRow dorisOneRow = new DorisOneRow(DORIS_SCHEMA);
                    dorisOneRow.put("id", id);
                    dorisOneRow.put("user_id", 0);
                    ctx.collect(dorisOneRow);
                    continue;
                }
                bitmap.forEach(userId -> {
                    DorisOneRow dorisOneRow = new DorisOneRow(DORIS_SCHEMA);
                    dorisOneRow.put("id", id);
                    dorisOneRow.put("user_id", userId);
                    ctx.collect(dorisOneRow);
                });
            }
            resultSet.close();
            preparedStatement.close();
            connection.close();
            redisTemplate.set(REDIS_KEY, PHASE_2);
            ctx.collect(new DorisOneRow(DORIS_SCHEMA));
        }

        @Override
        public void cancel() {
            cancel.set(true);
        }
    }


    @Slf4j
    @RequiredArgsConstructor
    private final static class CrowdUserBitmapSink extends RichSinkFunction<DorisOneRow> implements CheckpointedFunction {
        private final Config config;
        private DorisWriter dorisWriter;
        private RedisTemplate redisTemplate;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dorisWriter = new DorisWriter("dorisSink", 256 * 1024 * 1024);
            redisTemplate = new RedisTemplate("metadata");
        }

        @Override
        public void invoke(DorisOneRow dorisOneRow, Context context) {
            if (dorisOneRow.getColumnMap().isEmpty()) {
                dorisWriter.flush();
                redisTemplate.set(REDIS_KEY, PHASE_3);
                return;
            }
            dorisWriter.write(dorisOneRow);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            dorisWriter.flush();
        }

        @Override
        public void finish() {
            dorisWriter.flush();
        }

        @Override
        public void close() {
            dorisWriter.flush();
        }
    }
}
